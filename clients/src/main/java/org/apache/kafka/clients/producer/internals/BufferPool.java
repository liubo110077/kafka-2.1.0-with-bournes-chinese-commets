/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public class BufferPool {

    static final String WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time";

    private final long totalMemory;
    private final int poolableSize;
    private final ReentrantLock lock;
    private final Deque<ByteBuffer> free;
    private final Deque<Condition> waiters;
    /** Total available memory is the sum of nonPooledAvailableMemory and the number of byte buffers in free * poolableSize.  */
    private long nonPooledAvailableMemory;
    private final Metrics metrics;
    private final Time time;
    private final Sensor waitTime;

    /**
     * Create a new buffer pool
     *
     * @param memory The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize The buffer size to cache in the free list rather than deallocating
     * @param metrics instance of Metrics
     * @param time time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        this.totalMemory = memory;
        this.nonPooledAvailableMemory = memory;
        this.metrics = metrics;
        this.time = time;
        this.waitTime = this.metrics.sensor(WAIT_TIME_SENSOR_NAME);
        MetricName rateMetricName = metrics.metricName("bufferpool-wait-ratio",
                                                   metricGrpName,
                                                   "The fraction of time an appender waits for space allocation.");
        MetricName totalMetricName = metrics.metricName("bufferpool-wait-time-total",
                                                   metricGrpName,
                                                   "The total time an appender waits for space allocation.");
        this.waitTime.add(new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName));
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     *
     * @param size The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *         forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                                               + " bytes, but there is a hard limit of "
                                               + this.totalMemory
                                               + " on memory allocations.");

        //NIO中的ByteBuffer
        ByteBuffer buffer = null;
        this.lock.lock();
        try {
            // check if we have a free buffer of the right size pooled
            /**
             *  TODO  poolableSize = batch.size = 16KB
             *        this.free = new ArrayDeque<ByteBuffer>();
             *        刚开始this.free 为空，所以代码要继续运行
             *        如果申请的内存大于16KB，比如1MB，那么也要继续运行
             */
            if (size == poolableSize && !this.free.isEmpty())
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            // TODO 当前free队列ArrayDeque<ByteBuffer>的内存占用 = 队列的大小 * 16KB
            int freeListSize = freeSize() * this.poolableSize;

            // TODO this.nonPooledAvailableMemory初始值是32MB, freeListSize的初始值是0 ，如果申请的内存大小小于剩余的内存，表明还有足够的未分配内存。
            if (this.nonPooledAvailableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request, but need to allocate the buffer
                //尝试回收free队列中的最后一个元素，直到满足申请的size大小
                //第一次运行，this.free还是空的，所以不会运行下边的代码
                //后边运行，当内存够用时也不运行，继续创建
                freeUp(size);
                //将当前可用内存减少
                this.nonPooledAvailableMemory -= size;
            } else {
                //但是还有一种情况，BufferPool中的内存已经被占满，但是还没有调用unallocate方法释放内存时，已经无可用内存
                // we are out of memory and will have to block
                int accumulated = 0;
                //创建一个Condition对象
                Condition moreMemory = this.lock.newCondition();
                try {
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    //将这个condition对象放入waiters队列的最后一个
                    this.waiters.addLast(moreMemory);
                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    while (accumulated < size) {
                        long startWaitNs = time.nanoseconds();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            //等待更多内存的Condition对象等待，当前线程进入此condition的等待队列，一旦有线程执行了unallocate方法，就会释放一部分内存，并且将释放的内存放入free队列中
                            //并从this.waiters中取出第一个元素，并且进行唤醒，次代码就会继续执行
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            long endWaitNs = time.nanoseconds();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
                            //metric用，暂时忽略
                            recordWaitTime(timeNs);
                        }
                        //等待超时，抛出异常
                        if (waitingTimeElapsed) {
                            throw new TimeoutException("Failed to allocate memory within the configured max blocking time " + maxTimeToBlockMs + " ms.");
                        }

                        remainingTimeToBlockNs -= timeNs;

                        // check if we can satisfy this request from the free list,
                        // otherwise allocate memory
                        /**
                         *  TODO 第一次 accumulated == 0 满足
                         *             如果申请的内存大小刚好等于16KB
                         *             并且this.free不为空
                         *       则，将this.free中第一个元素弹出队列，并且赋值给buffer（就是返回值）
                         *       并将accumulated设置为申请的内存大小，下次循环时，因为 while(accumulated < size）不满足（因为accumulated==size），退出循环
                         */
                        if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                            // just grab a buffer from the free list
                            buffer = this.free.pollFirst();
                            accumulated = size;
                        } else {
                            // we'll need to allocate memory, but we may only get
                            // part of what we need on this iteration
                            /**
                             *  TODO 如果申请的内存不等于16KB，freeUp出size-accumulated 大小的内存
                             *      注意 nonPooledAvailableMemory 会随着freeUp增大，但是增大后的值也仅仅满足大于size，之后就不会再尝试回收了
                             *      如果不能够回收足够大小的内存（大于size），可能的原因是其他线程并未释放内存所以，this.free中没有足够的元素
                             *      第一次accumulated=0，所以size和this.nonPooledAvailableMemory的最小值肯定为this.nonPooledAvailableMemory
                             *      然后this.nonPooledAvailableMemory-=got，表明这段内存已经被占用了（虽然没有实际占用，但是先标识）
                             *      accumulated  += got，表明已经先占用了了这么多内存，继续循环，因为accumulated小于size（申请的内存）
                             *      然后继续等待，直到有线程释放了内存，并且accumulated会一直累加，一旦accumulated小于size不满足，说明已经有足够的内存了，就会退出循环
                             */
                            freeUp(size - accumulated);
                            int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
                            this.nonPooledAvailableMemory -= got;
                            accumulated += got;
                        }
                    }
                    //退出循环后，需要将accumulated清零
                    // Don't reclaim memory on throwable since nothing was thrown
                    accumulated = 0;
                } finally {
                    // When this loop was not able to successfully terminate don't loose available memory
                    //增加可用内存
                    this.nonPooledAvailableMemory += accumulated;
                    //将condition移出等待队列
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left
            // over for them
            try {
                //TODO 只要有可用内存或者this.fee不为空，并且等待队列中还有等待的condition，则通知condition继续执行
                if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty())
                    this.waiters.peekFirst().signal();
            } finally {
                // Another finally... otherwise find bugs complains
                lock.unlock();
            }
        }
        //第一次运行，buffer==null满足
        //返回一个size大小的ByteBuffer对象
        if (buffer == null)
            return safeAllocateByteBuffer(size);
        else
            return buffer;
    }

    // Protected for testing
    protected void recordWaitTime(long timeNs) {
        this.waitTime.record(timeNs, time.milliseconds());
    }

    /**
     * Allocate a buffer.  If buffer allocation fails (e.g. because of OOM) then return the size count back to
     * available memory and signal the next waiter if it exists.
     */
    private ByteBuffer safeAllocateByteBuffer(int size) {
        boolean error = true;
        try {
            ByteBuffer buffer = allocateByteBuffer(size);
            error = false;
            return buffer;
        } finally {
            if (error) {
                this.lock.lock();
                try {
                    this.nonPooledAvailableMemory += size;
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                } finally {
                    this.lock.unlock();
                }
            }
        }
    }

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
        //如果Deque<ByteBuffer> free中已经分配过，并且剩余的空间小于要申请的内存大小，则将free队列中的最后一个元素移除，并且将移除的大小增加给nonPooledAvailableMemory
        while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size)
            this.nonPooledAvailableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     *
     * @param buffer The buffer to return
     * @param size The size of the buffer to mark as deallocated, note that this may be smaller than buffer.capacity
     *             since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            if (size == this.poolableSize && size == buffer.capacity()) {
                //清空Buffer，但不是真的清空，只是改变指针的位置
                buffer.clear();
                //当deallocate时，将buffer还回到free队列中
                this.free.add(buffer);
            } else {
                this.nonPooledAvailableMemory += size;
            }
            //还挺复杂，看来BufferPool中内存不足时，会创建多个Condition对象
            //这里会拿出第一个Condition对象进行唤醒。
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory + freeSize() * (long) this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }
}

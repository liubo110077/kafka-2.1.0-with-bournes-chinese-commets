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
package org.apache.kafka.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for Thread that sets things up nicely
 */
public class KafkaThread extends Thread {

    private final Logger log = LoggerFactory.getLogger(getClass());
    
    public static KafkaThread daemon(final String name, Runnable runnable) {
        return new KafkaThread(name, runnable, true);
    }

    public static KafkaThread nonDaemon(final String name, Runnable runnable) {
        return new KafkaThread(name, runnable, false);
    }

    public KafkaThread(final String name, boolean daemon) {
        super(name);
        configureThread(name, daemon);
    }

    public KafkaThread(final String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        configureThread(name, daemon);
    }

    private void configureThread(final String name, boolean daemon) {
        //The Java Virtual Machine exits when the only threads running are all daemon threads.
        //当运行的线程全部是守护进程时，JVM退出
        /*
        Daemon的作用是为其他线程的运行提供服务，比如说GC线程。
        其实User Thread线程和Daemon Thread守护线程本质上来说去没啥区别的，唯一的区别之处就在虚拟机的离开时候：如果User Thread全部撤离，那么Daemon Thread也就没啥线程好服务的了，所以虚拟机也就退出了。
        只要当前JVM实例中尚存在任何一个非守护线程没有结束，守护线程就全部工作；只有当最后一个非守护线程结束时，守护线程随着JVM一同结束工作。守护线程最典型的应用就是 GC (垃圾回收器)
         */
        setDaemon(daemon);
        setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                log.error("Uncaught exception in thread '{}':", name, e);
            }
        });
    }

}

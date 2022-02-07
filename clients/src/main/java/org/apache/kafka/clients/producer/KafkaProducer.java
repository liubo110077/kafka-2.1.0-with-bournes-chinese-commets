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
package org.apache.kafka.clients.producer;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientDnsLookup;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.producer.internals.BufferPool;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.clients.producer.internals.ProducerMetrics;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;


/**
 * A Kafka client that publishes records to the Kafka cluster.
 * <P>
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all");
 * props.put("delivery.timeout.ms", 30000);
 * props.put("batch.size", 16384);
 * props.put("linger.ms", 1);
 * props.put("buffer.memory", 33554432);
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for (int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * <p>
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * <p>
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The "all" setting
 * we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
 * <p>
 * If the request fails, the producer can automatically retry, though since we have specified <code>retries</code>
 * as 0 it won't. Enabling retries also opens up the possibility of duplicates (see the documentation on
 * <a href="http://kafka.apache.org/documentation.html#semantics">message delivery semantics</a> for details).
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code> so under heavy load
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
 * a TimeoutException.
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the user provides with
 * their <code>ProducerRecord</code> into bytes. You can use the included {@link org.apache.kafka.common.serialization.ByteArraySerializer} or
 * {@link org.apache.kafka.common.serialization.StringSerializer} for simple string or byte types.
 * <p>
 * From Kafka 0.11, the KafkaProducer supports two additional modes: the idempotent producer and the transactional producer.
 * The idempotent producer strengthens Kafka's delivery semantics from at least once to exactly once delivery. In particular
 * producer retries will no longer introduce duplicates. The transactional producer allows an application to send messages
 * to multiple partitions (and topics!) atomically.
 * </p>
 * <p>
 * To enable idempotence, the <code>enable.idempotence</code> configuration must be set to true. If set, the
 * <code>retries</code> config will default to <code>Integer.MAX_VALUE</code> and the <code>acks</code> config will
 * default to <code>all</code>. There are no API changes for the idempotent producer, so existing applications will
 * not need to be modified to take advantage of this feature.
 * </p>
 * <p>
 * To take advantage of the idempotent producer, it is imperative to avoid application level re-sends since these cannot
 * be de-duplicated. As such, if an application enables idempotence, it is recommended to leave the <code>retries</code>
 * config unset, as it will be defaulted to <code>Integer.MAX_VALUE</code>. Additionally, if a {@link #send(ProducerRecord)}
 * returns an error even with infinite retries (for instance if the message expires in the buffer before being sent),
 * then it is recommended to shut down the producer and check the contents of the last produced message to ensure that
 * it is not duplicated. Finally, the producer can only guarantee idempotence for messages sent within a single session.
 * </p>
 * <p>To use the transactional producer and the attendant APIs, you must set the <code>transactional.id</code>
 * configuration property. If the <code>transactional.id</code> is set, idempotence is automatically enabled along with
 * the producer configs which idempotence depends on. Further, topics which are included in transactions should be configured
 * for durability. In particular, the <code>replication.factor</code> should be at least <code>3</code>, and the
 * <code>min.insync.replicas</code> for these topics should be set to 2. Finally, in order for transactional guarantees
 * to be realized from end-to-end, the consumers must be configured to read only committed messages as well.
 * </p>
 * <p>
 * The purpose of the <code>transactional.id</code> is to enable transaction recovery across multiple sessions of a
 * single producer instance. It would typically be derived from the shard identifier in a partitioned, stateful, application.
 * As such, it should be unique to each producer instance running within a partitioned application.
 * </p>
 * <p>All the new transactional APIs are blocking and will throw exceptions on failure. The example
 * below illustrates how the new APIs are meant to be used. It is similar to the example above, except that all
 * 100 messages are part of a single transaction.
 * </p>
 * <p>
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("transactional.id", "my-transactional-id");
 * Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
 *
 * producer.initTransactions();
 *
 * try {
 *     producer.beginTransaction();
 *     for (int i = 0; i < 100; i++)
 *         producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
 *     producer.commitTransaction();
 * } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
 *     // We can't recover from these exceptions, so our only option is to close the producer and exit.
 *     producer.close();
 * } catch (KafkaException e) {
 *     // For all other exceptions, just abort the transaction and try again.
 *     producer.abortTransaction();
 * }
 * producer.close();
 * } </pre>
 * </p>
 * <p>
 * As is hinted at in the example, there can be only one open transaction per producer. All messages sent between the
 * {@link #beginTransaction()} and {@link #commitTransaction()} calls will be part of a single transaction. When the
 * <code>transactional.id</code> is specified, all messages sent by the producer must be part of a transaction.
 * </p>
 * <p>
 * The transactional producer uses exceptions to communicate error states. In particular, it is not required
 * to specify callbacks for <code>producer.send()</code> or to call <code>.get()</code> on the returned Future: a
 * <code>KafkaException</code> would be thrown if any of the
 * <code>producer.send()</code> or transactional calls hit an irrecoverable error during a transaction. See the {@link #send(ProducerRecord)}
 * documentation for more details about detecting errors from a transactional send.
 * </p>
 * </p>By calling
 * <code>producer.abortTransaction()</code> upon receiving a <code>KafkaException</code> we can ensure that any
 * successful writes are marked as aborted, hence keeping the transactional guarantees.
 * </p>
 * <p>
 * This client can communicate with brokers that are version 0.10.0 or newer. Older or newer brokers may not support
 * certain client features.  For instance, the transactional APIs need broker versions 0.11.0 or later. You will receive an
 * <code>UnsupportedVersionException</code> when invoking an API that is not available in the running broker version.
 * </p>
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

    private final Logger log;
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    private static final String JMX_PREFIX = "kafka.producer";
    public static final String NETWORK_THREAD_PREFIX = "kafka-producer-network-thread";
    public static final String PRODUCER_METRIC_GROUP_NAME = "producer-metrics";

    private final String clientId;
    // Visible for testing
    final Metrics metrics;
    //分区器，假如一个Topic有3个分区，id分别为0，1，2，那么producer产生的message要发送到哪一个分区？
    //默认是DefaultPartitioner
    //The default partitioning strategy
    //If a partition is specified in the record, use it
    //If no partition is specified but a key is present choose a partition based on a hash of the key
    // If no partition or key is present choose a partition in a round-robin fashion
    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long totalMemorySize;
    private final Metadata metadata;
    //记录缓冲区，不是来一条记录就向Kafka发送一条记录，而是要缓冲起来
    //底层实现：queue
    //假如一个Topic有3个分区，id分别为0，1，2。那么RecordAccumulator中会有3个Deque分别作为三个分区的缓冲队列。
    private final RecordAccumulator accumulator;
    //Sender线程负责发送数据
    //获得RecordAccumulator accumulator中的数据，把数据封装成ClientRequest
    private final Sender sender;
    private final Thread ioThread;
    private final CompressionType compressionType;
    private final Sensor errors;
    private final Time time;
    //key serializer
    private final Serializer<K> keySerializer;
    //value serializer
    private final Serializer<V> valueSerializer;
    private final ProducerConfig producerConfig;
    private final long maxBlockTimeMs;
    //拦截器
    private final ProducerInterceptors<K, V> interceptors;
    private final ApiVersions apiVersions;
    private final TransactionManager transactionManager;
    private TransactionalRequestResult initTransactionsResult;

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     *
     */
    public KafkaProducer(final Map<String, Object> configs) {
        this(new ProducerConfig(configs), null, null, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
            keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     */
    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties), null, null, null, null, null, Time.SYSTEM);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * <p>
     * Note: after creating a {@code KafkaProducer} you must always {@link #close()} it to avoid resource leaks.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
                keySerializer, valueSerializer, null, null, null, Time.SYSTEM);
    }

    // visible for testing
    @SuppressWarnings("unchecked")
    KafkaProducer(ProducerConfig config,
                  Serializer<K> keySerializer,
                  Serializer<V> valueSerializer,
                  Metadata metadata,
                  KafkaClient kafkaClient,
                  ProducerInterceptors interceptors,
                  Time time) {
        try {
            Map<String, Object> userProvidedConfigs = config.originals();
            this.producerConfig = config;
            this.time = time;
            //服务器端用来区分producer客户端的标识
            String clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
            if (clientId.length() <= 0)
                clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
            this.clientId = clientId;

            //在kafka0.11之后支持事务
            String transactionalId = userProvidedConfigs.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG) ?
                    (String) userProvidedConfigs.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG) : null;
            LogContext logContext;
            if (transactionalId == null)
                logContext = new LogContext(String.format("[Producer clientId=%s] ", clientId));
            else
                logContext = new LogContext(String.format("[Producer clientId=%s, transactionalId=%s] ", clientId, transactionalId));
            log = logContext.logger(KafkaProducer.class);
            log.trace("Starting the Kafka producer");

            //metric是一些监控信息，我们分析源码的时候，不需要去关心
            Map<String, String> metricTags = Collections.singletonMap("client-id", clientId);
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    .recordLevel(Sensor.RecordingLevel.forName(config.getString(ProducerConfig.METRICS_RECORDING_LEVEL_CONFIG)))
                    .tags(metricTags);
            List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class,
                    Collections.singletonMap(ProducerConfig.CLIENT_ID_CONFIG, clientId));
            reporters.add(new JmxReporter(JMX_PREFIX));
            this.metrics = new Metrics(metricConfig, reporters, time);

            //设置分区器，默认是DefaultPartitioner
            /**
             * The default partitioning strategy:
             * <ul>
             * <li>If a partition is specified in the record, use it
             * <li>If no partition is specified but a key is present choose a partition based on a hash of the key
             * <li>If no partition or key is present choose a partition in a round-robin fashion
             */
            this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);

            //重试间隔
            //"The amount of time to wait before attempting to retry a failed request to a given topic partition.
            // This avoids repeatedly sending requests in a tight loop under some failure scenarios.";
            //默认值配置在ProducerConfig中,100毫秒
            //.define(RETRY_BACKOFF_MS_CONFIG, Type.LONG, 100L, atLeast(0L), Importance.LOW, CommonClientConfigs.RETRY_BACKOFF_MS_DOC)
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);

            //key的序列化器
            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                                                                         Serializer.class);
                this.keySerializer.configure(config.originals(), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = keySerializer;
            }

            //value的序列化器
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                                                                           Serializer.class);
                this.valueSerializer.configure(config.originals(), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = valueSerializer;
            }

            // load interceptors and make sure they get clientId
            userProvidedConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            ProducerConfig configWithClientId = new ProducerConfig(userProvidedConfigs, false);

            //拦截器，类似于一个过滤器
            List<ProducerInterceptor<K, V>> interceptorList = (List) configWithClientId.getConfiguredInstances(
                    ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptor.class);
            if (interceptors != null)
                this.interceptors = interceptors;
            else
                this.interceptors = new ProducerInterceptors<>(interceptorList);
            ClusterResourceListeners clusterResourceListeners = configureClusterResourceListeners(keySerializer,
                    valueSerializer, interceptorList, reporters);

            //一个请求的最大字节数（byte），服务端此设置的优先级比producer的优先级高。默认值1*1024*1024=1MB
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            //RecordAccumulator缓冲区字节数，默认值32 * 1024 * 1024，32MB
            //如果缓冲区已满，producer继续发送新的message，会被block住ProducerConfig.MAX_BLOCK_MS_CONFIG 毫秒，然后抛出异常。
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);

            //消息的压缩类型：支持none,gzip,snappy,lz4或zstd
            this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));

            //The configuration controls how long KafkaProducer.send() and KafkaProducer.partitionsFor() will block."
            //These methods can be blocked either because the buffer is full or metadata unavailable."
            //Blocking in the user-supplied serializers or partitioner will not be counted against this timeout.";
            this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);

            //事务管理者
            this.transactionManager = configureTransactionState(config, logContext, log);

            //配置超时间，包括delivery.timeout.ms，linger.ms以及request.timeout.ms
            int deliveryTimeoutMs = configureDeliveryTimeout(config, log);

            this.apiVersions = new ApiVersions();

            //构造RecordAccumulator
            /*
              - 在 Producer 端，如果我们希望消息尽快地被发送出去，因此不要有过多停留，所以必须设置 linger.ms=0，同时不要启用压缩。
                因为压缩操作本身要消耗 CPU 时间，会增加消息发送的延时。
                另外，最好不要设置 acks=all。
                我们刚刚在前面说过，Follower 副本同步往往是降低 Producer 端吞吐量和增加延时的首要原因。
              - 在 Producer 端，如果要改善吞吐量，通常的标配是增加消息批次的大小以及批次缓存时间，即 batch.size 和 linger.ms。
                目前它们的默认值都偏小，特别是默认的 16KB 的消息批次大小一般都不适用于生产环境。
                假设你的消息体大小是 1KB，默认一个消息批次也就大约 16 条消息，显然太小了。我们还是希望 Producer 能一次性发送更多的消息。
                除了这两个，你最好把压缩算法也配置上，以减少网络 I/O 传输量，从而间接提升吞吐量。当前，和 Kafka 适配最好的两个压缩算法是LZ4 和 zstd，不妨一试。
                同时，由于我们的优化目标是吞吐量，最好不要设置 acks=all 以及开启重试。前者引入的副本同步时间通常都是吞吐量的瓶颈，而后者在执行过程中也会拉低 Producer 应用的吞吐量。
                最后，如果你在多个线程中共享一个 Producer 实例，就可能会碰到缓冲区不够用的情形。倘若频繁地遭遇 TimeoutException：Failed to allocate memory within the configured max blocking time 这样的异常，那么你就必须显式地增加buffer.memory参数值，确保缓冲区总是有空间可以申请的。
             */
            this.accumulator = new RecordAccumulator(logContext,
                    //发送批次大小默认16384 byte,16KB
                    //会凑够16KB再进行发送，以增大客户端和服务器的吞吐量
                    //此参数和linger.ms会一起作用与发送信息的缓存。
                    config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
                    this.compressionType,
                    //linger.ms，不满足16KB的数据，是否延迟发送，默认值是0，表明来一条就发送一条，不延迟
                    //最好还是在延迟允许的情况下，将linger.ms设置成非0的数字
                    config.getInt(ProducerConfig.LINGER_MS_CONFIG),
                    retryBackoffMs,
                    deliveryTimeoutMs,
                    metrics,
                    PRODUCER_METRIC_GROUP_NAME,
                    time,
                    apiVersions,
                    transactionManager,
                    //32MB的缓冲区
                    new BufferPool(this.totalMemorySize, config.getInt(ProducerConfig.BATCH_SIZE_CONFIG), metrics, time, PRODUCER_METRIC_GROUP_NAME));
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(
                    config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                    config.getString(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG));

            //Metadata封装了Kafka集群的源数据信息，
            //主要有：Kafka集群的节点、有哪些topic、每个topic有哪些分区、该分区对应的ISR列表分布在哪些节点。
            //获取metadata需要给Kafka集群发送请求
            if (metadata != null) {
                this.metadata = metadata;
            } else {
                //METADATA_MAX_AGE_CONFIG
                //The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions.
                //默认时间是5 * 60 * 1000秒，共5分钟
                //每5分钟更新一次Kafka集群的源数据
                this.metadata = new Metadata(retryBackoffMs, config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG),
                    true, true, clusterResourceListeners);
                //向Kafka发送请求更新源数据
                //node01:9092,node02:9092,node03:9092
                this.metadata.update(Cluster.bootstrap(addresses), Collections.emptySet(), time.milliseconds());
            }
            this.errors = this.metrics.sensor("errors");
            //Sender实现了Runnable接口
            //业务代码
            this.sender = newSender(logContext, kafkaClient, this.metadata);
            String ioThreadName = NETWORK_THREAD_PREFIX + " | " + clientId;
            //又把Sender当成参数传递给了KafkaThread，为什么要这么做？
            //A wrapper for Thread that sets things up nicely
            //线程相关的代码和业务代码分离
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();
            config.logUnused();
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId, metrics);
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed this is to prevent resource leak. see KAFKA-2121
            close(0, TimeUnit.MILLISECONDS, true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    // visible for testing
    Sender newSender(LogContext logContext, KafkaClient kafkaClient, Metadata metadata) {
        /*
            max.in.flight.request.per.connection:默认是5
            发送数据的时候，有多个网络连接。每个网络连接可以忍受producer端发送给broker消息后，消息没有响应的个数。
            因为kafka有重试机制，所以有可能造成数据乱序，如果想要保证有序，这个值要设置为1
            The maximum number of unacknowledged requests the client will send on a single connection before blocking."
            Note that if this setting is set to be greater than 1 and there are failed sends, there is a risk of"
            message re-ordering due to retries (i.e., if retries are enabled).

            Kafka只能保证局部有序，即只能保证一个分区里的消息有序。而其具体实现是通过生产者为每个分区的消息维护一个发送队列，我们需要将保证顺序的消息都发送到同一个分区中。并且由于Kafka会同时发送多个消息，所以还需指定max.in.flight.requests.per.connection为1，保证前一个消息发送成功，后一个消息才开始发送。
            max.in.flight＞1 时能保证有序性吗？
            可以，设置幂等能保证。不过max.in.flight不能大于5。这是因为Broker端只会缓存最近5个Batch的SequenceNumber，例如我们发送1到6的报文，其中1发送失败，2-5发送成功，Broker缓存下来，当1重试时，Broker检查到1小于缓存中的最小序号，会抛出异常，而Producer将重试发送1超过最多次数或超时，影响性能。
            幂等如何保证有序性？
            通过引入ProduceID(PID)和SequenceNumber的概念，每个Producer在初始化时被分配唯一的PID，而<Topic,Partition>的每条有一个从0单调递增的SequenceNumber。在发送消息时，由以下三点保证：
            验证序号连续：Broker会验证Batch的SequenceNumber是否连续，若不连续，抛出异常；
            重试时，batch放置正确位置：Producer请求重试时，会根据SequenceNumber将Batch放在队列中的合适位置；
            重试时，max.in.flight调为1：当请求重试时，会把max.in.flight动态调整为1，保证请求序号小的先发送成功。
         */
        int maxInflightRequests = configureInflightRequests(producerConfig, transactionManager != null);
        int requestTimeoutMs = producerConfig.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
        ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(producerConfig);
        ProducerMetrics metricsRegistry = new ProducerMetrics(this.metrics);
        Sensor throttleTimeSensor = Sender.throttleTimeSensor(metricsRegistry.senderMetrics);


        KafkaClient client = kafkaClient != null ? kafkaClient : new NetworkClient(
                new Selector(producerConfig.getLong(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG),//Close idle connections after the number of milliseconds specified by this config.可以设置为-1表示不回收idle的网络连接
                        this.metrics, time, "producer", channelBuilder, logContext),
                metadata,
                clientId,
                maxInflightRequests,
                producerConfig.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),//base连接重试间隔时间
                producerConfig.getLong(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG),//最大连接重试间隔时间，如果连接重试连续失败，下一次重试以指数增长，直至最大连接重试时间。
                producerConfig.getInt(ProducerConfig.SEND_BUFFER_CONFIG),//TCP 发送缓冲区大小，默认值128KB
                producerConfig.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),//TCP接受缓冲区大小，默认值32KB
                requestTimeoutMs,
                ClientDnsLookup.forConfig(producerConfig.getString(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG)),
                time,
                true,
                apiVersions,
                throttleTimeSensor,
                logContext);
        //配置重试相关的参数
        int retries = configureRetries(producerConfig, transactionManager != null, log);
        //配置ack
        // 0    不等待
        // 1    等待leader
        // all  等待leader和follower
        short acks = configureAcks(producerConfig, transactionManager != null, log);
        //一个Sender中包含了一个NetworkClient用以真正的发送数据
        return new Sender(logContext,
                client,
                metadata,
                this.accumulator,
                maxInflightRequests == 1,
                producerConfig.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                acks,
                retries,
                metricsRegistry.senderMetrics,
                time,
                requestTimeoutMs,
                producerConfig.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG),
                this.transactionManager,
                apiVersions);
    }

    private static int configureDeliveryTimeout(ProducerConfig config, Logger log) {
        //调用了Producer.send()之后，成功或者失败反馈的最大等待上限时长，默认是120秒（120 * 1000）
        //这个参数限制了一个record发送之前被延迟总时长，包括了等待broker发送ack的时间以及发送失败retry的时间。
        //这个值的大小应该大于等于REQUEST_TIMEOUT_MS_CONFIG 和 LINGER_MS_CONFIG 之和。
        int deliveryTimeoutMs = config.getInt(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);

        //上面比如我们设置batch size为32KB,但是比如有的时刻消息比较少,过了很久,比如5min也没有凑够32KB,这样延时就很大,所以需要一个参数.
        // 再设置一个时间,到了这个时间,即使数据没达到32KB,也将这个批次发送出去.
        // 比如设置5ms,就是到了5ms,大小没到32KB,也会发出去
        // 默认值是0，no delay
        //By default (linger.ms=0), the producer will send messages as soon as there is a sender thread available to send them, even if there’s just one message in the batch.
        int lingerMs = config.getInt(ProducerConfig.LINGER_MS_CONFIG);

        //默认值是30秒
        //如果时间到了，会retry，再次发送消息
        // The configuration controls the maximum amount of time the client will wait
        // for the response of a request. If the response is not received before the timeout
        // elapses the client will resend the request if necessary or fail the request if
        // retries are exhausted.
        int requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);

        if (deliveryTimeoutMs < Integer.MAX_VALUE && deliveryTimeoutMs < lingerMs + requestTimeoutMs) {
            if (config.originals().containsKey(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
                // throw an exception if the user explicitly set an inconsistent value
                throw new ConfigException(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG
                    + " should be equal to or larger than " + ProducerConfig.LINGER_MS_CONFIG
                    + " + " + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            } else {
                // override deliveryTimeoutMs default value to lingerMs + requestTimeoutMs for backward compatibility
                deliveryTimeoutMs = lingerMs + requestTimeoutMs;
                log.warn("{} should be equal to or larger than {} + {}. Setting it to {}.",
                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, ProducerConfig.LINGER_MS_CONFIG,
                    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
            }
        }
        return deliveryTimeoutMs;
    }

    private static TransactionManager configureTransactionState(ProducerConfig config, LogContext logContext, Logger log) {

        TransactionManager transactionManager = null;

        boolean userConfiguredIdempotence = false;
        if (config.originals().containsKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))
            userConfiguredIdempotence = true;

        boolean userConfiguredTransactions = false;
        if (config.originals().containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG))
            userConfiguredTransactions = true;

        boolean idempotenceEnabled = config.getBoolean(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);

        if (!idempotenceEnabled && userConfiguredIdempotence && userConfiguredTransactions)
            throw new ConfigException("Cannot set a " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " without also enabling idempotence.");

        if (userConfiguredTransactions)
            idempotenceEnabled = true;

        if (idempotenceEnabled) {
            String transactionalId = config.getString(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
            int transactionTimeoutMs = config.getInt(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG);
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            transactionManager = new TransactionManager(logContext, transactionalId, transactionTimeoutMs, retryBackoffMs);
            if (transactionManager.isTransactional())
                log.info("Instantiated a transactional producer.");
            else
                log.info("Instantiated an idempotent producer.");
        }

        return transactionManager;
    }

    private static int configureRetries(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredRetries = false;
        /*
        2.retries使用建议
        使用retries的默认值交给使用方自己去控制，结果往往是不处理。所以通用设置建议设置如下：

        retries = Integer.MAX_VALUE
        max.in.flight.requests.per.connection = 1
        备注：这样设置后，发送客户端会一直进行重试直到broker返回ack；同时只有一个连接向broker发送数据保证了数据的顺序性。在Leader选举、集群中一个broker挂掉时，发送端会一直重试直到Leader选举结束。避免由于客户端对异常未处理造成的数据丢失，例如：遇到类似“This server is not the leader for that topic-partition”会自动恢复。

        3.retries后续发展
        该参数的设置已经在kafka 2.4版本中默认设置为Integer.MAX_VALUE；同时增加了delivery.timeout.ms的参数设置。
        也就是说在两分钟之内无限重试，但这两个参数的作用最终的结果还是有重试次数限制的。
        The default value for the producer's retries config was changed to
        Integer.MAX_VALUE, as we introduced delivery.timeout.ms in KIP-91,
        which sets an upper bound on the total time between sending a
        record and receiving acknowledgement from the broker.
        By default, the delivery timeout is set to 2 minutes.
         */
        if (config.originals().containsKey(ProducerConfig.RETRIES_CONFIG)) {
            userConfiguredRetries = true;
        }
        if (idempotenceEnabled && !userConfiguredRetries) {
            // We recommend setting infinite retries when the idempotent producer is enabled, so it makes sense to make
            // this the default.
            log.info("Overriding the default retries config to the recommended value of {} since the idempotent " +
                    "producer is enabled.", Integer.MAX_VALUE);
            return Integer.MAX_VALUE;
        }
        if (idempotenceEnabled && config.getInt(ProducerConfig.RETRIES_CONFIG) == 0) {
            throw new ConfigException("Must set " + ProducerConfig.RETRIES_CONFIG + " to non-zero when using the idempotent producer.");
        }
        return config.getInt(ProducerConfig.RETRIES_CONFIG);
    }

    private static int configureInflightRequests(ProducerConfig config, boolean idempotenceEnabled) {
        if (idempotenceEnabled && 5 < config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)) {
            throw new ConfigException("Must set " + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION + " to at most 5" +
                    " to use the idempotent producer.");
        }
        return config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
    }

    private static short configureAcks(ProducerConfig config, boolean idempotenceEnabled, Logger log) {
        boolean userConfiguredAcks = false;
        short acks = (short) parseAcks(config.getString(ProducerConfig.ACKS_CONFIG));
        if (config.originals().containsKey(ProducerConfig.ACKS_CONFIG)) {
            userConfiguredAcks = true;
        }

        if (idempotenceEnabled && !userConfiguredAcks) {
            log.info("Overriding the default {} to all since idempotence is enabled.", ProducerConfig.ACKS_CONFIG);
            return -1;
        }

        if (idempotenceEnabled && acks != -1) {
            throw new ConfigException("Must set " + ProducerConfig.ACKS_CONFIG + " to all in order to use the idempotent " +
                    "producer. Otherwise we cannot guarantee idempotence.");
        }
        return acks;
    }

    private static int parseAcks(String acksString) {
        try {
            return acksString.trim().equalsIgnoreCase("all") ? -1 : Integer.parseInt(acksString.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    /**
     * Needs to be called before any other methods when the transactional.id is set in the configuration.
     *
     * This method does the following:
     *   1. Ensures any transactions initiated by previous instances of the producer with the same
     *      transactional.id are completed. If the previous instance had failed with a transaction in
     *      progress, it will be aborted. If the last transaction had begun completion,
     *      but not yet finished, this method awaits its completion.
     *   2. Gets the internal producer id and epoch, used in all future transactional
     *      messages issued by the producer.
     *
     * Note that this method will raise {@link TimeoutException} if the transactional state cannot
     * be initialized before expiration of {@code max.block.ms}. Additionally, it will raise {@link InterruptException}
     * if interrupted. It is safe to retry in either case, but once the transactional state has been successfully
     * initialized, this method should no longer be used.
     *
     * @throws IllegalStateException if no transactional.id has been configured
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     * @throws TimeoutException if the time taken for initialize the transaction has surpassed <code>max.block.ms</code>.
     * @throws InterruptException if the thread is interrupted while blocked
     */
    public void initTransactions() {
        throwIfNoTransactionManager();
        if (initTransactionsResult == null) {
            initTransactionsResult = transactionManager.initializeTransactions();
            sender.wakeup();
        }

        try {
            if (initTransactionsResult.await(maxBlockTimeMs, TimeUnit.MILLISECONDS)) {
                initTransactionsResult = null;
            } else {
                throw new TimeoutException("Timeout expired while initializing transactional state in " + maxBlockTimeMs + "ms.");
            }
        } catch (InterruptedException e) {
            throw new InterruptException("Initialize transactions interrupted.", e);
        }
    }

    /**
     * Should be called before the start of each new transaction. Note that prior to the first invocation
     * of this method, you must invoke {@link #initTransactions()} exactly one time.
     *
     * @throws IllegalStateException if no transactional.id has been configured or if {@link #initTransactions()}
     *         has not yet been invoked
     * @throws ProducerFencedException if another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void beginTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        transactionManager.beginTransaction();
    }

    /**
     * Sends a list of specified offsets to the consumer group coordinator, and also marks
     * those offsets as part of the current transaction. These offsets will be considered
     * committed only if the transaction is committed successfully. The committed offset should
     * be the next message your application will consume, i.e. lastProcessedMessageOffset + 1.
     * <p>
     * This method should be used when you need to batch consumed and produced messages
     * together, typically in a consume-transform-produce pattern. Thus, the specified
     * {@code consumerGroupId} should be the same as config parameter {@code group.id} of the used
     * {@link KafkaConsumer consumer}. Note, that the consumer should have {@code enable.auto.commit=false}
     * and should also not commit offsets manually (via {@link KafkaConsumer#commitSync(Map) sync} or
     * {@link KafkaConsumer#commitAsync(Map, OffsetCommitCallback) async} commits).
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.UnsupportedForMessageFormatException  fatal error indicating the message
     *         format used for the offsets topic on the broker does not support transactions
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     */
    public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets,
                                         String consumerGroupId) throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.sendOffsetsToTransaction(offsets, consumerGroupId);
        sender.wakeup();
        result.await();
    }

    /**
     * Commits the ongoing transaction. This method will flush any unsent records before actually committing the transaction.
     *
     * Further, if any of the {@link #send(ProducerRecord)} calls which were part of the transaction hit irrecoverable
     * errors, this method will throw the last received exception immediately and the transaction will not be committed.
     * So all {@link #send(ProducerRecord)} calls in a transaction must succeed in order for this method to succeed.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal or abortable error, or for any
     *         other unexpected error
     */
    public void commitTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.beginCommit();
        sender.wakeup();
        result.await();
    }

    /**
     * Aborts the ongoing transaction. Any unflushed produce messages will be aborted when this call is made.
     * This call will throw an exception immediately if any prior {@link #send(ProducerRecord)} calls failed with a
     * {@link ProducerFencedException} or an instance of {@link org.apache.kafka.common.errors.AuthorizationException}.
     *
     * @throws IllegalStateException if no transactional.id has been configured or no transaction has been started
     * @throws ProducerFencedException fatal error indicating another producer with the same transactional.id is active
     * @throws org.apache.kafka.common.errors.UnsupportedVersionException fatal error indicating the broker
     *         does not support transactions (i.e. if its version is lower than 0.11.0.0)
     * @throws org.apache.kafka.common.errors.AuthorizationException fatal error indicating that the configured
     *         transactional.id is not authorized. See the exception for more details
     * @throws KafkaException if the producer has encountered a previous fatal error or for any other unexpected error
     */
    public void abortTransaction() throws ProducerFencedException {
        throwIfNoTransactionManager();
        TransactionalRequestResult result = transactionManager.beginAbort();
        sender.wakeup();
        result.await();
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
     * See {@link #send(ProducerRecord, Callback)} for details.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * <p>
     * The send is asynchronous and this method will return immediately once the record has been stored in the buffer of
     * records waiting to be sent. This allows sending many records in parallel without blocking to wait for the
     * response after each one.
     * <p>
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to, the offset
     * it was assigned and the timestamp of the record. If
     * {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime} is used by the topic, the timestamp
     * will be the user provided timestamp or the record send time if the user did not specify a timestamp for the
     * record. If {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime} is used for the
     * topic, the timestamp will be the Kafka broker local time when the message is appended.
     * <p>
     * Since the send call is asynchronous it returns a {@link java.util.concurrent.Future Future} for the
     * {@link RecordMetadata} that will be assigned to this record. Invoking {@link java.util.concurrent.Future#get()
     * get()} on this future will block until the associated request completes and then return the metadata for the record
     * or throw any exception that occurred while sending the record.
     * <p>
     * If you want to simulate a simple blocking call you can call the <code>get()</code> method immediately:
     *
     * <pre>
     * {@code
     * byte[] key = "key".getBytes();
     * byte[] value = "value".getBytes();
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value)
     * producer.send(record).get();
     * }</pre>
     * <p>
     * Fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     *
     * <pre>
     * {@code
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("the-topic", key, value);
     * producer.send(myRecord,
     *               new Callback() {
     *                   public void onCompletion(RecordMetadata metadata, Exception e) {
     *                       if(e != null) {
     *                          e.printStackTrace();
     *                       } else {
     *                          System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                       }
     *                   }
     *               });
     * }
     * </pre>
     *
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     *
     * <pre>
     * {@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * }
     * </pre>
     * <p>
     * When used as part of a transaction, it is not necessary to define a callback or check the result of the future
     * in order to detect errors from <code>send</code>. If any of the send calls failed with an irrecoverable error,
     * the final {@link #commitTransaction()} call will fail and throw the exception from the last failed send. When
     * this happens, your application should call {@link #abortTransaction()} to reset the state and continue to send
     * data.
     * </p>
     * <p>
     * Some transactional send errors cannot be resolved with a call to {@link #abortTransaction()}.  In particular,
     * if a transactional send finishes with a {@link ProducerFencedException}, a {@link org.apache.kafka.common.errors.OutOfOrderSequenceException},
     * a {@link org.apache.kafka.common.errors.UnsupportedVersionException}, or an
     * {@link org.apache.kafka.common.errors.AuthorizationException}, then the only option left is to call {@link #close()}.
     * Fatal errors cause the producer to enter a defunct state in which future API calls will continue to raise
     * the same underyling error wrapped in a new {@link KafkaException}.
     * </p>
     * <p>
     * It is a similar picture when idempotence is enabled, but no <code>transactional.id</code> has been configured.
     * In this case, {@link org.apache.kafka.common.errors.UnsupportedVersionException} and
     * {@link org.apache.kafka.common.errors.AuthorizationException} are considered fatal errors. However,
     * {@link ProducerFencedException} does not need to be handled. Additionally, it is possible to continue
     * sending after receiving an {@link org.apache.kafka.common.errors.OutOfOrderSequenceException}, but doing so
     * can result in out of order delivery of pending messages. To ensure proper ordering, you should close the
     * producer and create a new instance.
     * </p>
     * <p>
     * If the message format of the destination topic is not upgraded to 0.11.0.0, idempotent and transactional
     * produce requests will fail with an {@link org.apache.kafka.common.errors.UnsupportedForMessageFormatException}
     * error. If this is encountered during a transaction, it is possible to abort and continue. But note that future
     * sends to the same topic will continue receiving the same exception until the topic is upgraded.
     * </p>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     *
     * @param record The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *        indicates no callback)
     *
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException fatal error indicating that the producer is not allowed to write
     * @throws IllegalStateException if a transactional.id has been configured and no transaction has been started, or
     *                               when send is invoked after producer has been closed.
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws SerializationException If the key or value are not valid objects given the configured serializers
     * @throws TimeoutException If the time taken for fetching metadata or allocating memory for the record has surpassed <code>max.block.ms</code>.
     * @throws KafkaException If a Kafka related error occurs that does not belong to the public API exceptions.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // intercept the record, which can be potentially modified; this method does not throw exceptions
        ProducerRecord<K, V> interceptedRecord = this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }

    // Verify that this producer instance has not been closed. This method throws IllegalStateException if the producer
    // has already been closed.
    private void throwIfProducerClosed() {
        if (ioThread == null || !ioThread.isAlive())
            throw new IllegalStateException("Cannot perform operation after producer has been closed");
    }

    /**
     * Implementation of asynchronously send a record to a topic.
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            throwIfProducerClosed();
            // first make sure the metadata for the topic is available
            ClusterAndWaitTime clusterAndWaitTime;
            try {
                /**
                 * todo: 第一步：等待元数据的更新
                 * maxBlockTimeMs：等待最大时长 max.block.ms，默认值60秒
                 */
                clusterAndWaitTime = waitOnMetadata(record.topic(), record.partition(), maxBlockTimeMs);
            } catch (KafkaException e) {
                if (metadata.isClosed())
                    throw new KafkaException("Producer closed while send in progress", e);
                throw e;
            }
            //clusterAndWaitTime.waitedOnMetadataMs：拉取元数据用了多少时间
            //remainingWaitMs还剩多少时间
            long remainingWaitMs = Math.max(0, maxBlockTimeMs - clusterAndWaitTime.waitedOnMetadataMs);
            //把拉取到的源数据赋值给Cluster对象
            Cluster cluster = clusterAndWaitTime.cluster;
            /**
             * todo：第二步：对消息的key和value进行序列化
             */
            byte[] serializedKey;
            try {
                serializedKey = keySerializer.serialize(record.topic(), record.headers(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer", cce);
            }
            byte[] serializedValue;
            try {
                serializedValue = valueSerializer.serialize(record.topic(), record.headers(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer", cce);
            }
            /**
             * todo:第三步：根据分区器选择消息应该发送的分区
             */
            int partition = partition(record, serializedKey, serializedValue, cluster);
            tp = new TopicPartition(record.topic(), partition);

            setReadOnly(record.headers());
            Header[] headers = record.headers().toArray();

            /**
             * todo:第四步：判断要发送的消息有没有超过消息大小的最大值
             * 默认值是1M，我们一般都会修改它
             */
            int serializedSize = AbstractRecords.estimateSizeInBytesUpperBound(apiVersions.maxUsableProduceMagic(),
                    compressionType, serializedKey, serializedValue, headers);
            //计算消息的大小，后面要进行大小的判断
            ensureValidRecordSize(serializedSize);
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
            log.trace("Sending record {} with callback {} to topic {} partition {}", record, callback, record.topic(), partition);

            /**
             * todo:第五步：为异步send设置callback
             */
            // producer callback will make sure to call both 'callback' and interceptor callback
            Callback interceptCallback = new InterceptorCallback<>(callback, this.interceptors, tp);

            if (transactionManager != null && transactionManager.isTransactional())
                transactionManager.maybeAddPartitionToTransaction(tp);

            /**
             * todo:第六步：将消息添加到缓冲区 Accumulator（32MB）
             */
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey,
                    serializedValue, headers, interceptCallback, remainingWaitMs);

            /**
             * todo:第七步：如果批次满了，或者是一个新的批次，唤醒Sender线程来发送消息
             */
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), partition);
                this.sender.wakeup();
            }
            return result.future;
            // handling exceptions and record the errors;
            // for API exceptions return them in the future,
            // for other exceptions throw directly
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            if (callback != null)
                callback.onCompletion(null, e);
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw new InterruptException(e);
        } catch (BufferExhaustedException e) {
            this.errors.record();
            this.metrics.sensor("buffer-exhausted-records").record();
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (KafkaException e) {
            this.errors.record();
            this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            this.interceptors.onSendError(record, tp, e);
            throw e;
        }
    }

    private void setReadOnly(Headers headers) {
        if (headers instanceof RecordHeaders) {
            ((RecordHeaders) headers).setReadOnly();
        }
    }

    /**
     * Wait for cluster metadata including partitions for the given topic to be available.
     * @param topic The topic we want metadata for
     * @param partition A specific partition expected to exist in metadata, or null if there's no preference
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     * @return The cluster containing topic metadata and the amount of time we waited in ms
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    private ClusterAndWaitTime waitOnMetadata(String topic, Integer partition, long maxWaitMs) throws InterruptedException {
        // add topic to metadata topic list if it is not there already and reset expiry
        //第一次Cluster没有数据
        //当从Kafka中拉取到metadata信息后，metadata中就已经有值了
        Cluster cluster = metadata.fetch();

        if (cluster.invalidTopics().contains(topic))
            throw new InvalidTopicException(topic);

        //把topic添加到metadata中
        //第一次添加topic会将needUpdate设置为true
        metadata.add(topic);

        //获得topic对应的分区数量，第一次获得不到
        Integer partitionsCount = cluster.partitionCountForTopic(topic);
        // Return cached metadata if we have it, and if the record's partition is either undefined
        // or within the known partition range
        //如果已经有缓存的数据，直接返回，就不需要从Kafka获得数据了
        if (partitionsCount != null && (partition == null || partition < partitionsCount))
            return new ClusterAndWaitTime(cluster, 0);

        //代码执行到这里表明缓存里没有topic的元数据，需要远程拉取
        //当前时间
        long begin = time.milliseconds();
        //剩余的时间，初始值为最大等待时间
        long remainingWaitMs = maxWaitMs;
        //已经花费的时间
        long elapsed;
        // Issue metadata requests until we have metadata for the topic or maxWaitTimeMs is exceeded.
        // In case we already have cached metadata for the topic, but the requested partition is greater
        // than expected, issue an update request only once. This is necessary in case the metadata
        // is stale and the number of partitions for this topic has increased in the meantime.
        do {
            log.trace("Requesting metadata update for topic {}.", topic);
            metadata.add(topic);
            //needupdate设置为true，获取当前元数据的版本号，初始值为0
            int version = metadata.requestUpdate();
            //唤醒Sender线程
            sender.wakeup();//唤醒Sender线程之后，拉取元数据
            try {
                //等待metadata拉取完毕。当拉取metadata成功之后，被唤醒
                metadata.awaitUpdate(version, remainingWaitMs);
            } catch (TimeoutException ex) {
                // Rethrow with original maxWaitMs to prevent logging exception with remainingWaitMs
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            }
            //能走到这里，说明要么拉取元数据成功，要么已经超时
            //此时cluster中已经有真正的cluster元数据信息了
            cluster = metadata.fetch();
            elapsed = time.milliseconds() - begin;
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            if (cluster.unauthorizedTopics().contains(topic))
                throw new TopicAuthorizationException(topic);
            if (cluster.invalidTopics().contains(topic))
                throw new InvalidTopicException(topic);
            //更新剩余时间
            remainingWaitMs = maxWaitMs - elapsed;
            //如果partitionsCount的值不为null，表明已经成功的获取了元数据
            partitionsCount = cluster.partitionCountForTopic(topic);
        } while (partitionsCount == null);//如果partitionsCount为空，就会一直循环

        if (partition != null && partition >= partitionsCount) {
            throw new KafkaException(
                    String.format("Invalid partition given with record: %d is not in the range [0...%d).", partition, partitionsCount));
        }

        return new ClusterAndWaitTime(cluster, elapsed);
    }

    /**
     * Validate that the record size isn't too large
     */
    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the maximum request size you have configured with the " +
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG +
                    " configuration.");
        if (size > this.totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                    " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                    ProducerConfig.BUFFER_MEMORY_CONFIG +
                    " configuration.");
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed (e.g. <code>Future.isDone() == true</code>).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commit();
     * }
     * </pre>
     *
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     * </p>
     * <p>
     * Applications don't need to call this method for transactional producers, since the {@link #commitTransaction()} will
     * flush all buffered records before performing the commit. This ensures that all the {@link #send(ProducerRecord)}
     * calls made since the previous {@link #beginTransaction()} are completed before the commit.
     * </p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void flush() {
        log.trace("Flushing accumulated records in producer.");
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
    }

    /**
     * Get the partition metadata for the given topic. This can be used for custom partitioning.
     * @throws AuthenticationException if authentication fails. See the exception for more details
     * @throws AuthorizationException if not authorized to the specified topic. See the exception for more details
     * @throws InterruptException if the thread is interrupted while blocked
     * @throws TimeoutException if metadata could not be refreshed within {@code max.block.ms}
     * @throws KafkaException for all Kafka-related exceptions, including the case where this method is called after producer close
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        Objects.requireNonNull(topic, "topic cannot be null");
        try {
            return waitOnMetadata(topic, null, maxBlockTimeMs).cluster.partitionsForTopic(topic);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(0, TimeUnit.MILLISECONDS)</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @param timeUnit The time unit for the <code>timeout</code>
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     */
    @Override
    public void close(long timeout, TimeUnit timeUnit) {
        close(timeout, timeUnit, false);
    }

    private void close(long timeout, TimeUnit timeUnit, boolean swallowException) {
        if (timeout < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        long timeoutMs = timeUnit.toMillis(timeout);
        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeoutMs);

        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeoutMs > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                        "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.",
                        timeoutMs);
            } else {
                // Try to close gracefully.
                if (this.sender != null)
                    this.sender.initiateClose();
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeoutMs);
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, new InterruptException(t));
                        log.error("Interrupted while joining ioThread", t);
                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                    "within timeout {} ms.", timeoutMs);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, new InterruptException(e));
                }
            }
        }

        ClientUtils.closeQuietly(interceptors, "producer interceptors", firstException);
        ClientUtils.closeQuietly(metrics, "producer metrics", firstException);
        ClientUtils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        ClientUtils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        ClientUtils.closeQuietly(partitioner, "producer partitioner", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId, metrics);
        log.debug("Kafka producer has been closed");
        Throwable exception = firstException.get();
        if (exception != null && !swallowException) {
            if (exception instanceof InterruptException) {
                throw (InterruptException) exception;
            }
            throw new KafkaException("Failed to close kafka producer", exception);
        }
    }

    private ClusterResourceListeners configureClusterResourceListeners(Serializer<K> keySerializer, Serializer<V> valueSerializer, List<?>... candidateLists) {
        ClusterResourceListeners clusterResourceListeners = new ClusterResourceListeners();
        for (List<?> candidateList: candidateLists)
            clusterResourceListeners.maybeAddAll(candidateList);

        clusterResourceListeners.maybeAdd(keySerializer);
        clusterResourceListeners.maybeAdd(valueSerializer);
        return clusterResourceListeners;
    }

    /**
     * computes partition for given record.
     * if the record has partition returns the value otherwise
     * calls configured partitioner class to compute the partition.
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey, byte[] serializedValue, Cluster cluster) {
        //如果在ProducerRecord已经指定了分区编号，不会使用分区器
        //在创建ProducerRecord对象时，已经制定了分区编号
        //ProducerRecord(String topic, Integer partition, K key, V value)
        Integer partition = record.partition();
        return partition != null ?
                partition :
                partitioner.partition(
                        record.topic(), record.key(), serializedKey, record.value(), serializedValue, cluster);
    }

    private void throwIfNoTransactionManager() {
        if (transactionManager == null)
            throw new IllegalStateException("Cannot use transactional methods without enabling transactions " +
                    "by setting the " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " configuration property");
    }

    // Visible for testing
    String getClientId() {
        return clientId;
    }

    private static class ClusterAndWaitTime {
        final Cluster cluster;
        final long waitedOnMetadataMs;
        ClusterAndWaitTime(Cluster cluster, long waitedOnMetadataMs) {
            this.cluster = cluster;
            this.waitedOnMetadataMs = waitedOnMetadataMs;
        }
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

    /**
     * A callback called when producer request is complete. It in turn calls user-supplied callback (if given) and
     * notifies producer interceptors about the request completion.
     */
    private static class InterceptorCallback<K, V> implements Callback {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final TopicPartition tp;

        private InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors, TopicPartition tp) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            this.tp = tp;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            metadata = metadata != null ? metadata : new RecordMetadata(tp, -1, -1, RecordBatch.NO_TIMESTAMP, Long.valueOf(-1L), -1, -1);
            this.interceptors.onAcknowledgement(metadata, exception);
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }
    }
}

package com.tiv.kafka.study.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerSample {

    public static final String TOPIC_NAME = "test-topic";

    private static volatile Producer<String, String> producer;

    /**
     * 单例模式创建Kafka Producer
     * Kafka Producer是线程安全的,应当多线程复用
     * 生产中可以把properties的属性值存在配置文件中,把producer作为bean使用,不需要关闭
     *
     * @return
     */
    public static Producer<String, String> producer() {
        if (producer == null) {
            synchronized (ProducerSample.class) {
                if (producer == null) {
                    // 生产者配置
                    Properties properties = new Properties();
                    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                    // properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.tiv.kafka.study.producer.ProducerSample$SamplePartition");

                    // 生产者
                    producer = new KafkaProducer<>(properties);
                }
            }
        }
        return producer;
    }

    /**
     * 异步发送消息
     */
    public static void send() {
        for (int i = 0; i < 10; i++) {
            // 消息对象
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
            producer().send(record);
        }
        // 关闭通道
        // producer.close();
    }

    /**
     * 同步发送消息(异步阻塞发送)
     */
    public static void syncSend() throws ExecutionException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            // 消息对象
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
            Future<RecordMetadata> future = producer().send(record);
            // 阻塞
            RecordMetadata recordMetadata = future.get();
            System.out.printf("key:%s, value:%s, topic:%s, partition:%s, offset:%s%n", key, value, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        }
        // 关闭通道
        // producer.close();
    }

    /**
     * 异步发送带回调
     */
    public static void syncSendWithCallback() {
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            // 消息对象
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
            producer().send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.printf("key:%s, value:%s, topic:%s, partition:%s, offset:%s%n", key, value, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                }
            });
        }
        // 关闭通道
        // producer.close();
    }

    /**
     * 异步发送带自定义分区
     */
    public static void syncSendWithSamplePartition() {
        // properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.tiv.kafka.study.producer.ProducerSample$SamplePartition");
        for (int i = 0; i < 10; i++) {
            // 自定义分区的逻辑和key的格式相关
            String key = "key-" + i;
            String value = "value-" + i;
            // 消息对象
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
            producer().send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.printf("key:%s, value:%s, topic:%s, partition:%s, offset:%s%n", key, value, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                }
            });
        }
        // 关闭通道
        // producer.close();
    }

    /**
     * 自定义分区
     */
    public static class SamplePartition implements Partitioner {

        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            /*
              key-1
              key-2
              key-3
             */
            String keyStr = key.toString();
            int keyNumber = Integer.parseInt(keyStr.substring(keyStr.indexOf("-") + 1));

            List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);

            return keyNumber % partitions.size();
        }

        @Override
        public void close() {

        }

        @Override
        public void configure(Map<String, ?> map) {

        }

    }

}

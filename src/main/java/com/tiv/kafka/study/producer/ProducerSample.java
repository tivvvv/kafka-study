package com.tiv.kafka.study.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerSample {

    public static final String TOPIC_NAME = "test-topic";

    /**
     * 异步发送消息
     */
    public static void send() {
        // 生产者配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 生产者
        Producer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            // 消息对象
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "key-" + i, "value-" + i);
            producer.send(record);
        }
        // 关闭通道
        producer.close();
    }

    /**
     * 同步发送消息(异步阻塞发送)
     */
    public static void syncSend() throws ExecutionException, InterruptedException {
        // 生产者配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 生产者
        Producer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            // 消息对象
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
            Future<RecordMetadata> future = producer.send(record);
            // 阻塞
            RecordMetadata recordMetadata = future.get();
            System.out.printf("key:%s, value:%s, topic:%s, partition:%s, offset:%s%n", key, value, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
        }
        // 关闭通道
        producer.close();
    }

    /**
     * 异步发送带回调
     */
    public static void syncSendWithCallback() {
        // 生产者配置
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // 生产者
        Producer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            // 消息对象
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.printf("key:%s, value:%s, topic:%s, partition:%s, offset:%s%n", key, value, recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                }
            });
        }
        // 关闭通道
        producer.close();
    }

}

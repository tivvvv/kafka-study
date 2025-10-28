package com.tiv.kafka.study.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerSample {

    public static final String TOPIC_NAME = "test-topic";

    /**
     * 消费消息
     */
    public static void consume() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅一个或多个主题
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        System.out.println("消费者开始轮询消息...");
        for (int i = 0; i < 3; i++) {
            System.out.println("第 " + (i + 1) + " 次轮询");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            System.out.println("收到 " + records.count() + " 条消息");
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("key:%s, value:%s, topic:%s, partition:%s, offset:%s%n",
                        record.key(), record.value(), record.topic(), record.partition(), record.offset());
            }
        }
        consumer.close();
        System.out.println("消费者关闭");
    }

    /**
     * 手动提交消费消息
     */
    public static void consumeWithCommitAsync() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        // 关闭自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // 订阅一个或多个主题
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        System.out.println("消费者开始轮询消息...");
        for (int i = 0; i < 3; i++) {
            System.out.println("第 " + (i + 1) + " 次轮询");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            System.out.println("收到 " + records.count() + " 条消息");
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("key:%s, value:%s, topic:%s, partition:%s, offset:%s%n",
                        record.key(), record.value(), record.topic(), record.partition(), record.offset());
            }
            // 手动提交
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("消费者关闭");
    }

}

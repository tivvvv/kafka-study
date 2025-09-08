package com.tiv.kafka.study.admin;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.Properties;

public class AdminSample {

    public static final String TOPIC_NAME = "test-topic";

    public static AdminClient adminClient() {
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    public static void createTopic() {
        AdminClient adminClient = adminClient();
        short rf = 1;
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, rf);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
        System.out.println("createTopic--" + topics);
    }

}

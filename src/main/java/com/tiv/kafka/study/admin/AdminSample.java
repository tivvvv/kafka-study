package com.tiv.kafka.study.admin;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class AdminSample {

    public static final String TOPIC_NAME = "test-topic-1";

    private static volatile AdminClient adminClient;

    /**
     * 单例模式创建AdminClient,连接Kafka
     *
     * @return
     */
    public static AdminClient adminClient() {
        if (adminClient == null) {
            synchronized (AdminSample.class) {
                if (adminClient == null) {
                    Properties properties = new Properties();
                    properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                    adminClient = AdminClient.create(properties);
                }
            }
        }
        return adminClient;
    }

    /**
     * 创建Topic
     */
    public static void createTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        short rf = 1;
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, rf);
        CreateTopicsResult topics = adminClient.createTopics(Arrays.asList(newTopic));
        topics.all().get();
        System.out.println("createTopic--" + topics);
    }

    /**
     * 列出所有Topic
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void listTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> names = listTopicsResult.names().get();
        System.out.println("listTopics--" + names);
    }

    /**
     * 删除Topic
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void delTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList(TOPIC_NAME));
        deleteTopicsResult.all().get();
        System.out.println("delTopic--" + deleteTopicsResult);
    }

    /**
     * 列出配置项
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void listConfigs() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Arrays.asList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        System.out.println("listConfigs--" + configResourceConfigMap);
    }

    /**
     * 修改配置项
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void alterConfig() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();

        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);

        // 设置配置项
        Collection<AlterConfigOp> configEntries = Arrays.asList(
                new AlterConfigOp(new ConfigEntry("preallocate", "true"), AlterConfigOp.OpType.SET));
        configs.put(configResource, configEntries);

        AlterConfigsResult result = adminClient.incrementalAlterConfigs(configs);
        result.all().get();
        System.out.println("alterConfig--" + result);
    }

}

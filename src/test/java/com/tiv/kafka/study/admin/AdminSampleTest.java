package com.tiv.kafka.study.admin;

import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class AdminSampleTest {

    @Test
    public void testAdminClient() {
        AdminSample.adminClient();
    }

    @Test
    public void testCreateTopic() throws Exception {
        AdminSample.createTopic();
    }

    @Test
    public void testListTopics() throws Exception {
        AdminSample.listTopics();
    }

    @Test
    public void testDetailTopic() throws Exception {
        AdminSample.detailTopic();
    }

    @Test
    public void testDelTopic() throws Exception {
        AdminSample.delTopic();
    }

    @Test
    public void testListConfigs() throws Exception {
        AdminSample.listConfigs();
    }

    @Test
    public void testAlterConfig() throws Exception {
        AdminSample.alterConfig();
    }

    @Test
    public void testIncrPartition() throws Exception {
        AdminSample.incrPartition(2);
    }

}
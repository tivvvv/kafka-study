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

}
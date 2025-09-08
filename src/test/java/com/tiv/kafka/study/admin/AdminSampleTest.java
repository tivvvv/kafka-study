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
    public void testCreateTopic() {
        AdminSample.createTopic();
    }

}
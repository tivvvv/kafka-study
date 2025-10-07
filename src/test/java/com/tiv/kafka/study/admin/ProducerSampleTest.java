package com.tiv.kafka.study.admin;

import com.tiv.kafka.study.producer.ProducerSample;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ProducerSampleTest {

    @Test
    public void testSend() {
        ProducerSample.send();
    }

}

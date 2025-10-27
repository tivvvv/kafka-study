package com.tiv.kafka.study.admin;

import com.tiv.kafka.study.consumer.ConsumerSample;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class ConsumerSampleTest {

    @Test
    public void testConsume() {
        ConsumerSample.consume();
    }

}

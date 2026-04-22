package com.example.brokerdemo.brokers.kafka;

import com.example.brokerdemo.brokers.DemoNames;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
  @Bean
  NewTopic kafkaDemoTopic(DemoNames names) {
    return new NewTopic(names.kafka().topic(), 1, (short) 1);
  }
}


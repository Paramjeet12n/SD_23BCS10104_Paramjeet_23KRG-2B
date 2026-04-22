package com.example.brokerdemo.brokers;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "demo")
public record DemoNames(
    Kafka kafka,
    Rabbitmq rabbitmq,
    Redis redis
) {
  public record Kafka(String topic) {}
  public record Rabbitmq(String queue) {}
  public record Redis(String channel) {}
}


package com.example.brokerdemo.consumer;

import com.example.brokerdemo.brokers.redis.RedisSubscriber;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Component
public class ListenerToggler {
  private final KafkaListenerEndpointRegistry kafkaRegistry;
  private final RabbitListenerEndpointRegistry rabbitRegistry;
  private final RedisSubscriber redisSubscriber;

  public ListenerToggler(
      KafkaListenerEndpointRegistry kafkaRegistry,
      RabbitListenerEndpointRegistry rabbitRegistry,
      RedisSubscriber redisSubscriber
  ) {
    this.kafkaRegistry = kafkaRegistry;
    this.rabbitRegistry = rabbitRegistry;
    this.redisSubscriber = redisSubscriber;
  }

  public void apply(boolean enabled) {
    toggleKafka(enabled);
    toggleRabbit(enabled);
    // Redis demonstrates message loss by unsubscribing while OFF.
    redisSubscriber.refreshSubscription();
  }

  private void toggleKafka(boolean enabled) {
    var container = kafkaRegistry.getListenerContainer("kafkaDemoListener");
    if (container == null) return;
    if (enabled && !container.isRunning()) container.start();
    if (!enabled && container.isRunning()) container.stop();
  }

  private void toggleRabbit(boolean enabled) {
    var container = rabbitRegistry.getListenerContainer("rabbitDemoListener");
    if (container == null) return;
    if (enabled && !container.isRunning()) container.start();
    if (!enabled && container.isRunning()) container.stop();
  }
}


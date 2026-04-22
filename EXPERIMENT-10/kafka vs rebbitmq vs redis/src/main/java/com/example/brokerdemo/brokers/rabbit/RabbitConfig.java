package com.example.brokerdemo.brokers.rabbit;

import com.example.brokerdemo.brokers.DemoNames;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.boot.autoconfigure.amqp.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.springframework.amqp.core.AcknowledgeMode.MANUAL;

@Configuration
public class RabbitConfig {
  @Bean
  Queue demoQueue(DemoNames names) {
    return new Queue(names.rabbitmq().queue(), true);
  }

  @Bean
  MessageConverter messageConverter() {
    return new SimpleMessageConverter();
  }

  @Bean(name = "manualAckRabbitListenerContainerFactory")
  SimpleRabbitListenerContainerFactory manualAckRabbitListenerContainerFactory(
      SimpleRabbitListenerContainerFactoryConfigurer configurer,
      ConnectionFactory connectionFactory
  ) {
    SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
    configurer.configure(factory, connectionFactory);
    factory.setAcknowledgeMode(MANUAL);
    factory.setPrefetchCount(1);
    return factory;
  }
}


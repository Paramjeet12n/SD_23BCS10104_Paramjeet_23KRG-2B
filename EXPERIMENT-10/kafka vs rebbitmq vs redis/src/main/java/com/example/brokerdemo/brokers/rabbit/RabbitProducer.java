package com.example.brokerdemo.brokers.rabbit;

import com.example.brokerdemo.brokers.DemoNames;
import com.example.brokerdemo.brokers.MessageEnvelope;
import com.example.brokerdemo.logs.BrokerType;
import com.example.brokerdemo.logs.InMemoryLogStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class RabbitProducer {
  private final RabbitTemplate rabbitTemplate;
  private final DemoNames names;
  private final InMemoryLogStore logStore;
  private final ObjectMapper objectMapper;

  public RabbitProducer(RabbitTemplate rabbitTemplate, DemoNames names, InMemoryLogStore logStore, ObjectMapper objectMapper) {
    this.rabbitTemplate = rabbitTemplate;
    this.names = names;
    this.logStore = logStore;
    this.objectMapper = objectMapper;
  }

  public String send(String message) {
    String id = UUID.randomUUID().toString();
    MessageEnvelope env = new MessageEnvelope(id, System.currentTimeMillis(), message);
    String body;
    try {
      body = objectMapper.writeValueAsString(env);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize message envelope", e);
    }

    rabbitTemplate.convertAndSend(names.rabbitmq().queue(), body);
    logStore.add("SENT", BrokerType.rabbitmq, message, id, null);
    return id;
  }
}


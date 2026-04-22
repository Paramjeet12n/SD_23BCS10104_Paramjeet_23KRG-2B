package com.example.brokerdemo.brokers.kafka;

import com.example.brokerdemo.brokers.MessageEnvelope;
import com.example.brokerdemo.brokers.DemoNames;
import com.example.brokerdemo.logs.BrokerType;
import com.example.brokerdemo.logs.InMemoryLogStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class KafkaProducer {
  private final KafkaTemplate<String, String> kafkaTemplate;
  private final DemoNames names;
  private final InMemoryLogStore logStore;
  private final ObjectMapper objectMapper;

  public KafkaProducer(
      KafkaTemplate<String, String> kafkaTemplate,
      DemoNames names,
      InMemoryLogStore logStore,
      ObjectMapper objectMapper
  ) {
    this.kafkaTemplate = kafkaTemplate;
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

    kafkaTemplate.send(names.kafka().topic(), body);
    logStore.add("SENT", BrokerType.kafka, message, id, null);
    return id;
  }
}


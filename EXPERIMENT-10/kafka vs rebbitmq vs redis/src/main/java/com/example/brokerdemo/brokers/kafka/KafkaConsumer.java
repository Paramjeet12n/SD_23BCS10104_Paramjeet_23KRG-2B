package com.example.brokerdemo.brokers.kafka;

import com.example.brokerdemo.brokers.MessageEnvelope;
import com.example.brokerdemo.consumer.ConsumerControl;
import com.example.brokerdemo.logs.BrokerType;
import com.example.brokerdemo.logs.InMemoryLogStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
  private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

  private final ConsumerControl consumerControl;
  private final InMemoryLogStore logStore;
  private final ObjectMapper objectMapper;

  public KafkaConsumer(ConsumerControl consumerControl, InMemoryLogStore logStore, ObjectMapper objectMapper) {
    this.consumerControl = consumerControl;
    this.logStore = logStore;
    this.objectMapper = objectMapper;
  }

  @KafkaListener(id = "kafkaDemoListener", topics = "${demo.kafka.topic}")
  public void onMessage(String message, Acknowledgment ack) {
    MessageEnvelope env;
    try {
      env = objectMapper.readValue(message, MessageEnvelope.class);
    } catch (Exception e) {
      env = new MessageEnvelope(null, 0L, message);
    }

    if (!consumerControl.isEnabled()) {
      log.info("Kafka consumer STOPPED; leaving offset unacked (message will be consumed later): {}", env.payload());
      logStore.add("RECEIVED_BUT_STOPPED", BrokerType.kafka, env.payload(), env.id(), null);
      return;
    }

    long latencyMs = env.sentAtEpochMs() > 0 ? (System.currentTimeMillis() - env.sentAtEpochMs()) : -1;
    Long latency = latencyMs >= 0 ? latencyMs : null;

    log.info("Kafka received: {} (latencyMs={})", env.payload(), latency);
    logStore.add("RECEIVED", BrokerType.kafka, env.payload(), env.id(), latency);
    ack.acknowledge();
  }
}


package com.example.brokerdemo.brokers.rabbit;

import com.example.brokerdemo.brokers.MessageEnvelope;
import com.example.brokerdemo.consumer.ConsumerControl;
import com.example.brokerdemo.logs.BrokerType;
import com.example.brokerdemo.logs.InMemoryLogStore;
import com.rabbitmq.client.Channel;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Component
public class RabbitConsumer {
  private static final Logger log = LoggerFactory.getLogger(RabbitConsumer.class);

  private final ConsumerControl consumerControl;
  private final InMemoryLogStore logStore;
  private final ObjectMapper objectMapper;

  public RabbitConsumer(ConsumerControl consumerControl, InMemoryLogStore logStore, ObjectMapper objectMapper) {
    this.consumerControl = consumerControl;
    this.logStore = logStore;
    this.objectMapper = objectMapper;
  }

  @RabbitListener(
      id = "rabbitDemoListener",
      queues = "${demo.rabbitmq.queue}",
      containerFactory = "manualAckRabbitListenerContainerFactory"
  )
  public void onMessage(Message message, Channel channel) throws IOException {
    long tag = message.getMessageProperties().getDeliveryTag();
    String raw = new String(message.getBody(), StandardCharsets.UTF_8);
    MessageEnvelope env;
    try {
      env = objectMapper.readValue(raw, MessageEnvelope.class);
    } catch (Exception e) {
      env = new MessageEnvelope(null, 0L, raw);
    }

    if (!consumerControl.isEnabled()) {
      // When consumer is disabled we stop the listener container, so this should be rare.
      // Requeueing in a tight loop spams logs, so just requeue and return without extra logging.
      channel.basicReject(tag, true);
      return;
    }

    long latencyMs = env.sentAtEpochMs() > 0 ? (System.currentTimeMillis() - env.sentAtEpochMs()) : -1;
    Long latency = latencyMs >= 0 ? latencyMs : null;

    log.info("Rabbit received: {} (latencyMs={})", env.payload(), latency);
    logStore.add("RECEIVED", BrokerType.rabbitmq, env.payload(), env.id(), latency);
    channel.basicAck(tag, false);
  }
}


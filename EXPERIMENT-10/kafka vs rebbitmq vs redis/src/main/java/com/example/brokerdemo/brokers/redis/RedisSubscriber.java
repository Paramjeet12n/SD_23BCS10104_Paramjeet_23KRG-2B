package com.example.brokerdemo.brokers.redis;

import com.example.brokerdemo.brokers.MessageEnvelope;
import com.example.brokerdemo.consumer.ConsumerControl;
import com.example.brokerdemo.logs.BrokerType;
import com.example.brokerdemo.logs.InMemoryLogStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class RedisSubscriber implements MessageListener {
  private static final Logger log = LoggerFactory.getLogger(RedisSubscriber.class);

  private final ConsumerControl consumerControl;
  private final InMemoryLogStore logStore;
  private final RedisMessageListenerContainer container;
  private final ChannelTopic topic;
  private final ObjectMapper objectMapper;

  private volatile boolean subscribed = false;

  public RedisSubscriber(
      ConsumerControl consumerControl,
      InMemoryLogStore logStore,
      RedisMessageListenerContainer container,
      ChannelTopic topic,
      ObjectMapper objectMapper
  ) {
    this.consumerControl = consumerControl;
    this.logStore = logStore;
    this.container = container;
    this.topic = topic;
    this.objectMapper = objectMapper;
  }

  @PostConstruct
  void init() {
    refreshSubscription();
  }

  public String channel() {
    return topic.getTopic();
  }

  public synchronized void refreshSubscription() {
    boolean enabled = consumerControl.isEnabled();
    if (enabled && !subscribed) {
      container.addMessageListener(this, topic);
      container.start();
      subscribed = true;
      log.info("Redis subscriber STARTED (subscribed to {})", topic.getTopic());
      logStore.add("CONSUMER_STARTED", BrokerType.redis, "subscribed=" + topic.getTopic());
      return;
    }

    if (!enabled && subscribed) {
      container.removeMessageListener(this, topic);
      container.stop();
      subscribed = false;
      log.info("Redis subscriber STOPPED (unsubscribed from {})", topic.getTopic());
      logStore.add("CONSUMER_STOPPED", BrokerType.redis, "unsubscribed=" + topic.getTopic());
    }
  }

  @Override
  public void onMessage(Message message, byte[] pattern) {
    String raw = new String(message.getBody(), StandardCharsets.UTF_8);
    MessageEnvelope env;
    try {
      env = objectMapper.readValue(raw, MessageEnvelope.class);
    } catch (Exception e) {
      env = new MessageEnvelope(null, 0L, raw);
    }

    long latencyMs = env.sentAtEpochMs() > 0 ? (System.currentTimeMillis() - env.sentAtEpochMs()) : -1;
    Long latency = latencyMs >= 0 ? latencyMs : null;

    log.info("Redis received: {} (latencyMs={})", env.payload(), latency);
    logStore.add("RECEIVED", BrokerType.redis, env.payload(), env.id(), latency);
  }
}


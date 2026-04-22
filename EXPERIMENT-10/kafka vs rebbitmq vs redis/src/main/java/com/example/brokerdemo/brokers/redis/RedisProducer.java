package com.example.brokerdemo.brokers.redis;

import com.example.brokerdemo.brokers.MessageEnvelope;
import com.example.brokerdemo.logs.BrokerType;
import com.example.brokerdemo.logs.InMemoryLogStore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class RedisProducer {
  private final StringRedisTemplate redisTemplate;
  private final RedisSubscriber redisSubscriber;
  private final InMemoryLogStore logStore;
  private final ObjectMapper objectMapper;

  public RedisProducer(
      StringRedisTemplate redisTemplate,
      RedisSubscriber redisSubscriber,
      InMemoryLogStore logStore,
      ObjectMapper objectMapper
  ) {
    this.redisTemplate = redisTemplate;
    this.redisSubscriber = redisSubscriber;
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

    redisTemplate.convertAndSend(redisSubscriber.channel(), body);
    logStore.add("SENT", BrokerType.redis, message, id, null);
    return id;
  }
}


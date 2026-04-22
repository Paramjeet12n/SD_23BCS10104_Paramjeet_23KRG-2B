package com.example.brokerdemo.api;

import com.example.brokerdemo.brokers.kafka.KafkaProducer;
import com.example.brokerdemo.brokers.rabbit.RabbitProducer;
import com.example.brokerdemo.brokers.redis.RedisProducer;
import com.example.brokerdemo.brokers.redis.RedisSubscriber;
import com.example.brokerdemo.consumer.ConsumerControl;
import com.example.brokerdemo.consumer.ListenerToggler;
import com.example.brokerdemo.logs.BrokerType;
import com.example.brokerdemo.logs.InMemoryLogStore;
import com.example.brokerdemo.logs.LogEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
public class DemoController {
  private static final Logger log = LoggerFactory.getLogger(DemoController.class);

  private final KafkaProducer kafkaProducer;
  private final RabbitProducer rabbitProducer;
  private final RedisProducer redisProducer;
  private final RedisSubscriber redisSubscriber;
  private final ConsumerControl consumerControl;
  private final ListenerToggler listenerToggler;
  private final InMemoryLogStore logStore;

  public DemoController(
      KafkaProducer kafkaProducer,
      RabbitProducer rabbitProducer,
      RedisProducer redisProducer,
      RedisSubscriber redisSubscriber,
      ConsumerControl consumerControl,
      ListenerToggler listenerToggler,
      InMemoryLogStore logStore
  ) {
    this.kafkaProducer = kafkaProducer;
    this.rabbitProducer = rabbitProducer;
    this.redisProducer = redisProducer;
    this.redisSubscriber = redisSubscriber;
    this.consumerControl = consumerControl;
    this.listenerToggler = listenerToggler;
    this.logStore = logStore;
  }

  @PostMapping("/send")
  public ResponseEntity<?> send(@RequestBody SendRequest req) {
    if (req == null || req.message() == null || req.message().isBlank() || req.broker() == null) {
      return ResponseEntity.badRequest().body(Map.of("error", "Body must be: { message: string, broker: kafka|rabbitmq|redis }"));
    }

    BrokerType broker = req.broker();
    String msg = req.message().trim();

    log.info("POST /send broker={} message={}", broker, msg);
    String id;
    switch (broker) {
      case kafka -> id = kafkaProducer.send(msg);
      case rabbitmq -> id = rabbitProducer.send(msg);
      case redis -> id = redisProducer.send(msg);
      default -> id = null;
    }

    return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("ok", true, "id", id));
  }

  @GetMapping("/logs")
  public List<LogEvent> logs() {
    return logStore.snapshot();
  }

  @PostMapping("/logs/clear")
  public Map<String, Object> clear() {
    logStore.clear();
    return Map.of("ok", true);
  }

  @GetMapping("/consumer")
  public Map<String, Object> consumer() {
    return Map.of("enabled", consumerControl.isEnabled());
  }

  @PostMapping("/consumer")
  public Map<String, Object> setConsumer(@RequestBody ConsumerToggleRequest req) {
    boolean enabled = consumerControl.setEnabled(req != null && req.enabled());

    // Stop/start listener containers to avoid tight requeue loops and log spam.
    listenerToggler.apply(enabled);

    logStore.add("CONSUMER_TOGGLE", BrokerType.kafka, "enabled=" + enabled);
    logStore.add("CONSUMER_TOGGLE", BrokerType.rabbitmq, "enabled=" + enabled);
    logStore.add("CONSUMER_TOGGLE", BrokerType.redis, "enabled=" + enabled);

    return Map.of("enabled", enabled);
  }
}


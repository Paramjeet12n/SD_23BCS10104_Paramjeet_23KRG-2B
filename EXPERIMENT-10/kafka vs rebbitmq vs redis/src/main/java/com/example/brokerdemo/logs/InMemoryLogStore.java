package com.example.brokerdemo.logs;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.stereotype.Component;

@Component
public class InMemoryLogStore {
  private static final int MAX = 500;
  private final CopyOnWriteArrayList<LogEvent> events = new CopyOnWriteArrayList<>();

  public void add(String type, BrokerType broker, String message) {
    add(type, broker, message, null, null);
  }

  public void add(String type, BrokerType broker, String message, String id, Long latencyMs) {
    events.add(new LogEvent(Instant.now(), type, broker, message, id, latencyMs));
    trimIfNeeded();
  }

  public List<LogEvent> snapshot() {
    return new ArrayList<>(events);
  }

  public void clear() {
    events.clear();
  }

  private void trimIfNeeded() {
    int extra = events.size() - MAX;
    if (extra <= 0) return;
    for (int i = 0; i < extra; i++) {
      if (events.isEmpty()) return;
      events.remove(0);
    }
  }
}


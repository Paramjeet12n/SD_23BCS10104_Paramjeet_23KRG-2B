package com.example.brokerdemo.consumer;

import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.stereotype.Component;

@Component
public class ConsumerControl {
  private final AtomicBoolean enabled = new AtomicBoolean(true);

  public boolean isEnabled() {
    return enabled.get();
  }

  public boolean setEnabled(boolean value) {
    enabled.set(value);
    return enabled.get();
  }
}


package com.example.brokerdemo.logs;

import java.time.Instant;

public record LogEvent(
    Instant ts,
    String type,
    BrokerType broker,
    String message,
    String id,
    Long latencyMs
) {}


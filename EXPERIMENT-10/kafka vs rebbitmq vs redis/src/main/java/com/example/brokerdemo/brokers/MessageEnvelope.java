package com.example.brokerdemo.brokers;

public record MessageEnvelope(
    String id,
    long sentAtEpochMs,
    String payload
) {}


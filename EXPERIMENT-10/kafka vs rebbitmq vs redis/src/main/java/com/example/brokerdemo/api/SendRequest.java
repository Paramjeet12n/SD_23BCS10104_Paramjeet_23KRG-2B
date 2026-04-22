package com.example.brokerdemo.api;

import com.example.brokerdemo.logs.BrokerType;

public record SendRequest(
    String message,
    BrokerType broker
) {}


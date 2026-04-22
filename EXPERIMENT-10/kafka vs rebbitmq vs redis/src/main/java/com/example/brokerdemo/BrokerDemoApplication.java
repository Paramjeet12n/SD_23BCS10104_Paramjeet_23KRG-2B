package com.example.brokerdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class BrokerDemoApplication {
  public static void main(String[] args) {
    SpringApplication.run(BrokerDemoApplication.class, args);
  }
}


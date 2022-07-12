package com.thoughtworks.kafka.workshop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MarketPlaceEventConsumerApplication {

  public static void main(String[] args) {
    SpringApplication.run(MarketPlaceEventConsumerApplication.class, args);
  }
}

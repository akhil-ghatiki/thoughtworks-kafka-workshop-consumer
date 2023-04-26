package com.thoughtworks.kafka.workshop.config;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
@Slf4j
public class MarketPlaceEventsConsumerConfig {

  @Value(value = "${kafka.backoff.interval}")
  private Long interval;

  @Value(value = "${kafka.backoff.max_failure}")
  private Long maxAttempts;

  @Bean
  public ConsumerFactory<Integer, String> consumerFactory() {
    Map<String, Object> properties = new HashMap<>();
    properties.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "market-place-events-listener-group");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    return new DefaultKafkaConsumerFactory<>(properties);
  }

  @Bean
  public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
        new ConcurrentKafkaListenerContainerFactory<>();
    factory.setCommonErrorHandler(errorHandler());
    factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
    factory.afterPropertiesSet();
    factory.setConsumerFactory(consumerFactory());
    // factory.setConcurrency(3);
    // TODO  Runs 3 consumers on three different threads. This is recommended when you are not running
    //  your application in a cloud


    // factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    return factory;
  }

  @Bean
  public DefaultErrorHandler errorHandler() {
    BackOff fixedBackOff = new FixedBackOff(interval, maxAttempts);
    DefaultErrorHandler errorHandler = new DefaultErrorHandler((consumerRecord, exception) -> {
      log.info("All attempts exhausted for Consume Record: {}", consumerRecord);
    }, fixedBackOff);
    return errorHandler;
  }
}

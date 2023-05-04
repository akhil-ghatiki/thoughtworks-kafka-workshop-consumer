package com.thoughtworks.kafka.workshop.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MarketPlaceEventsConsumer {

  /**
   * Use only one consumer. If you plan to use this consumer, please comment the @Component of
   * MarketPlaceEventsConsumerManualAcknowledgement.java This will not create the bean of other
   * consumer.
   */
  @KafkaListener(
      topics = {"market-place-events"},
      groupId = "market-place-events-listener-group")
  public void onMessage(ConsumerRecord<Integer, String> consumerRecord) {
    log.info("Consume Record: {}", consumerRecord);
    throw new RuntimeException("Forced exception");
  }
}

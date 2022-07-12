package com.thoughtworks.kafka.workshop.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

// @Component
@Slf4j
public class MarketPlaceEventsConsumerManualAcknowledgement
    implements AcknowledgingMessageListener<Integer, String> {

  /**
   * Use only one consumer. If you plan to use this consumer, please comment the @Component of
   * MarketPlaceEventsConsumer.java This will not create the bean of other consumer.
   */
  @Override
  @KafkaListener(
      topics = {"market-place-events"},
      groupId = "market-place-events-listener-group")
  public void onMessage(
      ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
    log.info("Consume Record: {}", consumerRecord);
    acknowledgment.acknowledge();
  }
}

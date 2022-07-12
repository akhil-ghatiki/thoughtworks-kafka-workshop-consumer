package com.thoughtworks.kafka.workshop.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
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

    // TODO - exercise 1 - acknowledge the consumption here.

    // TODO - exercise 2 - [Pre-requisites - exercise 0 and exercise 1]
    // ...........................................................................................................................
    //  1. Run the consumer --> 2.Produce the payloads via producer --> 3. Check that these payloads
    // are consumed by this consumer
    //  4. Stop the consumer --> 5.Comment the code you wrote as part of exercise 1 --> 6. Re-run
    // the consumer
    //  7. Produce the payloads via producer --> 8. Check that these payloads are consumed by this
    // consumer
    //  9. Now kill the consumer and re-run --> 10.What did you observe ? :)
    //
    // ...........................................................................................................................
  }
}

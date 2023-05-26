package com.thoughtworks.kafka.workshop.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

//@Component
@Slf4j
public class MarketPlaceEventsConsumerWithNonBlockingRetry {

    /**
     * Use only one consumer. If you plan to use this consumer, please comment the @Component of
     * others. This will not create the bean of other consumer.
     */
    //TODO: Handle the exception here and send event to DLT. Hint: Explore RetryableTopic
    @KafkaListener(id = "market-place-events-listener-group", topics = {"market-place-events"})
    public void onMessage(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Received message: {} from topic: {}", message, topic);
        throw new RuntimeException("Test exception");
    }

    @DltHandler
    public void handleDlt(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Message: {} handled by dlq topic: {}", message, topic);
    }
}

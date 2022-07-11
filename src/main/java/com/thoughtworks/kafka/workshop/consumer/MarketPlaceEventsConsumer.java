package com.thoughtworks.kafka.workshop.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class MarketPlaceEventsConsumer {

    @KafkaListener(topics = {"market-place-events"}, groupId = "market-place-events-listener-group")
    public void onMessage(ConsumerRecord<Integer,String> consumerRecord){
        log.info("Consume Record: {}", consumerRecord);
    }
}

package com.cetin.education.spring.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * @Author mehmetali.cetin
 * @Date 2021-12-07
 */

//@Component
@Slf4j
public class LibraryEventConsumerManuelOffset implements AcknowledgingMessageListener<Integer, String> {
    @Override
    @KafkaListener(topics = {"library-events"})
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
        log.info("Consumer Record : {} ",consumerRecord);
        acknowledgment.acknowledge(); // commit to offset.
    }
}

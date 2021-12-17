package com.cetin.education.spring.kafka.service;

import com.cetin.education.spring.kafka.entity.Book;
import com.cetin.education.spring.kafka.entity.LibraryEvent;
import com.cetin.education.spring.kafka.jpa.LibraryEventsRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import javax.transaction.Transactional;
import java.io.DataInput;
import java.io.IOException;
import java.util.Optional;

/**
 * @Author mehmetali.cetin
 * @Date 2021-12-09
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventService {
    private final ObjectMapper objectMapper;
    private final LibraryEventsRepository libraryEventsRepository;
    private final KafkaTemplate<Integer, String> kafkaTemplate;

    public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value().toString(), LibraryEvent.class);
        log.info("libraryEvent : {}", libraryEvent);

        switch (libraryEvent.getLibraryEventType()){
            case NEW: // save operation
                save(libraryEvent);
            case UPDATE: // update operation
                //validate
                validate(libraryEvent);
                save(libraryEvent);
            default:
                log.info("Invalid Library Event Type");
        }
    }

    private void validate(LibraryEvent libraryEvent) {
        if(libraryEvent.getLibraryEventId() == null){
            throw new IllegalArgumentException("Library Event is missing");
        }
        if(libraryEvent.getLibraryEventId() == 000){
            throw new RecoverableDataAccessException("Recoverable Exception");
        }
        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        libraryEventOptional.orElseThrow(()-> new IllegalArgumentException("Not a valid library Event"));
        log.info("Invalid Library Event Type");
    }

    @Transactional
    public void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully Persisted The Library Event {} ", libraryEvent);
    }

    public void handleRecovery(ConsumerRecord<Integer, String> consumerRecord){
        Integer key = consumerRecord.key();
        String value = consumerRecord.value();
        final ListenableFuture<SendResult<Integer, String>> listenableFuture = kafkaTemplate.sendDefault(key,value);
        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<Integer, String>>() {
            @Override
            public void onFailure(Throwable ex) {
                handleFailure(key, value, ex);
            }

            @Override
            public void onSuccess(SendResult<Integer, String> result) {
                handleSuccess(key, value, result);
            }
        });
    }

    private void handleFailure(Integer key, String value, Throwable ex) {
        log.error("Error sending a message and the exception is {} and the key : {} and the value is {}",ex.getMessage(), key, value);
        try{
            throw ex;
        } catch (Throwable throwable){
            log.error("Error in OnFailure: {}", throwable.getMessage());
        }
    }

    private void handleSuccess(Integer key, String value, SendResult<Integer, String> result) {
        log.info("Message sent successfully for the key : {} and the value is {} , partition is {}", key, value, result.getRecordMetadata().partition());
    }
}

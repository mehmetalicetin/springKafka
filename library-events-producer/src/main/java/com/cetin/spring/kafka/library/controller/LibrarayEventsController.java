package com.cetin.spring.kafka.library.controller;

import com.cetin.spring.kafka.library.domain.LibraryEvent;
import com.cetin.spring.kafka.library.domain.LibraryEventType;
import com.cetin.spring.kafka.library.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibrarayEventsController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/vi/libraryevent")
    public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) throws JsonProcessingException, ExecutionException, InterruptedException, TimeoutException {
        log.info("Before send message");
//        libraryEventProducer.sendLibraryEvent(libraryEvent);
//        SendResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);

        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEvent_Approach2(libraryEvent);

//        log.info("SendResult is {}"+sendResult.getRecordMetadata());
        log.info("After send message");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}

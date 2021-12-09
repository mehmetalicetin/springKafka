package com.cetin.education.spring.kafka.service;

import com.cetin.education.spring.kafka.entity.Book;
import com.cetin.education.spring.kafka.entity.LibraryEvent;
import com.cetin.education.spring.kafka.jpa.LibraryEventsRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

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
}

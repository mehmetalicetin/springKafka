package com.cetin.spring.kafka.library.unit.controller;

import com.cetin.spring.kafka.library.domain.Book;
import com.cetin.spring.kafka.library.domain.LibraryEvent;
import com.cetin.spring.kafka.library.producer.LibraryEventProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.RequestMatcher;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.ResultMatcher;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest
@AutoConfigureMockMvc
public class LibraryEventControllerUnitTest {
    @Autowired
    MockMvc mockMvc;

    @MockBean
    LibraryEventProducer libraryEventProducer;

    ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void postLibraryEvent() throws Exception {
        Book book = Book.builder()
                .bookId(123)
                .bookAuthor("Ali")
                .bookName("Head First Kafka")
                .build();

        LibraryEvent libraryEvent = LibraryEvent
                .builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String value = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        RequestBuilder requestBuilder = post("/vi/libraryevent")
                .content(value)
                .contentType(MediaType.APPLICATION_JSON);

        ResultMatcher resultMatcher = status().isCreated();
        mockMvc.perform(requestBuilder)
                .andExpect(resultMatcher);
    }

    @Test
    void postLibraryEvent_4xx() throws Exception {
        Book book = Book.builder()
                .bookId(null)
                .bookAuthor(null)
                .bookName("Head First Kafka")
                .build();

        LibraryEvent libraryEvent = LibraryEvent
                .builder()
                .libraryEventId(null)
                .book(book)
                .build();

        String value = objectMapper.writeValueAsString(libraryEvent);

        when(libraryEventProducer.sendLibraryEvent_Approach2(isA(LibraryEvent.class))).thenReturn(null);

        RequestBuilder requestBuilder = post("/vi/libraryevent")
                .content(value)
                .contentType(MediaType.APPLICATION_JSON);

        ResultMatcher resultMatcher = status().is4xxClientError();

        String errorMessage = "book.bookAuthor - must not be blank, book.bookId - must not be null";
        mockMvc.perform(requestBuilder)
                .andExpect(resultMatcher)
                .andExpect(content().string(errorMessage));
    }
}

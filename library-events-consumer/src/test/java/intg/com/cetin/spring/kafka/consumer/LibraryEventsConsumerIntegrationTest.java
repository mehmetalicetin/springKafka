package com.cetin.spring.kafka.consumer;

import com.cetin.education.spring.kafka.consumer.LibraryEventConsumer;
import com.cetin.education.spring.kafka.entity.Book;
import com.cetin.education.spring.kafka.entity.LibraryEvent;
import com.cetin.education.spring.kafka.entity.LibraryEventType;
import com.cetin.education.spring.kafka.jpa.LibraryEventsRepository;
import com.cetin.education.spring.kafka.service.LibraryEventService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import javax.persistence.NoResultException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.sun.tools.doclint.Entity.times;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

/**
 * @Author mehmetali.cetin
 * @Date 2021-12-09
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.boostrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.boostrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafka;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry endpointRegistry;

    @SpyBean
    LibraryEventConsumer libraryEventConsumerSpy;

    @SpyBean
    LibraryEventService libraryEventServiceSpy;

    @SpyBean
    LibraryEventsRepository libraryEventsRepository;

    @SpyBean
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for (MessageListenerContainer messageListenerContainer : endpointRegistry.getListenerContainers()){
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafka.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll(); // @Test ile insert edilen data'yı in memory db'den siliyor.
    }

    @Test
    void publishNewLibraryEvent() throws ExecutionException, InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventId\":999,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Head First Kafka\",\"bookAuthor\":\"Karaman\"}}";
        kafkaTemplate.sendDefault(json).get();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        verify(libraryEventConsumerSpy,times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        List<LibraryEvent> repositoryAll = (List<LibraryEvent>) libraryEventsRepository.findAll();

        repositoryAll.forEach(list ->{
            assert list.getLibraryEventId() != null;
            Assertions.assertEquals(123, list.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws InterruptedException, JsonProcessingException {
        String json = "{\"libraryEventId\":999,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Head First Kafka\",\"bookAuthor\":\"Karaman\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book book = Book.builder().bookAuthor("Alanya").bookName("Head First Kafka").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(book);

        String updateJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(updateJson);


        //Block the thread
        CountDownLatch countDownLatch = new CountDownLatch(1);
        countDownLatch.await(3, TimeUnit.SECONDS);

        verify(libraryEventConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy,times(1)).processLibraryEvent(isA(ConsumerRecord.class));

        final Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(999);
        libraryEventOptional.filter(event->event.getBook().getBookName().equalsIgnoreCase("Alanya"))
                .orElseThrow(()->new NoResultException("No Find Any Result by Book Name is Alanya"));

    }

    @Test
    void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 123;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        System.out.println(json);
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        verify(libraryEventConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEventId);
        assertFalse(libraryEventOptional.isPresent());
    }

    @Test
    void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        verify(libraryEventConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishModifyLibraryEvent_000_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 000;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        //verify(libraryEventConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));
        verify(libraryEventServiceSpy, atLeast(1)).handleRecovery(isA(ConsumerRecord.class));
    }
}

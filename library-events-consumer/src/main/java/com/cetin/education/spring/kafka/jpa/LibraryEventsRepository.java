package com.cetin.education.spring.kafka.jpa;

import com.cetin.education.spring.kafka.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

/**
 * @Author mehmetali.cetin
 * @Date 2021-12-09
 */
@Repository
public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer> {
}

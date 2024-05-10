package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.BookRepository;
import com.learnkafka.jpa.LibraryEventsRepository;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class LibraryEventsService {
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    private LibraryEventsRepository libraryEventsRepository;

    @Autowired
    private BookRepository bookRepository;
    
    public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {
        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        log.info("libraryEvent {}", libraryEvent);
        
        switch (libraryEvent.getLibraryEventType()){
            case NEW:
                //Save Operation
                save(libraryEvent);
                break;
            case UPDATE:  
                //update operation
                break;
            default:
                log.info("Invalid library Event Type");
                
        }
    }
    @Transactional
    private void save(LibraryEvent libraryEvent) {
        if (!bookRepository.existsById(libraryEvent.getBook().getBookId())) {
            libraryEvent.getBook().setLibraryEvent(libraryEvent);
            libraryEventsRepository.save(libraryEvent);
            log.info("Successfully Persisted the library Event {} ", libraryEvent);
        }
    }
}

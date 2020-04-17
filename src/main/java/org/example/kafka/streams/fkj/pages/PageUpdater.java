package org.example.kafka.streams.fkj.pages;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@Slf4j
@Service
class PageUpdater implements Runnable {

    @Autowired
    private KafkaProducer<Integer, Page> producer;

    private Random r = new Random();

    @Autowired
    private NewTopic pagesTopic;

    @Scheduled(fixedDelay=1000)
    public void run() {
        log.info("Updating a page");
        Page pageToUpdate = Page.builder().id(r.nextInt(5)).title(UUID.randomUUID().toString()).build();
        producer.send(new ProducerRecord<>(pagesTopic.name(), pageToUpdate.getId(), pageToUpdate));
    }
}

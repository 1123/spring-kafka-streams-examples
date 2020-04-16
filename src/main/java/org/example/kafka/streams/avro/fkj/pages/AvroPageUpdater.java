package org.example.kafka.streams.avro.fkj.pages;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.kafka.streams.avro.fkj.AvroPageviewStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@Slf4j
@Service
public class AvroPageUpdater implements Runnable {

    @Autowired
    private KafkaProducer<Integer, Page> producer;

    @Autowired
    private NewTopic pagesTopic;

    private Random r = new Random();

    @Scheduled(initialDelay = 1000, fixedDelay=1000)
    public void run() {
        log.info("Updating a page");
        Page pageToUpdate = new Page();
        pageToUpdate.setId(r.nextInt(5));
        pageToUpdate.setTitle(UUID.randomUUID().toString());
        producer.send(new ProducerRecord<>(pagesTopic.name(), pageToUpdate.getId(), pageToUpdate));
    }
}

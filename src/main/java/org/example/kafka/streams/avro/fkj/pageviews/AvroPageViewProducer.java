package org.example.kafka.streams.avro.fkj.pageviews;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.kafka.streams.avro.fkj.AvroPageviewStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Random;

@Slf4j
@Service
public class AvroPageViewProducer implements Runnable {

    @Autowired
    private KafkaProducer<Integer, PageView> producer;

    @Autowired
    private NewTopic pageViewsTopic;

    private Random r = new Random();

    @Scheduled(initialDelay = 1000, fixedDelay=100)
    public void run() {
        log.info("Sending message");
        PageView toSend = new PageView();
        toSend.setId(r.nextInt(5));
        toSend.setTime(new Date().toString());
        producer.send(new ProducerRecord<>(pageViewsTopic.name(), 1, toSend));
    }
}

package org.example.kafka.streams.avro.fkj.pageviews;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.kafka.streams.avro.fkj.PageviewStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Random;

@Slf4j
@Service
class PageViewProducer implements Runnable {

    @Autowired
    private KafkaProducer<Integer, PageView> producer;

    private Random r = new Random();

    @Scheduled(fixedDelay=100)
    public void run() {
        log.info("Sending message");
        PageView toSend = new PageView();
        toSend.setId(r.nextInt(5));
        toSend.setTime(new Date().toString());
        producer.send(new ProducerRecord<>(PageviewStream.PAGEVIEW_TOPIC, 1, toSend));
    }
}

package org.example.kafka.streams.fkj.pageviews;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.example.kafka.streams.fkj.PageviewStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;

@Slf4j
@Service
class PageViewProducer implements Runnable {

    @Autowired
    private KafkaProducer<Integer, PageView> producer;

    @Autowired
    private NewTopic pageViewsTopic;

    private Random r = new Random();

    @Scheduled(fixedDelay=100)
    public void run() {
        log.info("Sending message");
        PageView toSend = PageView.builder().pageId(r.nextInt(5)).time("1.Aug").build();
        producer.send(new ProducerRecord<>(pageViewsTopic.name(), 1, toSend));
    }
}

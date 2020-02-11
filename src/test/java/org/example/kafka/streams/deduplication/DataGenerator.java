package org.example.kafka.streams.deduplication;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
@Slf4j
public class DataGenerator implements CommandLineRunner {
    private static final String firstId = UUID.randomUUID().toString();
    private static final String secondId = UUID.randomUUID().toString();
    private static final String thirdId = UUID.randomUUID().toString();

    private final static List<String> INPUT_VALUES = Arrays.asList(firstId, secondId, firstId, firstId, secondId, thirdId,
            thirdId, firstId, secondId);

    private Properties producerProperties() {
        Properties producerProperties = new Properties();
        producerProperties.put("key.serializer", StringSerializer.class);
        producerProperties.put("value.serializer", StringSerializer.class);
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return producerProperties;
    }

    @Override
    public void run(String... args) {
        log.info("Setting up input data.");
        List<ProducerRecord<String, String>> records = INPUT_VALUES.stream().map(v -> new ProducerRecord<String, String>("inputTopic", null, v)).collect(Collectors.toList());
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties());
        records.forEach(kafkaProducer::send);
    }
}

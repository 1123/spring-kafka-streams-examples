package org.example.kafka.streams.avro.fkj;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.example.kafka.streams.avro.fkj.pages.Page;
import org.example.kafka.streams.avro.fkj.pageviews.PageView;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Properties;

@EnableScheduling
@ComponentScan
class TestConfig {

    public static String APPLICATION_ID = "foreign-key-join-integration-test";
    public static String BOOTSTRAP_SERVER = "localhost:9092";

    @Bean
    private AdminClient kafkaAdminClient() {
        Properties clientProperties = new Properties();
        clientProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return KafkaAdminClient.create(clientProperties);
    }

    @Bean
    private KafkaProducer<Integer, PageView> pageviewProducerConfig() {
        return new KafkaProducer<>(producerConfig());
    }

    @Bean
    private KafkaProducer<Integer, Page> pageProducer() {
        return new KafkaProducer<>(producerConfig());
    }

    private Properties producerConfig() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfig.put("schema.registry.url", "http://localhost:8081");
        return producerConfig;
    }

    @Bean
    private Properties streamsConfiguration() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);
        streamsConfiguration.put("schema.registry.url", "http://localhost:8081");
        return streamsConfiguration;
    }

}

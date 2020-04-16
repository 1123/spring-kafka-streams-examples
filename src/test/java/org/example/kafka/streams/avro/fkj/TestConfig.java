package org.example.kafka.streams.avro.fkj;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@EnableScheduling
@ComponentScan
@EnableKafkaStreams
class TestConfig {

    private final String uuid = UUID.randomUUID().toString();

    private final static String APPLICATION_ID = "spring-avro-ks-app";

    public static String BOOTSTRAP_SERVER = "localhost:9092";

    @Bean public NewTopic pageViewsByPageTopic() {
        return TopicBuilder.name("pvbp-" + uuid).build();
    }

    @Bean public NewTopic pageViewsGroupedByKeyTopic() {
        return TopicBuilder.name("pvgpk-" + uuid).build();
    }

    @Bean
    public NewTopic pageViewsRekeyedByIdTopic() {
        return TopicBuilder.name("pvrbi-" + uuid).build();
    }

    @Bean
    public NewTopic pageViewsTopic() {
        return TopicBuilder
                .name("pageviews-" + uuid)
                .partitions(3)
                .build();
    }

    @Bean
    public NewTopic pagesTopic() {
        return TopicBuilder
                .name("pages-" + uuid)
                .partitions(3)
                .build();
    }

    @Bean
    public NewTopic enrichedPageViewsTopic() {
        return TopicBuilder.name("epv-" + uuid).build();
    }

    @Bean
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig() {
        Map<String, Object> streamsConfig = new HashMap<>();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp");
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);
        streamsConfig.put("schema.registry.url", "http://localhost:8081");
        streamsConfig.put(
                StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        streamsConfig.put(
                StreamsConfig.MAIN_CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
        return new KafkaStreamsConfiguration(streamsConfig);
    }

    @Bean
    private AdminClient kafkaAdminClient() {
        Properties clientProperties = new Properties();
        clientProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
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
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfig.put("schema.registry.url", "http://localhost:8081");
        producerConfig.put(
                ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        return producerConfig;
    }

}

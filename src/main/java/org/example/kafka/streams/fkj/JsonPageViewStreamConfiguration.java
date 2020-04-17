package org.example.kafka.streams.fkj;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.example.kafka.streams.fkj.pages.Page;
import org.example.kafka.streams.fkj.pages.PageSerializer;
import org.example.kafka.streams.fkj.pageviews.PageView;
import org.example.kafka.streams.fkj.pageviews.PageViewSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Configuration
@EnableKafkaStreams
@EnableScheduling
public class JsonPageViewStreamConfiguration {

    private final static String APPLICATION_ID = "spring-json-ks-app";

    private static String BOOTSTRAP_SERVERS = "localhost:9092";

    private final String uuid = UUID.randomUUID().toString();

    @Bean
    public KafkaStreamsConfiguration defaultKafkaStreamsConfig() {
        Map<String, Object> streamsConfiguration = new HashMap<>();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp");
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 2 * 1000);
        streamsConfiguration.put(
                StreamsConfig.PRODUCER_PREFIX + ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        streamsConfiguration.put(
                StreamsConfig.MAIN_CONSUMER_PREFIX + ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringConsumerInterceptor");
        return new KafkaStreamsConfiguration(streamsConfiguration);
    }

    @Bean(name="pageviewKafkaProducer")
    public KafkaProducer<Integer, PageView> pageviewProducerConfig() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PageViewSerializer.class);
        producerConfig.put(
                ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        return new KafkaProducer<>(producerConfig);
    }

    @Bean(name="pageKafkaProducer")
    public KafkaProducer<Integer, Page> pageKafkaProducer() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PageSerializer.class);
        return new KafkaProducer<>(producerConfig);
    }

    @Bean
    public AdminClient kafkaAdminClient() {
        Properties clientProperties = new Properties();
        clientProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        return KafkaAdminClient.create(clientProperties);
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
    public NewTopic pageViewsByPageTopic() {
        return TopicBuilder.name("pvbp-" + uuid).build();
    }

}

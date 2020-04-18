package org.example.kafka.streams.json.fkj;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.example.kafka.streams.json.fkj.pages.Page;
import org.example.kafka.streams.json.fkj.pages.PageSerializer;
import org.example.kafka.streams.json.fkj.pageviews.PageView;
import org.example.kafka.streams.json.fkj.pageviews.PageViewSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.Map;
import java.util.UUID;

@Configuration
@EnableKafkaStreams
@EnableScheduling
public class JsonEnrichmentConfig {

    private final String uuid = UUID.randomUUID().toString();

    @Bean
    public ProducerFactory<Integer, Page> pageProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        return new DefaultKafkaProducerFactory<>(props, IntegerSerializer::new, PageSerializer::new);
    }

    @Bean
    public KafkaTemplate<Integer, Page> pageKafkaTemplate(ProducerFactory<Integer, Page> pageProducerFactory) {
        return new KafkaTemplate<>(pageProducerFactory);
    }

    @Bean
    public ProducerFactory<Integer, PageView> pageViewProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> props = kafkaProperties.buildProducerProperties();
        return new DefaultKafkaProducerFactory<>(props, IntegerSerializer::new, PageViewSerializer::new);
    }

    @Bean
    public KafkaTemplate<Integer, PageView> pageViewKafkaTemplate(ProducerFactory<Integer, PageView> pageProducerFactory) {
        return new KafkaTemplate<>(pageProducerFactory);
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

    @Bean
    public NewTopic pageViewsTopic() {
        return TopicBuilder
                .name("pageViews-" + uuid)
                .partitions(3)
                .build();
    }
}

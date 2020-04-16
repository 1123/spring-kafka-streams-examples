package org.example.kafka.streams.avro.fkj;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.kafka.streams.avro.fkj.pages.Page;
import org.example.kafka.streams.avro.fkj.pageviews.PageView;
import org.example.kafka.streams.fkj.enrichedpageviews.EnrichedPageView;
import org.example.kafka.streams.fkj.enrichedpageviews.EnrichedPageViewSerde;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Configuration
@Slf4j
public class AvroPageviewStream {

    @Autowired
    private NewTopic pageViewsTopic;

    @Autowired
    private NewTopic pageViewsRekeyedByIdTopic;

    @Autowired
    private NewTopic pageViewsGroupedByKeyTopic;

    @Autowired
    private NewTopic pageViewsByPageTopic;

    @Autowired
    private NewTopic pagesTopic;

    @Autowired
    private NewTopic enrichedPageViewsTopic;

    @Autowired
    private AdminClient adminClient;

    private final Map<String, String> serdeConfig =
            Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                    "http://localhost:8081");

    private SpecificAvroSerde<PageView> pageViewSpecificAvroSerde() {
        final SpecificAvroSerde<PageView> pageViewSpecificAvroSerde = new SpecificAvroSerde<>();
        pageViewSpecificAvroSerde.configure(serdeConfig, false);
        return pageViewSpecificAvroSerde;
    }

    private SpecificAvroSerde<Page> pageSpecificAvroSerde() {
        final SpecificAvroSerde<Page> pageSpecificAvroSerde = new SpecificAvroSerde<>();
        pageSpecificAvroSerde.configure(serdeConfig, false);
        return pageSpecificAvroSerde;
    }

    private KStream<Integer,PageView> readInputAndRekey(StreamsBuilder builder) {
        // 1. read input stream and deserialize
        // 2. select a new key for the stream
        return builder.stream(pageViewsTopic.name(),
                        Consumed.with(Serdes.Integer(), pageViewSpecificAvroSerde())
                ).peek((k,v) -> log.info("page view in stream"))
                .selectKey((k,v) -> v.getId(), Named.as(pageViewsRekeyedByIdTopic.name()));
    }

    private void groupByPageCountAndWrite(KStream<Integer,PageView> pageViewsKStream) {
        // 3. group by the new key
        // 4. count the records by key.
        KTable<Integer, Long> pageViewsByPageCount = pageViewsKStream
                .groupByKey(
                        Grouped.with(
                                pageViewsGroupedByKeyTopic.name(),
                                Serdes.Integer(),
                                pageViewSpecificAvroSerde()
                        )
                ).count();
        pageViewsByPageCount.toStream().peek((k,v) -> System.err.println(k + ": " + v))
                .to(pageViewsByPageTopic.name(), Produced.with(Serdes.Integer(), Serdes.Long()));

    }

    private KTable<Integer, Page> createPagesKTable(StreamsBuilder builder) {
        // 5. Create a KTable for the pages
        KTable<Integer, Page> pageKTable = builder.table(pagesTopic.name(), Consumed.with(Serdes.Integer(), pageSpecificAvroSerde()));
        // 6. log the update to the page table
        pageKTable.toStream().peek(
                (k,v) -> System.err.println(
                        String.format("Page %d updated: %s", v.getId(), v.getTitle())
                )
        );
        return pageKTable;
    }

    private KStream<Integer, EnrichedPageView> joinPagesAndPageViews(
            KStream<Integer,PageView> pageViewsKStream,
            KTable<Integer, Page> pageKTable) {
        // 7. Join the pages and page views.
        KStream<Integer, EnrichedPageView> enrichedPageViewKStream =
                pageViewsKStream.join(
                        pageKTable,
                        (pageView, page) ->
                                EnrichedPageView.builder()
                                        .id(page.getId())
                                        .time(pageView.getTime().toString())
                                        .title(page.getTitle().toString())
                                        .build()
        );

        return enrichedPageViewKStream.peek((k,v) -> System.err.println(
                String.format("Enriched page view %d: %s", v.getId(), v.toString())
        ));

    }

    @Bean
    public KStream<?, ?> pageViewStream(StreamsBuilder kStreamBuilder) throws InterruptedException, ExecutionException {
        adminClient.createTopics(
                Arrays.asList(pageViewsTopic, pagesTopic)
        ).all().get();
        KStream<Integer, PageView> pageViewsKStream = readInputAndRekey(kStreamBuilder);
        groupByPageCountAndWrite(pageViewsKStream);

        KTable<Integer, Page> pageKTable = createPagesKTable(kStreamBuilder);
        KStream<Integer, EnrichedPageView> enrichedPageViewKStream = joinPagesAndPageViews(pageViewsKStream, pageKTable);
        // 8. sink the enriched stream to a new topic
        enrichedPageViewKStream.to(enrichedPageViewsTopic.name(), Produced.with(Serdes.Integer(), new EnrichedPageViewSerde()));
        System.err.println(kStreamBuilder.build().describe());
        return pageViewsKStream;
    }

}



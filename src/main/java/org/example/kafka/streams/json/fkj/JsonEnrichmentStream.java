package org.example.kafka.streams.json.fkj;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.kafka.streams.json.fkj.enrichedpageviews.EnrichedPageView;
import org.example.kafka.streams.json.fkj.enrichedpageviews.EnrichedPageViewSerde;
import org.example.kafka.streams.json.fkj.pages.Page;
import org.example.kafka.streams.json.fkj.pages.PageSerde;
import org.example.kafka.streams.json.fkj.pageviews.PageView;
import org.example.kafka.streams.json.fkj.pageviews.PageViewSerde;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class JsonEnrichmentStream {

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private NewTopic pageViewsTopic;

    @Autowired
    private NewTopic pageViewsByPageTopic;

    @Autowired
    private NewTopic pagesTopic;

    @Autowired
    private NewTopic enrichedPageViewsTopic;

    @Bean
    public KStream<?, ?> pageViewStream(StreamsBuilder builder)
            throws ExecutionException, InterruptedException {
        adminClient.createTopics(
                Arrays.asList(pageViewsTopic, pagesTopic)
        ).all().get();
        // 1. read input stream and deserialize
        // 2. select a new key for the stream
        KStream<Integer, PageView> pageViewsKStream =
                builder.stream(pageViewsTopic.name(), Consumed.with(Serdes.Integer(), new PageViewSerde()));

        KStream<Integer, PageView> pageViewsRekeyed = pageViewsKStream.selectKey((k, v) -> v.getPageId());
        // 3. group by the new key
        // 4. count the records by key.
        KTable<Integer, Long> pageViewsByPageCount = pageViewsRekeyed
                .groupByKey(Grouped.with(Serdes.Integer(), new PageViewSerde())).count();

        pageViewsByPageCount.toStream().peek((k,v) -> log.info("Page with id {} has been viewed {} times", k, v))
                .to(pageViewsByPageTopic.name(), Produced.with(Serdes.Integer(), Serdes.Long()));

        // 5. Create a KTable for the pages
        KTable<Integer, Page> pageKTable = builder.table(pagesTopic.name(), Consumed.with(Serdes.Integer(), new PageSerde()));

        // 7. Join the pages and page views.
        KStream<Integer, EnrichedPageView> enrichedPageViewKStream = pageViewsRekeyed.join(pageKTable,
                (pageView, page) -> EnrichedPageView.builder().id(page.getId()).time(pageView.getTime()).title(page.getTitle()).build()
        );

        enrichedPageViewKStream.peek((k,v) -> log.info("Enriched page view {}: {}", v.getId(), v.toString()));
        // 8. sink the enriched stream to a new topic
        enrichedPageViewKStream.to(enrichedPageViewsTopic.name(), Produced.with(Serdes.Integer(), new EnrichedPageViewSerde()));
        System.err.println(builder.build().describe());
        return enrichedPageViewKStream;
    }


}


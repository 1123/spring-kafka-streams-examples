package org.example.kafka.streams.fkj;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.example.kafka.streams.fkj.enrichedpageviews.EnrichedPageView;
import org.example.kafka.streams.fkj.enrichedpageviews.EnrichedPageViewSerde;
import org.example.kafka.streams.fkj.pages.Page;
import org.example.kafka.streams.fkj.pages.PageSerde;
import org.example.kafka.streams.fkj.pageviews.PageView;
import org.example.kafka.streams.fkj.pageviews.PageViewSerde;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;

@Component
public class PageviewStream {

    // UUIDs are used as topic names to avoid conflicting data in subsequent test runs.
    public static String PAGEVIEW_TOPIC = UUID.randomUUID().toString();
    public static String PAGE_TOPIC = UUID.randomUUID().toString();
    public static String PAGE_VIEWS_BY_PAGE = "pageviews_by_page";
    public static String ENRICHED_PAGEVIEW_TOPIC = UUID.randomUUID().toString();

    @Autowired
    @Qualifier("streamsConfig")
    private Properties streamsConfiguration;

    KafkaStreams getStream() {
        StreamsBuilder builder = new StreamsBuilder();
        // 1. read input stream and deserialize
        // 2. select a new key for the stream
        KStream<Integer, PageView> pageViewsKStream =
                builder.stream(PAGEVIEW_TOPIC, Consumed.with(Serdes.Integer(), new PageViewSerde()))
                        .peek((k,v) -> System.err.println("page view in stream"))
                        .selectKey(
                            (k,v) -> v.getPageId()
        );
        // 3. group by the new key
        // 4. count the records by key.
        KTable<Integer, Long> pageViewsByPageCount = pageViewsKStream
                .groupByKey(Grouped.with(Serdes.Integer(), new PageViewSerde())).count();

        pageViewsByPageCount.toStream().peek((k,v) -> System.err.println(k + ": " + v))
                .to(PAGE_VIEWS_BY_PAGE, Produced.with(Serdes.Integer(), Serdes.Long()));

        // 5. Create a KTable for the pages
        KTable<Integer, Page> pageKTable = builder.table(PAGE_TOPIC, Consumed.with(Serdes.Integer(), new PageSerde()));
        // 6. log the update to the page table
        pageKTable.toStream().peek(
                (k,v) -> System.err.println(
                        String.format("Page %d updated: %s", v.getId(), v.getTitle())
                )
        );

        // 7. Join the pages and page views.
        KStream<Integer, EnrichedPageView> enrichedPageViewKStream = pageViewsKStream.join(pageKTable,
                (pageView, page) -> EnrichedPageView.builder().id(page.getId()).time(pageView.getTime()).title(page.getTitle()).build()
        );

        enrichedPageViewKStream.peek((k,v) -> System.err.println(
                String.format("Enriched page view %d: %s", v.getId(), v.toString())
        ));
        // 8. sink the enriched stream to a new topic
        enrichedPageViewKStream.to(ENRICHED_PAGEVIEW_TOPIC, Produced.with(Serdes.Integer(), new EnrichedPageViewSerde()));
        System.err.println(builder.build().describe());
        return new KafkaStreams(builder.build(), streamsConfiguration);
    }


}


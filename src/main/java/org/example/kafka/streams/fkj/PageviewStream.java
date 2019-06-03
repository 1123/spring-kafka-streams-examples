package org.example.kafka.streams.fkj;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.UUID;

@Component
class PageviewStream {

    static String PAGEVIEW_TOPIC = UUID.randomUUID().toString();

    @Autowired
    @Qualifier("streamsConfig")
    private Properties streamsConfiguration;

    KafkaStreams getStream() {
        StreamsBuilder builder = new StreamsBuilder();
        // 1. read input stream and deserialize
        KStream<Integer, PageView> pageViewsKStream =
                builder.stream(PAGEVIEW_TOPIC, Consumed.with(Serdes.Integer(), new PageviewSerde()));
        // 2. select a new key for the stream
        // 3. group by that new key
        // 4. count the records by key.
        KTable<Integer, Long> pageViewsByPageCount = pageViewsKStream.selectKey(
                (k,v) -> v.pageId
        ).groupByKey(Grouped.with(Serdes.Integer(), new PageviewSerde())).count();

        pageViewsByPageCount.toStream().peek((k,v) -> System.err.println(k + ": " + v))
                .to("pageviewsByPageCount", Produced.with(Serdes.Integer(), Serdes.Long()));

        return new KafkaStreams(builder.build(), streamsConfiguration);
    }

}

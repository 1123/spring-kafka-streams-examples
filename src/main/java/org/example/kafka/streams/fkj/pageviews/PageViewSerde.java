package org.example.kafka.streams.fkj.pageviews;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

public class PageViewSerde implements Serde<PageView> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<PageView> serializer() {
        return new PageViewSerializer();
    }

    @Override
    public Deserializer<PageView> deserializer() {
        return new PageViewDeserializer();
    }
}


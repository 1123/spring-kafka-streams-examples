package org.example.kafka.streams.json.fkj.enrichedpageviews;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class EnrichedPageViewSerde implements Serde<EnrichedPageView> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<EnrichedPageView> serializer() {
        return new EnrichedPageViewSerializer();
    }

    @Override
    public Deserializer<EnrichedPageView> deserializer() {
        return new EnrichedPageViewDeserializer();
    }
}

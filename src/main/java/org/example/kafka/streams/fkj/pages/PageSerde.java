package org.example.kafka.streams.fkj.pages;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class PageSerde implements Serde<Page> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Page> serializer() {
        return new PageSerializer();
    }

    @Override
    public Deserializer<Page> deserializer() {
        return new PageDeserializer();
    }
}


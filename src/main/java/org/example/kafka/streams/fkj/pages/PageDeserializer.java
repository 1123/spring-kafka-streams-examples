package org.example.kafka.streams.fkj.pages;

import org.apache.kafka.common.serialization.Deserializer;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.Map;

class PageDeserializer implements Deserializer<Page> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Page deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Page.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}

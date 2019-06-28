package org.example.kafka.streams.fkj.pageviews;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

class PageViewDeserializer implements Deserializer<PageView> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public PageView deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, PageView.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}

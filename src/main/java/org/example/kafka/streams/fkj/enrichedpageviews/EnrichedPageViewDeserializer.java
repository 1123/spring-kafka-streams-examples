package org.example.kafka.streams.fkj.enrichedpageviews;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

class EnrichedPageViewDeserializer implements Deserializer<EnrichedPageView> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public EnrichedPageView deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, EnrichedPageView.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {

    }
}

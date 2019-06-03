package org.example.kafka.streams.fkj.enrichedpageviews;

import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Map;

@Service
public class EnrichedPageViewSerializer implements Serializer<EnrichedPageView> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, EnrichedPageView data) {
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {

    }
}

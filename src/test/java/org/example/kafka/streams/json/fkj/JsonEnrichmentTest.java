package org.example.kafka.streams.json.fkj;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(JsonEnrichmentConfig.class)
class JsonEnrichmentTest {

    @Test
    void test() throws InterruptedException {
        Thread.sleep(10000);
    }

}


package org.example.kafka.streams.avro.fkj;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(AvroPageViewStreamConfiguration.class)
class AvroForeignKeyJoinTest {

    @Test
    void test() throws InterruptedException {
        Thread.sleep(10000);
    }

}


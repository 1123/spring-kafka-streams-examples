package org.example.kafka.streams.avro.fkj;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Same as ForeignKeyJoinTest, but with Avro.
 */

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { org.example.kafka.streams.avro.fkj.TestConfig.class })
@Slf4j
class AvroForeignKeyJoinTest {

    @Test
    void test() throws InterruptedException {
        Thread.sleep(300000);
    }

}


package org.example.kafka.streams.avro.fkj;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * Same as ForeignKeyJoinTest, but with Avro.
 */

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { org.example.kafka.streams.avro.fkj.TestConfig.class })
@Slf4j
class AvroForeignKeyJoinTest {

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private PageviewStream pageviewStream;

    @BeforeEach
    void cleanup() throws ExecutionException, InterruptedException {
        log.info("Creating all relevant topics");
        CreateTopicsResult result = adminClient.createTopics(
                Arrays.asList(
                        new NewTopic(PageviewStream.PAGEVIEW_TOPIC, 1, (short) 1),
                        new NewTopic(PageviewStream.PAGE_TOPIC, 1, (short) 1),
                        new NewTopic(PageviewStream.ENRICHED_PAGEVIEW_TOPIC, 1, (short) 1)
                )
        );
        result.all().get();
        // TODO: there are more topics that need to be cleaned up.
        // Ideally this should also run after the test.
        // There is a class that possibly could do this for us: See the StreamsResetter class.
        adminClient.deleteTopics(Arrays.asList(
                "foreign-key-join-integration-test-KSTREAM-AGGREGATE-STATE-STORE-0000000004-changelog",
                "foreign-key-join-integration-test-KSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition"
        ));
    }

    @Test
    void test() throws InterruptedException {
        KafkaStreams stream = pageviewStream.getStream();
        stream.start();
        while (! stream.state().toString().equals("RUNNING")) {
            log.info("Waiting for stream to start");
            log.info("Stream state: " + stream.state());
            Thread.sleep(100);
        }
        Thread.sleep(5000);
    }

}


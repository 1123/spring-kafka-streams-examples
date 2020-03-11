package org.example.kafka.streams.avro.fkj;

import kafka.tools.StreamsResetter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.jupiter.api.AfterEach;
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
    private AvroPageviewStream pageviewStream;

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException {
        log.info("Creating all relevant topics");
        adminClient.createTopics(
                Arrays.asList(
                        new NewTopic(AvroPageviewStream.PAGEVIEW_TOPIC, 1, (short) 1),
                        new NewTopic(AvroPageviewStream.PAGE_TOPIC, 1, (short) 1),
                        new NewTopic(AvroPageviewStream.ENRICHED_PAGEVIEW_TOPIC, 1, (short) 1)
                )
        ).all().get();
    }

    @AfterEach
    void cleanup() throws ExecutionException, InterruptedException {
        // The StreamsResetter can be used for deleting intermediate topics, but not for input and output topics.
        log.info("Cleaning up");
        adminClient.deleteTopics(Arrays.asList(
                AvroPageviewStream.PAGE_TOPIC,
                AvroPageviewStream.PAGEVIEW_TOPIC,
                AvroPageviewStream.ENRICHED_PAGEVIEW_TOPIC,
                AvroPageviewStream.PAGEVIEWS_BY_PAGE
        )).all().get();
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
        Thread.sleep(2000);
        stream.close();
        Thread.sleep(5000);
        log.info("Stream state: " + stream.state());
        StreamsResetter streamsResetter = new StreamsResetter();
        streamsResetter.run(new String[]{"--bootstrap-servers", TestConfig.BOOTSTRAP_SERVER,
                "--application-id", TestConfig.APPLICATION_ID});
    }

}


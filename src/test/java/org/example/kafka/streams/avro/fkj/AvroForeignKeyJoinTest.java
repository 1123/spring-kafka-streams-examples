package org.example.kafka.streams.avro.fkj;

import kafka.tools.StreamsResetter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ThreadUtils;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.streams.KafkaStreams;
import org.example.kafka.streams.avro.fkj.pageviews.AvroPageViewProducer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

/**
 * Same as ForeignKeyJoinTest, but with Avro.
 */

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { org.example.kafka.streams.avro.fkj.TestConfig.class })
@Slf4j
class AvroForeignKeyJoinTest {

    @Value("kafka.stream.application.id")
    private String applicationId;

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private AvroPageviewStream pageviewStream;

    @Test
    void test() throws InterruptedException {
        Thread.sleep(10000);
    }

    private void waitForStreamToStart(KafkaStreams stream) throws InterruptedException {
        while (! stream.state().toString().equals("RUNNING")) {
            log.info("Waiting for stream to start");
            log.info("Stream state: " + stream.state());
            Thread.sleep(100);
        }
    }

    /*
     * In case there are still active consumers, then the streamresetter application will fail with an exception.
     * Therefore we need to check that all consumergroups have terminated prior to calling the streams resetter.
     */
    private void waitForAllConsumerGroupsToTerminate() throws ExecutionException, InterruptedException {
        while (! validateNoActiveConsumers(applicationId, adminClient)) {
            log.info("Still active consumers");
            Thread.sleep(1000);
        }
    }

    private boolean validateNoActiveConsumers(final String groupId,
                                           final Admin adminClient)
            throws ExecutionException, InterruptedException {
        final DescribeConsumerGroupsResult describeConsumerGroupsResult =
                adminClient.describeConsumerGroups(
                        Collections.singleton(groupId),
                        new DescribeConsumerGroupsOptions().timeoutMs(5000)
                );
        return new ArrayList<>(describeConsumerGroupsResult.describedGroups().get(groupId).get().members()).isEmpty();
    }

}


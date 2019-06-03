package org.example.kafka.streams.fkj;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Arrays;
import java.util.Collections;


@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { TestConfig.class })
class ForeignKeyJoinTest {

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private PageviewStream pageviewStream;

    /**
     * TODO: work in progress
     * This test should produce some pageviews, produce some pages.
     * Then do a one to many join between pageviews and pages.
     */

    @BeforeEach
    void cleanup() {
        adminClient.deleteTopics(Arrays.asList(
                "foreign-key-join-integration-test-KSTREAM-AGGREGATE-STATE-STORE-0000000004-changelog",
                "foreign-key-join-integration-test-KSTREAM-AGGREGATE-STATE-STORE-0000000004-repartition"
        ));
    }

    /*
     * kafka-console-consumer
     *   --bootstrap-server localhost:9092
     *   --topic pageviewsByPageCount
     *   --from-beginning
     *   --property print.key=true
     *   --key-deserializer org.apache.kafka.common.serialization.IntegerDeserializer
     *   --value-deserializer org.apache.kafka.common.serialization.LongDeserializer
     */

    @Test
    void test() throws InterruptedException {
        adminClient.createTopics(Collections.singletonList(new NewTopic(PageviewStream.PAGEVIEW_TOPIC, 1, (short) 1)));
        pageviewStream.getStream().start();
        Thread.sleep(10000);
    }

}


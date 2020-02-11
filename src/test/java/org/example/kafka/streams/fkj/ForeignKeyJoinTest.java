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

/**
 * This test generates some imaginary page views and updates to web pages.
 * Subsequently it enriches the dynamic page views with the static data from the web pages.
 *
 * The relationship between pages and page views is 1:n.
 * Page views must be re keyed prior to being joined -- since the original keys of the page views are not the page ids.
 */

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { TestConfig.class })
class ForeignKeyJoinTest {

    @Autowired
    private AdminClient adminClient;

    @Autowired
    private PageviewStream pageviewStream;

    @BeforeEach
    void cleanup() {
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
        adminClient.createTopics(Collections.singletonList(new NewTopic(PageviewStream.PAGEVIEW_TOPIC, 1, (short) 1)));
        pageviewStream.getStream().start();
        Thread.sleep(20000);
    }

}


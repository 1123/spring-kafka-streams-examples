package org.example.kafka.streams.fkj;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
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

    @Test
    void test() throws InterruptedException {
        Thread.sleep(2000000);
    }

}


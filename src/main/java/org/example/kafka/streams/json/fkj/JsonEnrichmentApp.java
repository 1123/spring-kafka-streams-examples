package org.example.kafka.streams.json.fkj;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * This application generates some imaginary page views and updates to web pages.
 * Subsequently it enriches the dynamic page views with the static data from the web pages.
 *
 * The relationship between pages and page views is 1:n.
 * Page views must be re-keyed prior to being joined -- since the original keys of the page views are not the page ids.
 *
 */

@SpringBootApplication
public class JsonEnrichmentApp {

    public static void main(String[] args) {
        SpringApplication.run(JsonEnrichmentApp.class, args);
    }

}


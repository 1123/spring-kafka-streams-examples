package org.example.kafka.streams.avro.fkj;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Same as JsonPageViewStreamMain, but with Avro.
 */

@SpringBootApplication
public class AvroEnrichmentApp {

    public static void main(String [] args) {
        SpringApplication.run(AvroEnrichmentApp.class);
    }

}

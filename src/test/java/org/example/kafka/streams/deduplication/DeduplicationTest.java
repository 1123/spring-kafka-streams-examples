package org.example.kafka.streams.deduplication;

import io.confluent.common.utils.TestUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.Properties;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { DeduplicationTest.Configuration.class })
public class DeduplicationTest {

    @ComponentScan
    static class Configuration {

        @Bean
        private Properties streamsConfiguration() {
            final Properties streamsConfiguration = new Properties();
            streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "deduplication-lambda-integration-test");
            streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
            streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getAbsolutePath());
            return streamsConfiguration;
        }

    }

    @Autowired
    private DataGenerator dataGenerator;

    @Autowired
    private Properties streamsConfiguration;

    static String STORE_NAME = "deduplication-store";

    @Test
    public void shouldRemoveDuplicatesFromTheInput() throws InterruptedException {
        dataGenerator.run();
        // Step 1: Configure and start the processor topology.
        final StreamsBuilder builder = new StreamsBuilder();

        // How long we "remember" an event.  During this time, any incoming duplicates of the event
        // will be, well, dropped, thereby de-duplicating the input data.
        Duration windowSize = Duration.ofSeconds(1);

        // The actual value depends on your use case.  To reduce memory and disk usage, you could
        // decrease the size to purge old windows more frequently at the cost of potentially missing out
        // on de-duplicating late-arriving records.

        // retention period must be at least window size -- for this use case, we don't need a longer retention period
        // and thus just use the window size as retention time
        Duration retention = Duration.ofSeconds(1);
        final StoreBuilder<WindowStore<String, Long>> dedupStoreBuilder = Stores.windowStoreBuilder(
                Stores.persistentWindowStore(STORE_NAME,
                        retention,
                        windowSize,
                        false
                ),
                Serdes.String(),
                Serdes.Long());

        builder.addStateStore(dedupStoreBuilder);

        final String inputTopic = "inputTopic";
        final String outputTopic = "outputTopic";

        final KStream<byte[], String> input = builder.stream(inputTopic);
        final KStream<byte[], String> deduplicated = input.transform(
                // In this example, we assume that the record value as-is represents a unique event ID by
                // which we can perform de-duplication.  If your records are different, adapt the extractor
                // function as needed.
                () -> new DeduplicationTransformer<>(windowSize.toMillis(), (key, value) -> value),
                STORE_NAME);
        deduplicated.to(outputTopic);
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
        Thread.sleep(10000);
        kafkaStreams.close();
    }
}


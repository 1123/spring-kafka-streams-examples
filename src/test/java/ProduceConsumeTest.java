import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { ProduceConsumeTest.ProduceConsumeTestConfiguration.class })
class ProduceConsumeTest {

    @Autowired
    private Producer<String, String> simpleProducer;

    @Autowired private Consumer<String, String> simpleConsumer;

    @Test
    void testProduce() {
        System.err.println("Test producing to Kafka");
        simpleProducer.send(new ProducerRecord<>("sample-topic", "bar"));
    }

    @Test
    void testConsume() {
        System.err.println("Test consuming from Kafka");
        simpleConsumer.subscribe(Collections.singletonList("sample-topic"));
        ConsumerRecords<String, String> consumerRecords = simpleConsumer.poll(Duration.ofSeconds(1));
        consumerRecords.iterator().forEachRemaining(System.err::println);
    }

    static class ProduceConsumeTestConfiguration {

        @Bean()
        public Producer<String, String> simpleProducer() {
            Properties producerProperties = new Properties();
            producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            return new KafkaProducer<>(producerProperties);
        }

        @Bean()
        public Consumer<String, String> simpleConsumer() {
            Properties consumerProperties = new Properties();
            consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return new KafkaConsumer<>(consumerProperties);
        }

    }

}

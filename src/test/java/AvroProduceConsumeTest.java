import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { AvroProduceConsumeTest.AvroProduceConsumeTestConfiguration.class })
class AvroProduceConsumeTest {

    static class AvroProduceConsumeTestConfiguration {

        @Bean()
        public Producer<Object, Object> avroProducer() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
            props.put("schema.registry.url", "http://localhost:8081");
            return new KafkaProducer<>(props);
        }

        @Bean()
        public Consumer<Object, Object> avroConsumer() {
            Properties props = new Properties();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put("schema.registry.url", "http://localhost:8081");
            return new KafkaConsumer<>(props);
        }

    }

    @Autowired
    @Qualifier("avroProducer")
    private Producer<Object, Object> avroProducer;

    @Autowired
    @Qualifier("avroConsumer")
    private Consumer<Object, Object> avroConsumer;

    @Test
    void testProduceAvro() {
        Schema.Parser parser = new Schema.Parser();
        String USER_SCHEMA_1 = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
        Schema schema = parser.parse(USER_SCHEMA_1);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", "value1");
        ProducerRecord<Object, Object> record = new ProducerRecord<>("topic1", "key1", avroRecord);
        avroProducer.send(record);
    }

    @Test
    void testProduceAvro2() {
        Schema.Parser parser = new Schema.Parser();
        String USER_SCHEMA_2 = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"},{\"name\":\"f2\",\"type\":\"string\", \"default\":\"bar\"}]}";
        Schema schema = parser.parse(USER_SCHEMA_2);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", "value1");
        avroRecord.put("f2", "bar");
        ProducerRecord<Object, Object> record = new ProducerRecord<>("topic1", "key1", avroRecord);
        avroProducer.send(record);
    }

    @Test
    void testProduceAvro3() {
        List<Schema.Field> fields = Arrays.asList(
                new Schema.Field("f1", Schema.create(Schema.Type.STRING), "first field", "foo"),
                new Schema.Field("f2", Schema.create(Schema.Type.STRING), "second field", "bar"),
                new Schema.Field("f3", Schema.create(Schema.Type.INT), "third field", 3)
        );
        Schema schema = Schema.createRecord("myrecord", "no doc", "", false);
        schema.setFields(fields);
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("f1", "value1");
        avroRecord.put("f2", "bar");
        avroRecord.put("f3", 3);
        ProducerRecord<Object, Object> record = new ProducerRecord<>("topic1", "key1", avroRecord);
        avroProducer.send(record);
    }

    /*
     * ksql> print 'float-test-topic';
     * ksql> create stream floats (f1 varchar, f2 DOUBLE) WITH (KAFkA_TOPIC='float-test-topic', VALUE_FORMAT='AVRO');
     * SET 'auto.offset.reset' = 'earliest';
     * select f1, sum(f2) from floats group by f1;
     */
    @Test
    void testSchemaWithFloat() {
        List<Schema.Field> fields = Arrays.asList(
                new Schema.Field("f1", Schema.create(Schema.Type.STRING), "a string field", "foo"),
                new Schema.Field("f2", Schema.create(Schema.Type.FLOAT), "a float field", 3.0123)
        );
        Schema schema = Schema.createRecord("myFloatSchema", "no doc", "", false);
        schema.setFields(fields);
        Random r = new Random();
        for (int i = 0; i < 100; i++) {
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("f1", "value_" + r.nextInt(5));
            avroRecord.put("f2", r.nextFloat());
            ProducerRecord<Object, Object> record =
                    new ProducerRecord<>("float-test-topic", UUID.randomUUID().toString(), avroRecord);
            avroProducer.send(record);
        }
    }

    @Test
    void testConsumeAvro() {
        avroConsumer.subscribe(Collections.singletonList("topic1"));
        ConsumerRecords<Object, Object> consumerRecords = avroConsumer.poll(Duration.ofSeconds(1));
        consumerRecords.iterator().forEachRemaining(System.err::println);
    }

    @Test
    void testGenerateSchema() {
        Schema schema = ReflectData.get().getSchema(MyRecord.class);
        System.err.println(schema.toString());
    }

}

@Data
class MyRecord {

    String f1 = "abc";
    String f2 = "xyz";

}



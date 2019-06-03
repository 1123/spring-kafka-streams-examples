import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Demo for creating topics through admin client.
 */

@SpringBootApplication
public class TopicCreator {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient kafkaAdminClient = KafkaAdminClient.create(properties);
        CreateTopicsResult result = kafkaAdminClient.createTopics(Collections.singletonList(new NewTopic("foo", 3, (short) 1)));
        result.all().get();
    }

}
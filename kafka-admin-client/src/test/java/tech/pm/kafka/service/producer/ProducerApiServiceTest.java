package tech.pm.kafka.service.producer;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tech.pm.kafka.property.ServiceProperty;
import tech.pm.kafka.service.admin.AdminApiService;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Testcontainers
public class ProducerApiServiceTest {

  @Container
  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

  private final Properties consumerProperties = new Properties();
  private final Properties producerProperties = new Properties();
  private final Properties adminProperties = new Properties();
  private final String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
  private final Map<ServiceProperty, String> serviceProperties = new HashMap<>();

  @BeforeEach
  void setUp() {
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "group_test_1");
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    serviceProperties.put(ServiceProperty.TOPIC_NAME, "producer_test_topic");
    serviceProperties.put(ServiceProperty.PARTITIONS, "3");
    serviceProperties.put(ServiceProperty.REPLICATION_FACTOR, "1");
    serviceProperties.put(ServiceProperty.CONFIG_FILE, "src/main/resources/topic_config.properties");
    serviceProperties.put(ServiceProperty.SOURCE_FILE, "src/main/resources/test.txt");
  }

  @Test
  public void writeToTopicFromFile() {

    AdminApiService.createTopic(adminProperties, serviceProperties);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);

    ProducerApiService.writeToTopicFromFile(producerProperties, serviceProperties);

    consumer.subscribe(Collections.singleton(serviceProperties.get(ServiceProperty.TOPIC_NAME)));
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

    for (ConsumerRecord<String, String> record : records) {
      assertThat(record.value()).contains("hello Kafka");
    }
  }
}

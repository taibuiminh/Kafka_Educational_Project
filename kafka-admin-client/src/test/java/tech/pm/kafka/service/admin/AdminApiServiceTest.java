package tech.pm.kafka.service.admin;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tech.pm.kafka.exceptions.topic.InvalidConfigFileException;
import tech.pm.kafka.exceptions.topic.InvalidPartitionsValueException;
import tech.pm.kafka.exceptions.topic.InvalidReplicationFactorValueException;
import tech.pm.kafka.property.ServiceProperty;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class AdminApiServiceTest {

  @Container
  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

  private final Properties adminProperties = new Properties();
  private final Properties producerProperties = new Properties();
  private final String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();

  @BeforeEach
  void setup() {
    adminProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
  }

  @Test
  void createTopic() throws Exception {
    String topic = "admin_test_topic";

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);
    serviceProperties.put(ServiceProperty.PARTITIONS, "6");
    serviceProperties.put(ServiceProperty.REPLICATION_FACTOR, "1");
    serviceProperties.put(ServiceProperty.CONFIG_FILE, "src/main/resources/topic_config.properties");

    AdminApiService.createTopic(adminProperties, serviceProperties);

    String topicCommand = "kafka-topics --bootstrap-server localhost:9092 --list";

    String stdout = KAFKA_CONTAINER.execInContainer("/bin/sh", "-c", topicCommand).getStdout();

    assertThat(stdout).contains(topic);
  }

  @Test
  void setOffsetForConsumerGroup() throws IOException, InterruptedException {
    String topic = "consumer_group_set_offsets_topic";
    String groupId = "consumer_group_test_10";

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);
    serviceProperties.put(ServiceProperty.CONSUMER_GROUP, groupId);
    serviceProperties.put(ServiceProperty.OFFSET, String.valueOf(5L));

    createTopicInContainer(topic);
    sendSomeMessages(topic);
    assertThat(readFromTopic(topic, groupId)).isEqualTo(10);

    AdminApiService.setOffsetForConsumerGroup(adminProperties, serviceProperties);

    assertThat(readFromTopic(topic, groupId)).isEqualTo(5);
  }

  @Test
  void getOffsetForConsumerGroup() throws IOException, InterruptedException {
    String topic = "consumer_group_get_offsets_topic";
    String groupId = "consumer_group_test_11";

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);
    serviceProperties.put(ServiceProperty.CONSUMER_GROUP, groupId);
    serviceProperties.put(ServiceProperty.PARTITION, String.valueOf(0));

    createTopicInContainer(topic);
    sendSomeMessages(topic);
    assertThat(readFromTopic(topic, groupId)).isEqualTo(10);
    long offset = AdminApiService.getOffsetForConsumerGroup(adminProperties, serviceProperties);
    assertEquals(offset, 10);
  }

  @Test
  void deleteTopic() throws Exception {
    String topic = "delete_topic";

    createTopicInContainer(topic);

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);

    AdminApiService.deleteTopic(adminProperties, serviceProperties);

    String topicCommand = "kafka-topics --bootstrap-server=localhost:9092 --list";

    String stdout = KAFKA_CONTAINER.execInContainer("/bin/sh", "-c", topicCommand).getStdout();

    assertFalse(stdout.contains(topic));
  }

  @Test
  void clearTopic() throws Exception {
    String topic = "clear_topic";

    createTopicInContainer(topic);

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);
    serviceProperties.put(ServiceProperty.BOOTSTRAP_SERVER, bootstrapServers);
    serviceProperties.put(ServiceProperty.CONSUMER_GROUP, "test_group");

    sendSomeMessages(topic);

    assertTrue(AdminApiService.clearTopic(adminProperties, serviceProperties));
  }

  @Test
  void getTopicListForConsumerGroup() throws IOException, InterruptedException {
    String topicOne = "topic_one";
    String topicTwo = "topic_two";
    String groupId = "consumer_group_test_11";
    List<String> listOfTopics = List.of(topicTwo, topicOne);

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.CONSUMER_GROUP, groupId);

    createTopicInContainer(topicOne);
    createTopicInContainer(topicTwo);

    sendSomeMessages(topicOne);
    sendSomeMessages(topicTwo);

    assertThat(readFromTopic(topicOne, groupId)).isEqualTo(10);
    assertThat(readFromTopic(topicTwo, groupId)).isEqualTo(10);

    assertThat(AdminApiService.getTopicsByConsumerGroup(adminProperties, serviceProperties)).isEqualTo(listOfTopics);
  }

  @Test
  void throwNumberFormatException() throws IOException, InterruptedException {
    String topic = "topic_error_1";

    createTopicInContainer(topic);

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);
    serviceProperties.put(ServiceProperty.PARTITIONS, "string");

    assertThrows(NumberFormatException.class, () -> AdminApiService.createTopic(adminProperties, serviceProperties));
  }

  @Test
  void throwInvalidPartitionsValueException() throws IOException, InterruptedException {
    String topic = "topic_error_2";

    createTopicInContainer(topic);

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);
    serviceProperties.put(ServiceProperty.PARTITIONS, "-1");
    serviceProperties.put(ServiceProperty.REPLICATION_FACTOR, "3");

    assertThrows(InvalidPartitionsValueException.class, () -> AdminApiService.createTopic(adminProperties, serviceProperties));
  }

  @Test
  void throwInvalidReplicationFactorValueException() throws IOException, InterruptedException {
    String topic = "topic_error_3";

    createTopicInContainer(topic);

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);
    serviceProperties.put(ServiceProperty.PARTITIONS, "3");
    serviceProperties.put(ServiceProperty.REPLICATION_FACTOR, "-1");

    assertThrows(InvalidReplicationFactorValueException.class, () -> AdminApiService.createTopic(adminProperties, serviceProperties));
  }

  @Test
  void throwInvalidConfigFileException() throws IOException, InterruptedException {
    String topic = "topic_error_4";

    createTopicInContainer(topic);

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);
    serviceProperties.put(ServiceProperty.PARTITIONS, "3");
    serviceProperties.put(ServiceProperty.REPLICATION_FACTOR, "1");
    serviceProperties.put(ServiceProperty.CONFIG_FILE, "src/main/resources/unvisible.file");

    assertThrows(InvalidConfigFileException.class, () -> AdminApiService.createTopic(adminProperties, serviceProperties));
  }

  private void createTopicInContainer(String topic) throws IOException, InterruptedException {
    KAFKA_CONTAINER.execInContainer("/bin/sh", "-c",
                                    String.format("kafka-topics --bootstrap-server localhost:9092 --create --topic %s", topic));
  }

  private void sendSomeMessages(String topic) {
    Producer<String, String> producer = new KafkaProducer<>(producerProperties);

    for (int i = 0; i < 10; i++) {
      Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, Integer.toString(i), "Message " + i));

      try {
        future.get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }

    producer.flush();
    producer.close();
  }

  private int readFromTopic(String topic, String groupId) {
    Properties consumerGroupProperties = new Properties();
    consumerGroupProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerGroupProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerGroupProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerGroupProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerGroupProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    Consumer<String, String> consumer = new KafkaConsumer<>(consumerGroupProperties);

    try (consumer) {
      consumer.subscribe(Collections.singleton(topic));
      consumer.commitSync();

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
          if (record.offset() == 9) {
            return records.count();
          }
        }
      }
    }
  }
}
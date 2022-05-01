package tech.pm.kafka.service.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tech.pm.kafka.property.ServiceProperty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
class ConsumerApiServiceTest {

  @Container
  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

  private final Properties consumerProperties = new Properties();
  private final Properties producerProperties = new Properties();
  private final String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();

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
  }

  @Test
  void readMessageInTopicByTimestamp() throws IOException, InterruptedException {
    String topic = "timestamp_test";

    createTopicInContainer(topic);

    List<RecordMetadata> recordsMetadata = sendSomeMessagesAndGetRecordsMetadata(topic);

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);
    serviceProperties.put(ServiceProperty.TIMESTAMP, String.valueOf(recordsMetadata.get(7).timestamp()));

    assertThat(ConsumerApiService.readMessageInTopicByTimestamp(consumerProperties, serviceProperties)).contains(String.valueOf(recordsMetadata.get(7).topic())).contains(String.valueOf(recordsMetadata.get(7).partition())).contains(String.valueOf(recordsMetadata.get(7).offset())).contains(String.valueOf(recordsMetadata.get(7).timestamp()));
  }

  @Test
  void throwKafkaExceptionByTimestamp() throws IOException, InterruptedException {
    String topic = "exeption_timestamp";

    createTopicInContainer(topic);

    List<RecordMetadata> recordsMetadata = sendSomeMessagesAndGetRecordsMetadata(topic);

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);
    serviceProperties.put(ServiceProperty.TIMESTAMP, String.valueOf(recordsMetadata.get(7).timestamp()) + 1000L);

    assertThrows(KafkaException.class, () -> ConsumerApiService.readMessageInTopicByTimestamp(consumerProperties, serviceProperties));
  }

  @Test
  void readMessageInTopicByOffset() throws IOException, InterruptedException {
    String topic = "offset_topic";

    createTopicInContainer(topic);

    List<RecordMetadata> recordsMetadata = sendSomeMessagesAndGetRecordsMetadata(topic);

    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, topic);
    serviceProperties.put(ServiceProperty.PARTITION, String.valueOf(recordsMetadata.get(7).partition()));
    serviceProperties.put(ServiceProperty.OFFSET, String.valueOf(recordsMetadata.get(7).offset()));

    String record = ConsumerApiService.readMessageInTopicPartitionByOffset(consumerProperties, serviceProperties);

    assertThat(record).contains(String.valueOf(recordsMetadata.get(7).topic())).contains(String.valueOf(recordsMetadata.get(7).partition())).contains(String.valueOf(recordsMetadata.get(7).offset())).contains(String.valueOf(recordsMetadata.get(7).timestamp()));
  }

  private void createTopicInContainer(String topic) throws IOException, InterruptedException {
    KAFKA_CONTAINER.execInContainer("/bin/sh", "-c", String.format("kafka-topics --bootstrap-server localhost:9092 --create --topic %s", topic));
  }

  private List<RecordMetadata> sendSomeMessagesAndGetRecordsMetadata(String topic) {
    Producer<String, String> producer = new KafkaProducer<>(producerProperties);

    List<RecordMetadata> recordsMetadata = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, Integer.toString(i), "Message " + i));

      try {
        RecordMetadata recordMetadata = future.get();

        recordsMetadata.add(recordMetadata);
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }

    producer.flush();
    producer.close();

    return recordsMetadata;
  }
}
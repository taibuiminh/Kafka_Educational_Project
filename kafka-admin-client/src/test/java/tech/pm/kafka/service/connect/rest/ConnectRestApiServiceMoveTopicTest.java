package tech.pm.kafka.service.connect.rest;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tech.pm.kafka.exceptions.connect.ConnectorNotFoundException;
import tech.pm.kafka.exceptions.connect.UnexpectedResponseCodeException;
import tech.pm.kafka.property.ServiceProperty;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class ConnectRestApiServiceMoveTopicTest {

  private static final Logger log = LoggerFactory.getLogger(ConnectRestApiServiceMoveTopicTest.class);

  @Container
  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

  @Container
  private static final DockerComposeContainer<?> CONTAINER = new DockerComposeContainer<>(
    new File("src/main/resources/test-move-topic-compose.yml"))
    .withExposedService("zookeeper", 2181)
    .withExposedService("broker", 29092)
    .withExposedService("schema-registry", 8081)
    .withExposedService("connect", 8083);

  private final Properties consumerApiPropertiesForNewConsumerGroup = new Properties();
  private final Properties consumerApiPropertiesForOldConsumerGroup = new Properties();
  private final Properties producerApiProperties = new Properties();
  private final Properties adminApiProperties = new Properties();

  private final Map<ServiceProperty, String> serviceProperties = new HashMap<>();
  private final Map<ServiceProperty, String> servicePropertiesForNewConsumerGroup = new HashMap<>();
  private final Map<ServiceProperty, String> servicePropertiesForOldConsumerGroup = new HashMap<>();

  private final String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
  private final String connectorsUrl = String.format("http://%s:%s",
                                                     CONTAINER.getServiceHost("connect", 8083),
                                                     CONTAINER.getServicePort("connect", 8083));
  private final String newConnector = "new-connector";
  private final String oldConnector = "old-connector";

  @BeforeEach
  void setUp() {
    String groupIdNew = "connect-dwh-new-connector";
    String groupIdOld = "connect-dwh-old-connector";

    consumerApiPropertiesForNewConsumerGroup.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerApiPropertiesForNewConsumerGroup.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdNew);
    consumerApiPropertiesForNewConsumerGroup.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerApiPropertiesForNewConsumerGroup.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerApiPropertiesForNewConsumerGroup.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    consumerApiPropertiesForOldConsumerGroup.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerApiPropertiesForOldConsumerGroup.put(ConsumerConfig.GROUP_ID_CONFIG, groupIdOld);
    consumerApiPropertiesForOldConsumerGroup.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerApiPropertiesForOldConsumerGroup.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerApiPropertiesForOldConsumerGroup.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    adminApiProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    producerApiProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerApiProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerApiProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    serviceProperties.put(ServiceProperty.OLD_CONNECTOR_NAME, oldConnector);
    serviceProperties.put(ServiceProperty.NEW_CONNECTOR_NAME, newConnector);
    serviceProperties.put(ServiceProperty.CONNECTORS_URL, connectorsUrl);

    servicePropertiesForNewConsumerGroup.put(ServiceProperty.CONSUMER_GROUP, groupIdNew);
    servicePropertiesForNewConsumerGroup.put(ServiceProperty.BOOTSTRAP_SERVER, bootstrapServers);

    servicePropertiesForOldConsumerGroup.put(ServiceProperty.CONSUMER_GROUP, groupIdOld);
    servicePropertiesForOldConsumerGroup.put(ServiceProperty.BOOTSTRAP_SERVER, bootstrapServers);

    log.info("Starting connector initialization");

    initializeConnector(newConnector);
    initializeConnector(oldConnector);
  }


  @Test
  void moveKafkaTopicFromConnectorToAnotherConnectorTest() {
    assertTrue(containsConnector(oldConnector));
    assertTrue(containsConnector(newConnector));

    String new1 = "new1";
    String new2 = "new2";

    String old1 = "old1";
    String old2 = "old2";
    String old3 = "old3";
    String old4 = "old4";

    sendSomeMessages(new1);
    readFromTopic(new1, consumerApiPropertiesForNewConsumerGroup);

    sendSomeMessages(new2);
    sendSomeMessages(new2);
    readFromTopic(new2, consumerApiPropertiesForNewConsumerGroup);

    sendSomeMessages(old1);
    readFromTopic(old1, consumerApiPropertiesForOldConsumerGroup);
    sendSomeMessages(old1);

    sendSomeMessages(old2);
    readFromTopic(old2, consumerApiPropertiesForOldConsumerGroup);

    sendSomeMessages(old3);
    readFromTopic(old3, consumerApiPropertiesForOldConsumerGroup);
    sendSomeMessages(old3);

    sendSomeMessages(old4);
    sendSomeMessages(old4);
    sendSomeMessages(old4);
    readFromTopic(old4, consumerApiPropertiesForOldConsumerGroup);
    sendSomeMessages(old4);

    serviceProperties.put(ServiceProperty.TOPIC_NAME, old4);
    assertDoesNotThrow(() -> ConnectRestApiService.moveTopicToAnotherConnector(adminApiProperties,
                                                                               consumerApiPropertiesForOldConsumerGroup,
                                                                               servicePropertiesForOldConsumerGroup,
                                                                               servicePropertiesForNewConsumerGroup,
                                                                               serviceProperties));

    assertTrue(containsConnector(oldConnector));
    assertTrue(containsConnector(newConnector));
  }


  private void sendSomeMessages(String topic) {
    try (Producer<String, String> producer = new KafkaProducer<>(producerApiProperties)) {
      for (int i = 0; i < 100; i++) {
        Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, Integer.toString(i), "Message " + i));
        future.get();
      }
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
  }


  private void readFromTopic(String topic, Properties consumerApiProperties) {
    try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerApiProperties)) {
      consumer.subscribe(Collections.singleton(topic));
      consumer.commitSync();

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, String> record : records) {
          if (record.offset() == 50) {
            return;
          }
        }
      }
    }
  }


  private boolean containsConnector(String connectorName) {
    try {
      HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(connectorsUrl + "/connectors"))
        .GET()
        .build();

      HttpResponse<String> response = HttpClient.newBuilder()
        .build()
        .send(request, HttpResponse.BodyHandlers.ofString());

      int responseCode = response.statusCode();

      if (responseCode != 200) {
        throw new ConnectorNotFoundException(connectorName);
      }
    } catch (IOException | URISyntaxException | InterruptedException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }


  private void initializeConnector(String connectorName) {
    String path = String.format("src/main/resources/%s.json", connectorName);
    try {
      HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(connectorsUrl + "/connectors"))
        .POST(HttpRequest.BodyPublishers.ofFile(Path.of(path)))
        .header("Accept", "application/json")
        .header("Content-type", "application/json")
        .build();

      HttpResponse<String> response = HttpClient.newBuilder()
        .build()
        .send(request, HttpResponse.BodyHandlers.ofString());

      log.info(String.format("Connector %s was initialized. Response: %s", connectorName, response));

      int responseCode = response.statusCode();
      if (responseCode != 201) {
        throw new UnexpectedResponseCodeException(responseCode, connectorName);
      }
    } catch (IOException | URISyntaxException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}
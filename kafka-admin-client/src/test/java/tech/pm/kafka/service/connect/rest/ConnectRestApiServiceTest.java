package tech.pm.kafka.service.connect.rest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
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
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
class ConnectRestApiServiceTest {

  @Container
  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

  @Container
  private static final DockerComposeContainer<?> CONTAINER = new DockerComposeContainer<>(
    new File("src/main/resources/test-compose.yml"))
    .withExposedService("zookeeper", 2181)
    .withExposedService("broker", 29092)
    .withExposedService("schema-registry", 8081)
    .withExposedService("connect", 8083);

  private final Properties consumerApiProperties = new Properties();
  private final Properties producerApiProperties = new Properties();
  private final Properties adminApiProperties = new Properties();

  private final Map<ServiceProperty, String> serviceProperties = new HashMap<>();

  private final String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();

  private final String testConnector = "test-connector";

  private final String connectorsUrl = String.format("http://%s:%s",
                                                     CONTAINER.getServiceHost("connect", 8083),
                                                     CONTAINER.getServicePort("connect", 8083));

  private final String defaultConnector = "datagen-pageviews";

  @BeforeEach
  void setUp() {
    String groupId = "test-group";

    consumerApiProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerApiProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerApiProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerApiProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerApiProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    adminApiProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    producerApiProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerApiProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerApiProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    serviceProperties.put(ServiceProperty.CONSUMER_GROUP, groupId);
    serviceProperties.put(ServiceProperty.NEW_CONSUMER_GROUP, testConnector);
    serviceProperties.put(ServiceProperty.OLD_CONNECTOR_NAME, defaultConnector);
    serviceProperties.put(ServiceProperty.NEW_CONNECTOR_NAME, testConnector);
    serviceProperties.put(ServiceProperty.CONNECTORS_URL, connectorsUrl);

    initializeConnector();
  }


  @AfterEach
  void deleteConnectors() {
    ConnectRestApiService.deleteOldConnector(testConnector);
  }


  @Test
  void renameKafkaConnector() {
    String topic = "test-topic";

    createTopic(topic);

    sendSomeMessages(topic);

    readFromTopic(topic);

    assertDoesNotThrow(() -> ConnectRestApiService.renameKafkaConnector(adminApiProperties,
                                                                        consumerApiProperties,
                                                                        serviceProperties));

    assertTrue(containsConnector(testConnector));
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


  private void readFromTopic(String topic) {
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


  private void createTopic(String topic) {
    try (Admin admin = Admin.create(adminApiProperties)) {

      NewTopic newTopic = new NewTopic(topic, 1, (short) 1);

      CreateTopicsResult result;

      result = admin.createTopics(Collections.singleton(newTopic));

      KafkaFuture<Void> future = result.values().get(topic);

      future.get();

    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
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


  private void initializeConnector() {
    try {
      HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(connectorsUrl + "/connectors"))
        .POST(HttpRequest.BodyPublishers.ofFile(Path.of("src/main/resources/connector_pageviews_cos.json")))
        .header("Accept", "application/json")
        .header("Content-type", "application/json")
        .build();

      HttpResponse<String> response = HttpClient.newBuilder()
        .build()
        .send(request, HttpResponse.BodyHandlers.ofString());

      int responseCode = response.statusCode();
      if (responseCode != 201) {
        throw new UnexpectedResponseCodeException(responseCode, defaultConnector);
      }
    } catch (IOException | URISyntaxException | InterruptedException e) {
      e.printStackTrace();
    }
  }
}
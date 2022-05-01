package tech.pm.kafka.service.connect.rest;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.pm.kafka.exceptions.admin.TopicNotFoundForConsumerGroupException;
import tech.pm.kafka.exceptions.connect.ConnectorCreatingException;
import tech.pm.kafka.exceptions.connect.ConnectorDeletionException;
import tech.pm.kafka.exceptions.connect.ConnectorNotFoundException;
import tech.pm.kafka.exceptions.connect.UnexpectedResponseCodeException;
import tech.pm.kafka.property.ServiceProperty;
import tech.pm.kafka.service.admin.AdminApiService;
import tech.pm.kafka.service.consumer.ConsumerApiService;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConnectRestApiService {

  private static final Logger log = LoggerFactory.getLogger(ConnectRestApiService.class);

  private static String connectorsUrl;

  public static void renameKafkaConnector(Properties adminApiProperties,
                                          Properties consumerApiProperties,
                                          Map<ServiceProperty, String> serviceProperties) {

    String oldConnectorName = serviceProperties.get(ServiceProperty.OLD_CONNECTOR_NAME);
    String newConnectorName = serviceProperties.get(ServiceProperty.NEW_CONNECTOR_NAME);
    String consumerGroup = consumerApiProperties.getProperty(ConsumerConfig.GROUP_ID_CONFIG);
    String newConsumerGroup = serviceProperties.get(ServiceProperty.NEW_CONSUMER_GROUP);

    log.info(String.format("Starting renaming connector %s to %s", oldConnectorName, newConnectorName));

    //TODO: initialize connectorsUrl in another place
    connectorsUrl = String.format("%s/connectors", serviceProperties.get(ServiceProperty.CONNECTORS_URL));
    log.info(String.format("Connectors URL is: %s", connectorsUrl));

    JSONObject oldConnectorConfig = getOldConnectorConfig(oldConnectorName);
    log.info(String.format("Connector configuration was received: %s", oldConnectorConfig));

    Map<TopicPartition, OffsetAndMetadata> oldOffsets = AdminApiService
      .getOffsetsForConsumerGroup(serviceProperties, adminApiProperties);
    log.info(String.format("Offsets for consumer group %s were received: %s", consumerGroup, oldOffsets));

    List<String> topicsByConsumerGroup = AdminApiService.getTopicsByConsumerGroup(adminApiProperties, serviceProperties);
    log.info(String.format("Topics to which consumer group %s has subscribed were received: %s",
                           consumerGroup, topicsByConsumerGroup));

    consumerApiProperties.put(ConsumerConfig.GROUP_ID_CONFIG, newConnectorName);
    serviceProperties.put(ServiceProperty.CONSUMER_GROUP, newConsumerGroup);
    log.info(String.format("Consumer and service properties for connector %s was created", newConnectorName));

    Consumer<String, String> consumer = ConsumerApiService
      .createConsumerWithListOfTopics(consumerApiProperties, topicsByConsumerGroup);
    log.info(String.format("Creating consumer group %s...", newConsumerGroup));

    AdminApiService.waitUntilConsumerGroupWasCreated(adminApiProperties, serviceProperties);
    log.info(String.format("Consumer group %s was created", newConsumerGroup));

    ConsumerApiService.stopConsumer(consumer);
    log.info("Consumer was stopped");

    AdminApiService.setOffsetsForConsumerGroup(oldOffsets, serviceProperties, adminApiProperties);
    log.info(String.format("Offsets for consumer group %s were set", newConsumerGroup));

    createNewConnector(newConnectorName, oldConnectorConfig);
    log.info(String.format("New connector %s was created", newConnectorName));

    deleteOldConnector(oldConnectorName);
    log.info(String.format("Connector %s was deleted", oldConnectorName));
  }

  public static void moveTopicToAnotherConnector(Properties adminApiProperties,
                                                 Properties consumerPropertiesForOldConnector,
                                                 Map<ServiceProperty, String> servicePropertiesForOldConsumerGroup,
                                                 Map<ServiceProperty, String> servicePropertiesForNewConsumerGroup,
                                                 Map<ServiceProperty, String> serviceProperties) {

    String oldConnectorName = serviceProperties.get(ServiceProperty.OLD_CONNECTOR_NAME);
    String newConnectorName = serviceProperties.get(ServiceProperty.NEW_CONNECTOR_NAME);

    connectorsUrl = String.format("%s/connectors", serviceProperties.get(ServiceProperty.CONNECTORS_URL));
    log.info(String.format("Connectors URL is: %s", connectorsUrl));

    JSONObject oldConnectorConfig = getConnectorConfig(oldConnectorName);
    log.info(String.format("Old connector %s configs was gotten: %s", oldConnectorName, oldConnectorConfig));

    JSONObject newConnectorConfig = getConnectorConfig(newConnectorName);
    log.info(String.format("New connector %s configs was gotten: %s", newConnectorName, newConnectorConfig));

    String topicName = serviceProperties.get(ServiceProperty.TOPIC_NAME);
    log.info(String.format("Starting moving topic from connector %s to %s", oldConnectorName, newConnectorName));

    Map<TopicPartition, OffsetAndMetadata> oldOffsets = AdminApiService
      .getOffsetsForConsumerGroup(servicePropertiesForOldConsumerGroup, adminApiProperties);

    Map<TopicPartition, OffsetAndMetadata> oldOffsetsForTopic = new HashMap<>();
    Map<TopicPartition, OffsetAndMetadata> oldOffsetsForRemainingTopics = new HashMap<>();
    List<String> remainingTopics = new ArrayList<>();

    for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : oldOffsets.entrySet()) {
      if (entry.getKey().topic().equals(topicName)) {
        oldOffsetsForTopic.put(entry.getKey(), entry.getValue());
      } else {
        oldOffsetsForRemainingTopics.put(entry.getKey(), entry.getValue());
        remainingTopics.add(entry.getKey().topic());
      }
    }

    log.info(String.format("Offsets for moving topic %s: %s", topicName, oldOffsetsForTopic));
    log.info(String.format("Offsets for remaining topics %s: %s", remainingTopics, oldOffsetsForRemainingTopics));

    AdminApiService.setOffsetsForConsumerGroup(oldOffsetsForTopic, servicePropertiesForNewConsumerGroup, adminApiProperties);
    log.info(String.format("Offsets for consumer group %s were set", newConnectorName));

    if (!AdminApiService.getTopicsByConsumerGroup(adminApiProperties, servicePropertiesForNewConsumerGroup).contains(topicName)) {
      log.error(String.format("Moving topic %s is not found in new connector %s", topicName, newConnectorName));
      throw new TopicNotFoundForConsumerGroupException(servicePropertiesForNewConsumerGroup.get(ServiceProperty.CONSUMER_GROUP), topicName);
    }

    List<String> topicsByOldConsumerGroup = AdminApiService.getTopicsByConsumerGroup(adminApiProperties, servicePropertiesForOldConsumerGroup);

    if (topicsByOldConsumerGroup.size() == 1) {
      log.info(String.format("Only one topic was found in old connector %s", oldConnectorName));
      Consumer<String, String> consumerOld = ConsumerApiService.createConsumer(consumerPropertiesForOldConnector, serviceProperties);

      consumerOld.unsubscribe();
      consumerOld.close();

      deleteOldConnector(oldConnectorName);
      log.info(String.format("Connector %s deleted", oldConnectorName));
    } else {
      unsubscribeOneTopicFromConnector(servicePropertiesForOldConsumerGroup,
                                       oldOffsetsForRemainingTopics,
                                       adminApiProperties);
      log.info(String.format("Topic %s was removed from old connector %s", topicName, oldConnectorName));
      JSONObject updatedJsonConfigForOldConnector = updateConnectorListOfTopicsInConfig(oldConnectorConfig, servicePropertiesForOldConsumerGroup, adminApiProperties);
      updateConnectorConfig(oldConnectorName, updatedJsonConfigForOldConnector);
      log.info(String.format("Old connector %s config was updated", oldConnectorName));
    }

    JSONObject newJsonConfig = updateConnectorListOfTopicsInConfig(newConnectorConfig, servicePropertiesForNewConsumerGroup, adminApiProperties);
    updateConnectorConfig(newConnectorName, newJsonConfig);
    log.info(String.format("New connector %s config was updated", newConnectorName));
    log.info(String.format("Moving topic %s from connector %s to connector %s was finished", topicName, oldConnectorName, newConnectorName));
  }


  private static void unsubscribeOneTopicFromConnector(Map<ServiceProperty, String> servicePropertiesForOldConsumerGroup,
                                                       Map<TopicPartition, OffsetAndMetadata> oldOffsetsForRemainingTopics,
                                                       Properties adminApiProperties) {
    AdminApiService.deleteConsumerGroup(adminApiProperties, servicePropertiesForOldConsumerGroup);
    AdminApiService.setOffsetsForConsumerGroup(oldOffsetsForRemainingTopics, servicePropertiesForOldConsumerGroup, adminApiProperties);
  }


  private static JSONObject updateConnectorListOfTopicsInConfig(JSONObject connectorConfig, Map<ServiceProperty, String> servicePropertiesForConsumerGroup, Properties adminApiProperties) {
    List<String> topicsByConsumerGroup = AdminApiService.getTopicsByConsumerGroup(adminApiProperties, servicePropertiesForConsumerGroup);
    String list = String.join(", ", topicsByConsumerGroup);
    connectorConfig.put("topics", list);

    return connectorConfig;
  }

  private static void updateConnectorConfig(String connectorName, JSONObject newConfig) {
    try {

      String stringBody = newConfig.toString();

      HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(String.format("%s/%s/config", connectorsUrl, connectorName)))
        .PUT(HttpRequest.BodyPublishers
               .ofInputStream(() -> new ByteArrayInputStream(stringBody.getBytes(StandardCharsets.UTF_8))))
        .header("Accept", "application/json")
        .header("Content-type", "application/json")
        .build();

      HttpResponse<String> response = HttpClient.newBuilder()
        .build()
        .send(request, HttpResponse.BodyHandlers.ofString());

      int responseCode = response.statusCode();
      if (responseCode != 200) {
        throw new UnexpectedResponseCodeException(responseCode, connectorName);
      }
    } catch (URISyntaxException | IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static JSONObject getConnectorConfig(String connectorName) {

    try {
      HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(String.format("%s/%s/config", connectorsUrl, connectorName)))
        .GET()
        .build();

      HttpResponse<String> response = HttpClient.newBuilder()
        .build()
        .send(request, HttpResponse.BodyHandlers.ofString());

      int responseCode = response.statusCode();
      if (responseCode != 200) {
        UnexpectedResponseCodeException error = new UnexpectedResponseCodeException(responseCode, connectorName);
        log.error("Process: receiving configuration of connector", error);
        throw error;
      }

      String responseBody = response.body();

      return new JSONObject(responseBody);

    } catch (URISyntaxException | IOException | InterruptedException e) {
      log.error(e.getMessage(), e);
      throw new ConnectorNotFoundException(connectorName);
    }
  }


  private static JSONObject getOldConnectorConfig(String oldConnectorName) {

    try {
      HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(String.format("%s/%s/config", connectorsUrl, oldConnectorName)))
        .GET()
        .build();

      HttpResponse<String> response = HttpClient.newBuilder()
        .build()
        .send(request, HttpResponse.BodyHandlers.ofString());

      int responseCode = response.statusCode();
      if (responseCode != 200) {
        UnexpectedResponseCodeException error = new UnexpectedResponseCodeException(responseCode, oldConnectorName);
        log.error("Process: receiving configuration of old connector", error);
        throw error;
      }

      String responseBody = response.body();
      JSONObject resultJson = new JSONObject(responseBody);
      resultJson.remove("name");

      return resultJson;

    } catch (URISyntaxException | IOException | InterruptedException e) {
      log.error(e.getMessage(), e);
      throw new ConnectorNotFoundException(oldConnectorName);
    }
  }

  public static void deleteOldConnector(String oldConnectorName) {

    //TODO: initialize connectorsUrl in another place
    try {
      HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(String.format("%s/%s", connectorsUrl, oldConnectorName)))
        .DELETE()
        .build();

      HttpResponse<String> response = HttpClient.newBuilder()
        .build()
        .send(request, HttpResponse.BodyHandlers.ofString());

      int responseCode = response.statusCode();
      if (responseCode != 204) {
        UnexpectedResponseCodeException error = new UnexpectedResponseCodeException(responseCode, oldConnectorName);
        log.error("Process: deleting old connector", error);
        throw error;
      }

    } catch (IOException | URISyntaxException | InterruptedException e) {
      log.error(e.getMessage(), e);
      throw new ConnectorDeletionException(oldConnectorName);
    }
  }

  private static void createNewConnector(String newConnectorName, JSONObject oldConnectorConfig) {

    try {
      JSONObject requestBody = new JSONObject();
      requestBody.put("name", newConnectorName);
      requestBody.put("config", oldConnectorConfig);

      String stringBody = requestBody.toString();

      HttpRequest request = HttpRequest.newBuilder()
        .uri(new URI(connectorsUrl))
        .POST(HttpRequest.BodyPublishers
                .ofInputStream(() -> new ByteArrayInputStream(stringBody.getBytes(StandardCharsets.UTF_8))))
        .header("Accept", "application/json")
        .header("Content-type", "application/json")
        .build();

      HttpResponse<String> response = HttpClient.newBuilder()
        .build()
        .send(request, HttpResponse.BodyHandlers.ofString());

      int responseCode = response.statusCode();
      if (responseCode != 201) {
        UnexpectedResponseCodeException error = new UnexpectedResponseCodeException(responseCode, newConnectorName);
        log.error("Process: creating new connector", error);
        throw error;
      }

    } catch (IOException | URISyntaxException | InterruptedException e) {
      log.error(e.getMessage(), e);
      throw new ConnectorCreatingException(newConnectorName);
    }
  }
}

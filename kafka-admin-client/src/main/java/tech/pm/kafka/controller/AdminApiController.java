package tech.pm.kafka.controller;

import org.apache.kafka.clients.admin.AdminClientConfig;
import tech.pm.kafka.controller.helper.PropertyChecker;
import tech.pm.kafka.controller.helper.PropertyCreator;
import tech.pm.kafka.exceptions.input.RequiredPropertyNotFoundException;
import tech.pm.kafka.property.ServiceProperty;
import tech.pm.kafka.service.admin.AdminApiService;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AdminApiController {

  public static Properties createAdminClientProperties(Map<ServiceProperty, String> inputProperties) {
    Properties apiProperties = new Properties();
    if (inputProperties.get(ServiceProperty.BOOTSTRAP_SERVER) == null) {
      throw new RequiredPropertyNotFoundException("server");
    }
    apiProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, inputProperties.get(ServiceProperty.BOOTSTRAP_SERVER));

    return apiProperties;
  }

  public static void createTopic(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.TOPIC_NAME,
                                                     ServiceProperty.PARTITIONS,
                                                     ServiceProperty.REPLICATION_FACTOR);

    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);
    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);

    String configFile = inputProperties.get(ServiceProperty.CONFIG_FILE);

    if (configFile != null) {
      serviceProperties.put(ServiceProperty.CONFIG_FILE, configFile);
    }

    AdminApiService.createTopic(createAdminClientProperties(inputProperties), serviceProperties);
  }

  public static void deleteTopic(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.TOPIC_NAME);

    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);
    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);

    AdminApiService.deleteTopic(createAdminClientProperties(inputProperties), serviceProperties);
  }

  public static void purgeTopic(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.TOPIC_NAME,
                                                     ServiceProperty.BOOTSTRAP_SERVER,
                                                     ServiceProperty.CONSUMER_GROUP);

    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);
    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);

    AdminApiService.clearTopic(createAdminClientProperties(inputProperties), serviceProperties);
  }

  public static long getCurrentOffsetForConsumerGroup(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.TOPIC_NAME,
                                                     ServiceProperty.PARTITION,
                                                     ServiceProperty.CONSUMER_GROUP);

    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);
    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);

    return AdminApiService.getOffsetForConsumerGroup(createAdminClientProperties(inputProperties), serviceProperties);
  }

  public static void setCurrentOffsetForConsumerGroup(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.TOPIC_NAME,
                                                     ServiceProperty.OFFSET,
                                                     ServiceProperty.CONSUMER_GROUP);

    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);
    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);

    AdminApiService.setOffsetForConsumerGroup(createAdminClientProperties(inputProperties), serviceProperties);
  }

  public static List<String> getTopicsNames(Map<ServiceProperty, String> inputProperties) {
    return AdminApiService.getTopics(createAdminClientProperties(inputProperties));
  }

  public static List<String> getConsumers(Map<ServiceProperty, String> inputProperties) {
    return AdminApiService.getConsumerGroups(createAdminClientProperties(inputProperties));
  }

  public static List<String> getTopicsByConsumerGroup(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.CONSUMER_GROUP);
    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);
    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);

    return AdminApiService.getTopicsByConsumerGroup(createAdminClientProperties(inputProperties), serviceProperties);
  }

  public static List<String> getTopicConfig(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.TOPIC_NAME);
    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);
    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);

    return AdminApiService.getTopicConfig(createAdminClientProperties(inputProperties), serviceProperties);
  }

  public static Map<String, Object> getTopicDetails(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.TOPIC_NAME);
    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);
    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);

    return AdminApiService.getTopicDetails(createAdminClientProperties(inputProperties), serviceProperties);
  }

  public static void deleteConsumerGroup(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.CONSUMER_GROUP);
    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);
    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);

    AdminApiService.deleteConsumerGroup(createAdminClientProperties(inputProperties), serviceProperties);
  }


}

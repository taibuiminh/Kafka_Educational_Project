package tech.pm.kafka.controller;

import tech.pm.kafka.controller.helper.PropertyChecker;
import tech.pm.kafka.controller.helper.PropertyCreator;
import tech.pm.kafka.property.ServiceProperty;
import tech.pm.kafka.service.connect.rest.ConnectRestApiService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConnectRestApiController {

  public static void renameKafkaConnector(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.CONSUMER_GROUP,
                                                     ServiceProperty.NEW_CONSUMER_GROUP,
                                                     ServiceProperty.OLD_CONNECTOR_NAME,
                                                     ServiceProperty.NEW_CONNECTOR_NAME,
                                                     ServiceProperty.CONNECTORS_URL);

    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);

    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);
    Properties adminProperties = AdminApiController.createAdminClientProperties(inputProperties);
    Properties consumerProperties = ConsumerApiController.createConsumerProperties(inputProperties);

    ConnectRestApiService.renameKafkaConnector(adminProperties, consumerProperties, serviceProperties);
  }

  public static void moveTopicToAnotherConnector(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.OLD_CONNECTOR_NAME, //connect-dwh-(connectorName) - full is consumer group
                                                     ServiceProperty.NEW_CONNECTOR_NAME,
                                                     ServiceProperty.CONNECTORS_URL,
                                                     ServiceProperty.TOPIC_NAME);

    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);

    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty); // all required properties

    Properties adminProperties = AdminApiController.createAdminClientProperties(inputProperties); // for all methods - server

    String newConsumerGroup = getConsumerGroupNameFromConnectorName(inputProperties.get(ServiceProperty.NEW_CONNECTOR_NAME));
    String oldConsumerGroup = getConsumerGroupNameFromConnectorName(inputProperties.get(ServiceProperty.OLD_CONNECTOR_NAME));
    inputProperties.put(ServiceProperty.CONSUMER_GROUP, newConsumerGroup);

    Map<ServiceProperty, String> servicePropertiesForOldConsumerGroup = new HashMap<>(); // getOffsetsForConsumerGroup - consumer group old connector
    servicePropertiesForOldConsumerGroup.put(ServiceProperty.CONSUMER_GROUP, oldConsumerGroup);
    servicePropertiesForOldConsumerGroup.put(ServiceProperty.BOOTSTRAP_SERVER, inputProperties.get(ServiceProperty.BOOTSTRAP_SERVER));

    Map<ServiceProperty, String> servicePropertiesForNewConsumerGroup = new HashMap<>(); // setOffsetsForConsumerGroup - consumer group new connector
    servicePropertiesForNewConsumerGroup.put(ServiceProperty.CONSUMER_GROUP, newConsumerGroup);
    servicePropertiesForNewConsumerGroup.put(ServiceProperty.BOOTSTRAP_SERVER, inputProperties.get(ServiceProperty.BOOTSTRAP_SERVER));

    Properties consumerPropertiesForOldConnector = ConsumerApiController.createConsumerProperties(servicePropertiesForOldConsumerGroup); //create consumer - consumer group for new connector

    ConnectRestApiService.moveTopicToAnotherConnector(adminProperties,
                                                      consumerPropertiesForOldConnector,
                                                      servicePropertiesForOldConsumerGroup,
                                                      servicePropertiesForNewConsumerGroup,
                                                      serviceProperties);
  }

  private static String getConsumerGroupNameFromConnectorName(String connectorName) {
    return String.format("connect-dwh-%s", connectorName);
  }
}

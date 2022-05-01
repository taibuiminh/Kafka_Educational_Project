package tech.pm.kafka.controller;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import tech.pm.kafka.controller.helper.PropertyChecker;
import tech.pm.kafka.controller.helper.PropertyCreator;
import tech.pm.kafka.exceptions.input.RequiredPropertyNotFoundException;
import tech.pm.kafka.property.ServiceProperty;
import tech.pm.kafka.service.consumer.ConsumerApiService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerApiController {

  public static Properties createConsumerProperties(Map<ServiceProperty, String> inputProperties) {
    Properties apiProperties = new Properties();
    apiProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, inputProperties.get(ServiceProperty.BOOTSTRAP_SERVER));
    apiProperties.put(ConsumerConfig.GROUP_ID_CONFIG, inputProperties.get(ServiceProperty.CONSUMER_GROUP));
    apiProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    apiProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    apiProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    apiProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    apiProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return apiProperties;
  }

  public static String readMessageInTopicByTimestampOrOffset(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.TOPIC_NAME);

    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);
    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);

    String timestamp = inputProperties.get(ServiceProperty.TIMESTAMP);

    if (timestamp != null) {
      serviceProperties.put(ServiceProperty.TIMESTAMP, timestamp);

      return ConsumerApiService.readMessageInTopicByTimestamp(createConsumerProperties(inputProperties), serviceProperties);
    }

    String offset = inputProperties.get(ServiceProperty.OFFSET);
    String partition = inputProperties.get(ServiceProperty.PARTITION);

    if (offset != null && partition != null) {
      serviceProperties.put(ServiceProperty.OFFSET, offset);
      serviceProperties.put(ServiceProperty.PARTITION, partition);

      return ConsumerApiService.readMessageInTopicPartitionByOffset(createConsumerProperties(inputProperties), serviceProperties);
    }

    throw new RequiredPropertyNotFoundException("read message in topic");
  }


  public static void createConsumer(Map<ServiceProperty, String> inputProperties) {
    Map<ServiceProperty, String> serviceProperties = new HashMap<>();
    serviceProperties.put(ServiceProperty.TOPIC_NAME, inputProperties.get(ServiceProperty.TOPIC_NAME));
    ConsumerApiService.createConsumer(createConsumerProperties(inputProperties), serviceProperties);
  }
}

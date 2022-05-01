package tech.pm.kafka.controller;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import tech.pm.kafka.controller.helper.PropertyChecker;
import tech.pm.kafka.controller.helper.PropertyCreator;
import tech.pm.kafka.exceptions.input.RequiredPropertyNotFoundException;
import tech.pm.kafka.property.ServiceProperty;
import tech.pm.kafka.service.producer.ProducerApiService;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ProducerApiController {

  public static Properties createProducerProperties(Map<ServiceProperty, String> inputProperties) {
    Properties producerProperties = new Properties();
    if (inputProperties.get(ServiceProperty.BOOTSTRAP_SERVER) == null) {
      throw new RequiredPropertyNotFoundException("server");
    }
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, inputProperties.get(ServiceProperty.BOOTSTRAP_SERVER));
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    return producerProperties;
  }

  public static void writeToTopicFromFile(Map<ServiceProperty, String> inputProperties) {
    List<ServiceProperty> requiredProperty = List.of(ServiceProperty.TOPIC_NAME,
                                                     ServiceProperty.SOURCE_FILE);

    PropertyChecker.checkRequiredProperties(inputProperties, requiredProperty);
    Map<ServiceProperty, String> serviceProperties = PropertyCreator.createServiceProperties(inputProperties, requiredProperty);

    ProducerApiService.writeToTopicFromFile(createProducerProperties(inputProperties), serviceProperties);
  }
}

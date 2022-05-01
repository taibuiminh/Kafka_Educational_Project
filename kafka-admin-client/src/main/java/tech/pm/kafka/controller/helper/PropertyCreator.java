package tech.pm.kafka.controller.helper;

import tech.pm.kafka.property.ServiceProperty;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PropertyCreator {
  public static Map<ServiceProperty, String> createServiceProperties(Map<ServiceProperty, String> inputProperties, List<ServiceProperty> requiredProperty) {
    Map<ServiceProperty, String> serviceProperties = new HashMap<>();

    for (ServiceProperty property : requiredProperty) {
      serviceProperties.put(property, inputProperties.get(property));
    }
    return serviceProperties;
  }
}

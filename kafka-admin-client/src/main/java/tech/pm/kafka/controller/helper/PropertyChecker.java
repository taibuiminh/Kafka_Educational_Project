package tech.pm.kafka.controller.helper;

import tech.pm.kafka.exceptions.input.RequiredPropertyNotFoundException;
import tech.pm.kafka.property.ServiceProperty;

import java.util.List;
import java.util.Map;

public class PropertyChecker {
  public static void checkRequiredProperties(Map<ServiceProperty, String> inputProperties, List<ServiceProperty> requiredProperties) {
    for (ServiceProperty property : requiredProperties) {
      if (inputProperties.get(property) == null) {
        throw new RequiredPropertyNotFoundException(String.format("Required property not set: %s", property));
      }
    }
  }
}

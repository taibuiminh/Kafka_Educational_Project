package tech.pm.kafka.exceptions.input;

public class RequiredPropertyNotFoundException extends RuntimeException {

  public RequiredPropertyNotFoundException(String message) {
    super("Required property for command: " + message + " not found!");
  }
}

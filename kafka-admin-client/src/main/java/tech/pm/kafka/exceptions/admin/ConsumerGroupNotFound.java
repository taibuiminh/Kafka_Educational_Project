package tech.pm.kafka.exceptions.admin;

public class ConsumerGroupNotFound extends RuntimeException {
  public ConsumerGroupNotFound(String message) {
    super("Consumer group " + message + " is not found");
  }
}

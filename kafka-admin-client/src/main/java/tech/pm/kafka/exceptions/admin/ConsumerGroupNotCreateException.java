package tech.pm.kafka.exceptions.admin;

public class ConsumerGroupNotCreateException extends RuntimeException {

  public ConsumerGroupNotCreateException(String message) {
    super("Consumer group not create exception: " + message);
  }
}

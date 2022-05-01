package tech.pm.kafka.exceptions.consumer;

public class ConsumerCreatingException extends RuntimeException {

  public ConsumerCreatingException(String message) {
    super("Exception while creating consumer: " + message);
  }
}

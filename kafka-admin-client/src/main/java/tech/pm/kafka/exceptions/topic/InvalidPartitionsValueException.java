package tech.pm.kafka.exceptions.topic;

public class InvalidPartitionsValueException extends RuntimeException {

  public InvalidPartitionsValueException(String message) {
    super("Invalid partitions value: " + message);
  }
}

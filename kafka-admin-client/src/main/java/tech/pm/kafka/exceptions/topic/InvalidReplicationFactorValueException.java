package tech.pm.kafka.exceptions.topic;

public class InvalidReplicationFactorValueException extends RuntimeException {

  public InvalidReplicationFactorValueException(String message) {
    super("Invalid replication factor value: " + message);
  }
}

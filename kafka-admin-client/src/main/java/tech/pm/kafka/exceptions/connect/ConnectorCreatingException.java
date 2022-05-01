package tech.pm.kafka.exceptions.connect;

public class ConnectorCreatingException extends RuntimeException {

  public ConnectorCreatingException(String message) {
    super("Exception was thrown while creating: " + message);
  }
}

package tech.pm.kafka.exceptions.connect;

public class ConnectorDeletionException extends RuntimeException {

  public ConnectorDeletionException(String message) {
    super("Exception was thrown while deleting: " + message);
  }
}

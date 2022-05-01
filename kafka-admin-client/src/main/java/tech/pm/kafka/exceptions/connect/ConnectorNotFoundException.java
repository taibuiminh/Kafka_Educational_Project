package tech.pm.kafka.exceptions.connect;

public class ConnectorNotFoundException extends RuntimeException {

  public ConnectorNotFoundException(String message) {
    super("Connector not found: " + message);
  }
}

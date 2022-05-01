package tech.pm.kafka.exceptions.admin;

public class OffsetNotFoundException extends RuntimeException {

  public OffsetNotFoundException(String message) {
    super("Offset is not found for " + message);
  }
}

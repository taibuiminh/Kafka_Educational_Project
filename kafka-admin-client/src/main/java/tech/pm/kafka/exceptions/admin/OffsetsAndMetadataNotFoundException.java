package tech.pm.kafka.exceptions.admin;

public class OffsetsAndMetadataNotFoundException extends RuntimeException {

  public OffsetsAndMetadataNotFoundException(String message) {
    super("Offsets and metadata not found for consumer group: " + message);
  }
}

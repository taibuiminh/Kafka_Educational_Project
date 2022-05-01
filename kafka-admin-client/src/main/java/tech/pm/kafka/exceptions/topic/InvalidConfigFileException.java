package tech.pm.kafka.exceptions.topic;

public class InvalidConfigFileException extends RuntimeException {

  public InvalidConfigFileException(String message) {
    super("Invalid config file: " + message);
  }
}

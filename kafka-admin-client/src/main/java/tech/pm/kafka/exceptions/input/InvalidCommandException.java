package tech.pm.kafka.exceptions.input;

public class InvalidCommandException extends RuntimeException {

  public InvalidCommandException(String message) {
    super("Invalid command: " + message);
  }
}

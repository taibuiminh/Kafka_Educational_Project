package tech.pm.kafka.exceptions.input;

import java.io.IOException;

public class InvalidArgumentException extends IOException {

  public InvalidArgumentException(String message) {
    super("Invalid Argument: " + message);
  }
}

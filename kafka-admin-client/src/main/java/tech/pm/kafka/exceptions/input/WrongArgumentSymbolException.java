package tech.pm.kafka.exceptions.input;

import java.io.IOException;

public class WrongArgumentSymbolException extends IOException {

  public WrongArgumentSymbolException(String message) {
    super("Syntax error in the argument: " + message);
  }
}

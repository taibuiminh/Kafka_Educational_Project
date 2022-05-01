package tech.pm.kafka.exceptions.connect;

public class UnexpectedResponseCodeException extends RuntimeException{

  public UnexpectedResponseCodeException(int code, String connector) {
    super(String.format("Unexpected response code %d received when processing connector %s", code, connector));
  }
}

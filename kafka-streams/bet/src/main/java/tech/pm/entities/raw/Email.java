package tech.pm.entities.raw;

import lombok.Builder;
import lombok.Value;


@Value
@Builder
public class Email {
  String playerId;
  boolean verified;
}

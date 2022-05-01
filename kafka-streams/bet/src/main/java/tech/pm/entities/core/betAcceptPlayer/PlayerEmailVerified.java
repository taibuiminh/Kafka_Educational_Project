package tech.pm.entities.core.betAcceptPlayer;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PlayerEmailVerified {
  String playerId;
  String firstName;
  String lastName;
  String email;
  Boolean isEmailVerified;
  String defaultCurrency;
  String brand;

}


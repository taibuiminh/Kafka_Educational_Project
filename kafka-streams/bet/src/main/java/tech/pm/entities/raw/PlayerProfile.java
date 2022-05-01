package tech.pm.entities.raw;

import lombok.Builder;
import lombok.Value;


@Value
@Builder
public class PlayerProfile {
  String playerId;
  String firstName;
  String lastName;
  String email;
  String defaultCurrency;
  String brand;
}

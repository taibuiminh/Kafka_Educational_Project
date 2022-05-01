package tech.pm.entities.core.betAcceptPlayer;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class BetAcceptPlayer {
  //  String dt;
  String betAcceptPlayerId;
  String betTimestamp;
  String currency;
  double amount;
  double acceptedBetOdd;
  double possiblePayout;
  String playerId;
  String firstName;
  String lastName;
  String email;
  boolean isEmailVerified;
  String defaultCurrency;
  String brand;
}


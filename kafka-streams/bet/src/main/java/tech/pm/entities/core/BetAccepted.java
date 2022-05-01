package tech.pm.entities.core;

import lombok.Builder;
import lombok.Value;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;

import java.util.List;

@Value
@Builder
public class BetAccepted {
  String betId;
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
  List<ItemJoinEvent> itemsList;
}

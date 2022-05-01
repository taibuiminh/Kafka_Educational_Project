package tech.pm.entities.core.betAcceptItem;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ItemJoinEvent {
  int itemIndex;
  String betId;
  double itemAcceptedAmount;
  double itemAcceptedOdd;
  String eventId;
  String eventName;
  String startTime;
}

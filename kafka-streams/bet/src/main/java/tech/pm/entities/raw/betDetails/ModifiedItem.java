package tech.pm.entities.raw.betDetails;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class ModifiedItem {
  int itemIndex;
  String betId;
  String eventId;
  double itemAcceptedAmount;
  double itemAcceptedOdd;
}

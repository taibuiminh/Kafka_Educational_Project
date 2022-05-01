package tech.pm.entities.raw.betDetails;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class Item {
  int itemIndex;
  String tradingType;
  String eventStage;
  double originalOdd;
  double acceptedOdd;
  double amount;
  double baseAmount;
  double possiblePayout;
  double possibleBasePayout;
  String originalScore;
  String lineItemId;
  String eventId;
  String selectionKey;
  String marketKey;
  int originalOutcomeVersion;
  int acceptedOutcomeVersion;
  String trader;
  double eventLimit;
  String eventLimitType;
  double defaultMarketLimit;
  double playerLimit;
  double marketTotalBaseAmount;
  boolean isOutdated;
  double defaultOdd;
  ItemEventDetails itemEventDetails;
  int changes;
  boolean isGoldbet;
}

package tech.pm.entities.raw;

import lombok.Builder;
import lombok.Value;
import tech.pm.entities.raw.betDetails.Item;

import java.util.List;

@Value
@Builder
public class Bet {
  String betId;
  String createTime;
  String acceptTime;
  String playerId;
  int playerSegmentId;
  int playerProfitStatus;
  int playerBetNumber;
  String priceChangePolicy;
  String playerIp;
  double amount;
  double baseAmount;
  String currencyId;//TODO: Ask about this field. Sometimes it does not present in json
  double exchangeRate;
  String betType;
  int betSize;
  int systemSize;
  double originalBetOdd;
  double acceptedBetOdd;
  String channel;
  String brandId;
  String language;
  List<Item> items;
  boolean isBlocked;
  boolean isMaxBet;
  boolean isOverask;
  boolean isTestbet;
  boolean isFreebet;
  int platform;
  String extraData;
  boolean wasEdited;

  //  String bonus;
//  String placementTypes;
}

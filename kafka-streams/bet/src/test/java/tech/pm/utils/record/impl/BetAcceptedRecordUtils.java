package tech.pm.utils.record.impl;


import org.apache.kafka.streams.KeyValue;
import org.json.JSONArray;
import org.json.JSONObject;
import tech.pm.entities.core.BetAccepted;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;
import tech.pm.updator.JsonUpdater;
import tech.pm.utils.record.TestRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BetAcceptedRecordUtils implements TestRecordUtils<BetAccepted> {

  @Override
  public List<KeyValue<String, BetAccepted>> generateExpectedKeyValueRecords(int idFrom, int idTo,
                                                                             JSONObject json,
                                                                             JsonUpdater updater
  ) {
    List<KeyValue<String, BetAccepted>> expectedRecords = new ArrayList<>(idTo - idFrom);


    String betTimestamp = json.getString("betTimestamp");
    String currency = json.getString("currency");
    double amount = json.getDouble("amount");
    double acceptedBetOdd = json.getDouble("acceptedBetOdd");
    double possiblePayout = json.getDouble("possiblePayout");

    String firstName = json.getString("firstName");
    String lastName = json.getString("lastName");
    String email = json.getString("email");
    boolean isEmailVerified = json.getBoolean("isEmailVerified");
    String defaultCurrency = json.getString("defaultCurrency");
    String brand = json.getString("brand");


    for (int i = idFrom; i < idTo; i++) {
      String key = updater.update(json, i);

      String betId = json.getString("betId");
      String playerId = json.getString("playerId");
      JSONArray items = json.optJSONArray("items");
      List<ItemJoinEvent> itemJoinEventList = getListOfItems(items, betId);

      BetAccepted value = BetAccepted.builder()
        .betId(betId)
        .betTimestamp(betTimestamp)
        .currency(currency)
        .amount(amount)
        .acceptedBetOdd(acceptedBetOdd)
        .possiblePayout(possiblePayout)
        .playerId(playerId)
        .firstName(firstName)
        .lastName(lastName)
        .email(email)
        .isEmailVerified(isEmailVerified)
        .defaultCurrency(defaultCurrency)
        .brand(brand)
        .itemsList(itemJoinEventList)
        .build();

      expectedRecords.add(new KeyValue<>(key, value));

      log.info("Generate expected record: key - {}, value - {}", key, value);
    }

    Collections.shuffle(expectedRecords);

    return expectedRecords;
  }

  private List<ItemJoinEvent> getListOfItems(JSONArray items, String betItemId) {
    List<ItemJoinEvent> itemJoinEventList = new ArrayList<>();
    if (items != null) {
      for (int i = 0; i < items.length(); i++) {
        JSONObject item = items.getJSONObject(i);
        int itemIndex = item.getInt("itemIndex");
        double itemAcceptedAmount = item.getDouble("itemAcceptedAmount");
        double itemAcceptedOdd = item.getDouble("itemAcceptedOdd");
        String eventId = "E-ID-" + i;
        String eventName = item.getString("eventName");
        String startTime = item.getString("startTime");
        ItemJoinEvent itemJoinEvent = ItemJoinEvent.builder()
          .itemIndex(itemIndex)
          .betId(betItemId)
          .itemAcceptedAmount(itemAcceptedAmount)
          .itemAcceptedOdd(itemAcceptedOdd)
          .eventId(eventId)
          .eventName(eventName)
          .startTime(startTime)
          .build();
        itemJoinEventList.add(itemJoinEvent);
      }
    }
    return itemJoinEventList;
  }
}


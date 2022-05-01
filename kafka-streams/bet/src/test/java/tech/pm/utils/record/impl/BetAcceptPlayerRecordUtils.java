package tech.pm.utils.record.impl;

import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;
import tech.pm.entities.core.betAcceptPlayer.BetAcceptPlayer;
import tech.pm.updator.JsonUpdater;
import tech.pm.utils.record.TestRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BetAcceptPlayerRecordUtils implements TestRecordUtils<BetAcceptPlayer> {

  @Override
  public List<KeyValue<String, BetAcceptPlayer>> generateExpectedKeyValueRecords(int idFrom, int idTo,
                                                                                 JSONObject json,
                                                                                 JsonUpdater updater
  ) {
    List<KeyValue<String, BetAcceptPlayer>> expectedRecords = new ArrayList<>(idTo - idFrom);


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

      String betAcceptPlayerId = json.getString("betAcceptPlayerId");
      String playerId = json.getString("playerId");

      BetAcceptPlayer value = BetAcceptPlayer.builder()
        .betAcceptPlayerId(betAcceptPlayerId)
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
        .build();

      expectedRecords.add(new KeyValue<>(key, value));

      log.info("Generate expected record: key - {}, value - {}", key, value);
    }

    Collections.shuffle(expectedRecords);

    return expectedRecords;
  }
}

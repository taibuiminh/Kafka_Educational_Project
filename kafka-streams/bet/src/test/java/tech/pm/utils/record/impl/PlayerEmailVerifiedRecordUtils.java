package tech.pm.utils.record.impl;

import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;
import tech.pm.entities.core.betAcceptPlayer.PlayerEmailVerified;
import tech.pm.updator.JsonUpdater;
import tech.pm.utils.record.TestRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PlayerEmailVerifiedRecordUtils implements TestRecordUtils<PlayerEmailVerified> {

  @Override
  public List<KeyValue<String, PlayerEmailVerified>> generateExpectedKeyValueRecords(int idFrom, int idTo,
                                                                                     JSONObject json,
                                                                                     JsonUpdater updater
  ) {
    List<KeyValue<String, PlayerEmailVerified>> expectedRecords = new ArrayList<>(idTo - idFrom);

    String firstName = json.optString("firstName", null);
    String lastName = json.optString("lastName", null);
    String email = json.optString("email", null);
    Boolean isEmailVerified = json.getBoolean("isEmailVerified");
    String defaultCurrency = json.getString("defaultCurrency");
    String brand = json.getString("brand");

    for (int i = idFrom; i < idTo; i++) {
      String key = updater.update(json, i);

      String playerId = json.getString("playerId");

      PlayerEmailVerified value = PlayerEmailVerified.builder()
        .playerId(playerId)
        .brand(brand)
        .firstName(firstName)
        .lastName(lastName)
        .email(email)
        .isEmailVerified(isEmailVerified)
        .defaultCurrency(defaultCurrency)
        .build();

      expectedRecords.add(new KeyValue<>(key, value));

      log.info("Generate expected record: key - {}, value - {}", key, value);
    }

    Collections.shuffle(expectedRecords);

    return expectedRecords;
  }
}

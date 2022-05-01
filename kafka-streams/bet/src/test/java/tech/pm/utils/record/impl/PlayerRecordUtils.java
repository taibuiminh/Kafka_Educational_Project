package tech.pm.utils.record.impl;

import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;
import tech.pm.entities.core.betAcceptPlayer.Player;
import tech.pm.updator.JsonUpdater;
import tech.pm.utils.record.TestRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PlayerRecordUtils implements TestRecordUtils<Player> {

  @Override
  public List<KeyValue<String, Player>> generateExpectedKeyValueRecords(int idFrom, int idTo,
                                                                        JSONObject json,
                                                                        JsonUpdater updater
  ) {
    List<KeyValue<String, Player>> expectedRecords = new ArrayList<>(idTo - idFrom);

    String firstName = json.optString("firstName", null);
    String lastName = json.optString("lastName", null);
    String email = json.optString("email", null);
    Boolean isEmailVerified = json.getBoolean("isEmailVerified");
    String defaultCurrency = json.getString("defaultCurrency");
    String brand = json.getString("brand");
    boolean testPlayer = json.getBoolean("testPlayer");

    for (int i = idFrom; i < idTo; i++) {
      String key = updater.update(json, i);

      String playerId = json.getString("playerId");

      Player value = Player.builder()
        .playerId(playerId)
        .brand(brand)
        .firstName(firstName)
        .lastName(lastName)
        .email(email)
        .isEmailVerified(isEmailVerified)
        .defaultCurrency(defaultCurrency)
        .testPlayer(testPlayer)
        .build();

      expectedRecords.add(new KeyValue<>(key, value));

      log.info("Generate expected record: key - {}, value - {}", key, value);
    }

    Collections.shuffle(expectedRecords);

    return expectedRecords;
  }
}

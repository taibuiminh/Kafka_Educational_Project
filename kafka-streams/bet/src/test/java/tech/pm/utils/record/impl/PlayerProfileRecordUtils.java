package tech.pm.utils.record.impl;

import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;
import tech.pm.entities.raw.PlayerProfile;
import tech.pm.updator.JsonUpdater;
import tech.pm.utils.record.TestRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class PlayerProfileRecordUtils implements TestRecordUtils<PlayerProfile> {

  @Override
  public List<KeyValue<String, PlayerProfile>> generateExpectedKeyValueRecords(int idFrom, int idTo,
                                                                               JSONObject json,
                                                                               JsonUpdater updater
  ) {
    List<KeyValue<String, PlayerProfile>> expectedRecords = new ArrayList<>(idTo - idFrom);

    String brand = json.getString("brand");

    JSONObject profile = json.getJSONObject("profile");

    String firstName = profile.optString("firstName", null);
    String lastName = profile.optString("lastName", null);
    String email = profile.optString("email", null);
    String defaultCurrency = profile.getString("defaultCurrency");

    for (int i = idFrom; i < idTo; i++) {
      String key = updater.update(json, i);

      String playerId = json.getString("id");

      PlayerProfile value = PlayerProfile.builder()
        .playerId(playerId)
        .brand(brand)
        .firstName(firstName)
        .lastName(lastName)
        .email(email)
        .defaultCurrency(defaultCurrency)
        .build();

      expectedRecords.add(new KeyValue<>(key, value));

      log.info("Generate expected record: key - {}, value - {}", key, value);
    }

    Collections.shuffle(expectedRecords);

    return expectedRecords;
  }
}

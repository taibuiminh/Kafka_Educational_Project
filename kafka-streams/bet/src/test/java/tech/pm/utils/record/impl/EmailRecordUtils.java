package tech.pm.utils.record.impl;

import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;
import tech.pm.entities.raw.Email;
import tech.pm.updator.JsonUpdater;
import tech.pm.utils.record.TestRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EmailRecordUtils implements TestRecordUtils<Email> {

  @Override
  public List<KeyValue<String, Email>> generateExpectedKeyValueRecords(int idFrom, int idTo,
                                                                       JSONObject json,
                                                                       JsonUpdater updater
  ) {
    List<KeyValue<String, Email>> expectedRecords = new ArrayList<>(idTo - idFrom);

    boolean verified = json.getBoolean("isEmailVerified");

    for (int i = idFrom; i < idTo; i++) {
      String key = updater.update(json, i);

      String playerId = json.getString("playerId");

      Email value = Email.builder()
        .playerId(playerId)
        .verified(verified)
        .build();

      expectedRecords.add(new KeyValue<>(key, value));

      log.info("Generate expected record: key - {}, value - {}", key, value);
    }

    Collections.shuffle(expectedRecords);

    return expectedRecords;
  }
}

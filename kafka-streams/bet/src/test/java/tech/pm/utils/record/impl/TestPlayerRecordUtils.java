package tech.pm.utils.record.impl;

import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;
import tech.pm.entities.raw.TestPlayer;
import tech.pm.updator.JsonUpdater;
import tech.pm.utils.record.TestRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestPlayerRecordUtils implements TestRecordUtils<TestPlayer> {

  @Override
  public List<KeyValue<String, TestPlayer>> generateExpectedKeyValueRecords(int idFrom, int idTo,
                                                                            JSONObject json,
                                                                            JsonUpdater updater
  ) {
    List<KeyValue<String, TestPlayer>> expectedRecords = new ArrayList<>(idTo - idFrom);

    boolean testPlayer = json.getBoolean("testPlayer");

    for (int i = idFrom; i < idTo; i++) {
      String key = updater.update(json, i);

      String id = json.getString("id");

      TestPlayer value = TestPlayer.builder()
        .id(id)
        .testPlayer(testPlayer)
        .build();

      expectedRecords.add(new KeyValue<>(key, value));

      log.info("Generate expected record: key - {}, value - {}", key, value);
    }

    Collections.shuffle(expectedRecords);

    return expectedRecords;
  }
}

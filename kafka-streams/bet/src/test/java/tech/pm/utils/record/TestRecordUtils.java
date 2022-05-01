package tech.pm.utils.record;

import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.pm.updator.JsonUpdater;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public interface TestRecordUtils<T> {
  Logger log = LoggerFactory.getLogger(TestRecordUtils.class);

  static List<KeyValue<String, String>> generateTestKeyValueRecords(int idFrom, int idTo,
                                                                    JSONObject json,
                                                                    JsonUpdater updator
  ) {
    List<KeyValue<String, String>> testRecords = new ArrayList<>(idTo - idFrom);

    for (int i = idFrom; i < idTo; i++) {
      String key = updator.update(json, i);
      String value = json.toString();

      testRecords.add(new KeyValue<>(key, value));

      log.info("Generate test record: key - {}, value - {}", key, value);
    }

    Collections.shuffle(testRecords);

    return testRecords;
  }

  List<KeyValue<String, T>> generateExpectedKeyValueRecords(int idFrom, int idTo,
                                                            JSONObject json,
                                                            JsonUpdater updator);
}

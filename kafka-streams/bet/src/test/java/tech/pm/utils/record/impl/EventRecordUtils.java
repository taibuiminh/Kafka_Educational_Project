package tech.pm.utils.record.impl;

import org.apache.kafka.streams.KeyValue;
import org.json.JSONObject;
import tech.pm.entities.raw.Event;
import tech.pm.updator.JsonUpdater;
import tech.pm.utils.record.TestRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EventRecordUtils implements TestRecordUtils<Event> {

  @Override
  public List<KeyValue<String, Event>> generateExpectedKeyValueRecords(int idFrom, int idTo,
                                                                       JSONObject json,
                                                                       JsonUpdater updater
  ) {
    List<KeyValue<String, Event>> expectedRecords = new ArrayList<>(idTo - idFrom);


    String sport = json.getString("Sport");
    String startTime = json.getString("StartTime");
    JSONObject nameJson = json.getJSONObject("Name");
    String name = nameJson.getString("en");

    for (int i = idFrom; i < idTo; i++) {
      String key = updater.update(json, i);

      String id = json.getString("Id");


      Event value = Event.builder()
        .id(id)
        .name(name)
        .sport(sport)
        .start_time(startTime)
        .build();

      expectedRecords.add(new KeyValue<>(key, value));

      log.info("Generate expected record: key - {}, value - {}", key, value);
    }

    Collections.shuffle(expectedRecords);

    return expectedRecords;
  }
}

package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.raw.Event;

import java.util.Map;

@Slf4j
public class EventDeserializer implements Deserializer<Event> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public Event deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      JSONObject eventJson = new JSONObject(new String(data));

      String id = eventJson.getString("Id");
      String sport = eventJson.getString("Sport");
      String status = eventJson.getString("Status");
      String startTime = eventJson.getString("StartTime");
      JSONObject nameJson = eventJson.getJSONObject("Name");
      String name = nameJson.getString("en");

      return Event.builder()
        .id(id)
        .name(name)
        .sport(sport)
        .status(status)
        .start_time(startTime)
        .build();

    } catch (JSONException e) {
      log.error("Skipping record with bad data: {} Error:", new String(data));
    }
    return null;
  }

  @Override
  public void close() {
    Deserializer.super.close();
  }
}

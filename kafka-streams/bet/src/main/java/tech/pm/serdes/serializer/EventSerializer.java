package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.raw.Event;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class EventSerializer implements Serializer<Event> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, Event event) {
    if (event == null)
      return null;

    JSONObject output = new JSONObject();
    output.put("Id", event.getId());
    output.put("Sport", event.getSport());
    output.put("Status", event.getStatus());
    output.put("StartTime", event.getStart_time());

    JSONObject name = new JSONObject();
    name.put("en", event.getName());

    output.put("Name", name);
    return JSONWriter.valueToString(output).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}

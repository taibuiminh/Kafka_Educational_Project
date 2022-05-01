package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ItemJoinEventSerializer implements Serializer<ItemJoinEvent> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, ItemJoinEvent itemJoinEvent) {
    if (itemJoinEvent == null)
      return null;

    JSONObject output = new JSONObject();
    output.put("itemIndex", itemJoinEvent.getItemIndex());
    output.put("betId", itemJoinEvent.getBetId());
    output.put("itemAcceptedAmount", itemJoinEvent.getItemAcceptedAmount());
    output.put("itemAcceptedOdd", itemJoinEvent.getItemAcceptedOdd());
    output.put("eventId", itemJoinEvent.getEventId());
    output.put("eventName", itemJoinEvent.getEventName());
    output.put("startTime", itemJoinEvent.getStartTime());

    return JSONWriter.valueToString(output).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}


package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ItemJoinEventListSerializer implements Serializer<List<ItemJoinEvent>> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, List<ItemJoinEvent> itemJoinEventList) {
    if (itemJoinEventList == null)
      return null;

    List<JSONObject> jsonObjectList = new ArrayList<>();
    JSONObject json = new JSONObject();
    for (ItemJoinEvent itemJoinEvent : itemJoinEventList) {
      JSONObject output = new JSONObject();
      output.put("itemIndex", itemJoinEvent.getItemIndex());
      output.put("betId", itemJoinEvent.getBetId());
      output.put("itemAcceptedAmount", itemJoinEvent.getItemAcceptedAmount());
      output.put("itemAcceptedOdd", itemJoinEvent.getItemAcceptedOdd());
      output.put("eventId", itemJoinEvent.getEventId());
      output.put("eventName", itemJoinEvent.getEventName());
      output.put("startTime", itemJoinEvent.getStartTime());
      jsonObjectList.add(output);
    }
    json.put("itemJoinEventList", jsonObjectList);
    return JSONWriter.valueToString(json).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}


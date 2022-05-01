package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.core.betAcceptItem.BetAcceptItem;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BetAcceptItemSerializer implements Serializer<BetAcceptItem> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, BetAcceptItem betAcceptItem) {
    if (betAcceptItem == null)
      return null;

    JSONObject output = new JSONObject();
    output.put("betItemId", betAcceptItem.getBetItemId());
    List<JSONObject> items = createItemJoinEvenJsonList(betAcceptItem);
    output.put("items", items);


    return JSONWriter.valueToString(output).getBytes(StandardCharsets.UTF_8);
  }

  private List<JSONObject> createItemJoinEvenJsonList(BetAcceptItem betAcceptItem) {
    List<JSONObject> jsonObjectList = new ArrayList<>();
    List<ItemJoinEvent> itemJoinEventList = betAcceptItem.getItems();
    for (ItemJoinEvent itemJoinEvent : itemJoinEventList) {
      JSONObject jsonObject = new JSONObject();
      jsonObject.put("itemIndex", itemJoinEvent.getItemIndex());
      jsonObject.put("betId", itemJoinEvent.getBetId());
      jsonObject.put("itemAcceptedAmount", itemJoinEvent.getItemAcceptedAmount());
      jsonObject.put("itemAcceptedOdd", itemJoinEvent.getItemAcceptedOdd());
      jsonObject.put("eventId", itemJoinEvent.getEventId());
      jsonObject.put("eventName", itemJoinEvent.getEventName());
      jsonObject.put("startTime", itemJoinEvent.getStartTime());

      jsonObjectList.add(jsonObject);
    }
    return jsonObjectList;
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}

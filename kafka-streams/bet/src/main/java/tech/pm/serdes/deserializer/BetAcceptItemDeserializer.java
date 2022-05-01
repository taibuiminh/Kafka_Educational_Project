package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.core.betAcceptItem.BetAcceptItem;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class BetAcceptItemDeserializer implements Deserializer<BetAcceptItem> {


  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public BetAcceptItem deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      JSONObject inputJson = new JSONObject(new String(data));
      String betItemId = inputJson.getString("betItemId");

      JSONArray itemsJsonArray = inputJson.optJSONArray("items");
      List<ItemJoinEvent> items = getListOfItems(itemsJsonArray);

      return BetAcceptItem.builder()
        .betItemId(betItemId)
        .items(items)
        .build();
    } catch (JSONException e) {
      log.error("Skipping record with bad data: {} Error:", new String(data), e);

      return null;
    }
  }

  private List<ItemJoinEvent> getListOfItems(JSONArray itemsJsonArray) {
    List<ItemJoinEvent> items = new ArrayList<>();
    if (itemsJsonArray != null) {
      for (int i = 0; i < itemsJsonArray.length(); i++) {
        JSONObject item = itemsJsonArray.getJSONObject(i);
        int itemIndex = item.getInt("itemIndex");
        String betId = item.getString("betId");
        double itemAcceptedAmount = item.getDouble("itemAcceptedAmount");
        double itemAcceptedOdd = item.getDouble("itemAcceptedOdd");
        String eventId = item.getString("eventId");
        String eventName = item.getString("eventName");
        String startTime = item.getString("startTime");
        ItemJoinEvent itemJoinEvent = ItemJoinEvent.builder()
          .itemIndex(itemIndex)
          .betId(betId)
          .itemAcceptedAmount(itemAcceptedAmount)
          .itemAcceptedOdd(itemAcceptedOdd)
          .eventId(eventId)
          .eventName(eventName)
          .startTime(startTime)
          .build();
        items.add(itemJoinEvent);
      }
    }
    return items;
  }

  @Override
  public void close() {
    Deserializer.super.close();
  }
}

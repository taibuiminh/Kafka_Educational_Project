package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ItemJoinEventListDeserializer implements Deserializer<List<ItemJoinEvent>> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public List<ItemJoinEvent> deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      List<ItemJoinEvent> list = new ArrayList<>();
      JSONObject json = new JSONObject(new String(data));
      JSONArray itemJoinEventList = json.getJSONArray("itemJoinEventList");
      for (int i = 0; i < itemJoinEventList.length(); i++) {
        JSONObject itemJoinEvent = itemJoinEventList.getJSONObject(i);
        int itemIndex = itemJoinEvent.getInt("itemIndex");
        String betId = itemJoinEvent.getString("betId");
        double itemAcceptedAmount = itemJoinEvent.getDouble("itemAcceptedAmount");
        double itemAcceptedOdd = itemJoinEvent.getDouble("itemAcceptedOdd");
        String eventId = itemJoinEvent.getString("eventId");
        String eventName = itemJoinEvent.getString("eventName");
        String startTime = itemJoinEvent.getString("startTime");
        ItemJoinEvent itemJoinEventObject = ItemJoinEvent.builder()
          .itemIndex(itemIndex)
          .betId(betId)
          .itemAcceptedOdd(itemAcceptedOdd)
          .itemAcceptedAmount(itemAcceptedAmount)
          .eventId(eventId)
          .eventName(eventName)
          .startTime(startTime)
          .build();
        list.add(itemJoinEventObject);
      }
      return list;

    } catch (JSONException e) {
      log.error("Skipping record with bad data: {} Error:", new String(data), e);
    }
    return null;

  }

  @Override
  public void close() {
    Deserializer.super.close();
  }
}

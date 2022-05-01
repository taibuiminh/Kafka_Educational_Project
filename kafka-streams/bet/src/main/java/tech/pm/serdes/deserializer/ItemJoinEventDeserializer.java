package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;

import java.util.Map;

@Slf4j
public class ItemJoinEventDeserializer implements Deserializer<ItemJoinEvent> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public ItemJoinEvent deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      JSONObject itemJoinEvent = new JSONObject(new String(data));

      int itemIndex = itemJoinEvent.getInt("itemIndex");
      String betId = itemJoinEvent.getString("betId");
      double itemAcceptedAmount = itemJoinEvent.getDouble("itemAcceptedAmount");
      double itemAcceptedOdd = itemJoinEvent.getDouble("itemAcceptedOdd");
      String eventId = itemJoinEvent.getString("eventId");
      String eventName = itemJoinEvent.getString("eventName");
      String startTime = itemJoinEvent.getString("startTime");

      return ItemJoinEvent.builder()
        .itemIndex(itemIndex)
        .betId(betId)
        .itemAcceptedOdd(itemAcceptedOdd)
        .itemAcceptedAmount(itemAcceptedAmount)
        .eventId(eventId)
        .eventName(eventName)
        .startTime(startTime)
        .build();

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

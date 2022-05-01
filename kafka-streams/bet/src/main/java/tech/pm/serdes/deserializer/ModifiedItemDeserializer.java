package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.raw.betDetails.ModifiedItem;

import java.util.Map;

@Slf4j
public class ModifiedItemDeserializer implements Deserializer<ModifiedItem> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public ModifiedItem deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      JSONObject itemJoinEvent = new JSONObject(new String(data));

      int itemIndex = itemJoinEvent.getInt("itemIndex");
      String betId = itemJoinEvent.getString("betId");
      String eventId = itemJoinEvent.getString("eventId");
      double itemAcceptedAmount = itemJoinEvent.getDouble("itemAcceptedAmount");
      double itemAcceptedOdd = itemJoinEvent.getDouble("itemAcceptedOdd");

      return ModifiedItem.builder()
        .itemIndex(itemIndex)
        .betId(betId)
        .eventId(eventId)
        .itemAcceptedOdd(itemAcceptedOdd)
        .itemAcceptedAmount(itemAcceptedAmount)
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


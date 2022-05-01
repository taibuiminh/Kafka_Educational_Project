package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.raw.betDetails.ModifiedItem;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ModifiedItemSerializer implements Serializer<ModifiedItem> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, ModifiedItem modifiedItem) {
    if (modifiedItem == null)
      return null;

    JSONObject output = new JSONObject();
    output.put("itemIndex", modifiedItem.getItemIndex());
    output.put("betId", modifiedItem.getBetId());
    output.put("eventId", modifiedItem.getEventId());
    output.put("itemAcceptedAmount", modifiedItem.getItemAcceptedAmount());
    output.put("itemAcceptedOdd", modifiedItem.getItemAcceptedOdd());

    return JSONWriter.valueToString(output).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}


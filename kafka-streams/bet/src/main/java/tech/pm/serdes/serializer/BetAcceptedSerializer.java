package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.core.BetAccepted;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BetAcceptedSerializer implements Serializer<BetAccepted> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, BetAccepted betAccepted) {
    if (betAccepted == null)
      return null;

    JSONObject output = new JSONObject();
    output.put("betId", betAccepted.getBetId());
    output.put("betTimestamp", betAccepted.getBetTimestamp());
    output.put("currency", betAccepted.getCurrency());
    output.put("amount", betAccepted.getAmount());
    output.put("acceptedBetOdd", betAccepted.getAcceptedBetOdd());
    output.put("possiblePayout", betAccepted.getPossiblePayout());
    output.put("playerId", betAccepted.getPlayerId());
    output.put("firstName", betAccepted.getFirstName());
    output.put("lastName", betAccepted.getLastName());
    output.put("email", betAccepted.getEmail());
    output.put("isEmailVerified", betAccepted.isEmailVerified());
    output.put("defaultCurrency", betAccepted.getDefaultCurrency());
    output.put("brand", betAccepted.getBrand());
    List<JSONObject> itemJoinEventList = createItemJoinEvenJsonList(betAccepted);
    output.put("items", itemJoinEventList);


    return JSONWriter.valueToString(output).getBytes(StandardCharsets.UTF_8);
  }

  private List<JSONObject> createItemJoinEvenJsonList(BetAccepted betAccepted) {
    List<JSONObject> jsonObjectList = new ArrayList<>();
    List<ItemJoinEvent> itemsList = betAccepted.getItemsList();
    for (ItemJoinEvent itemJoinEvent : itemsList) {
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

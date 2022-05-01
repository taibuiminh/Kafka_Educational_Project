package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.core.BetAccepted;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class BetAcceptedDeserializer implements Deserializer<BetAccepted> {


  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public BetAccepted deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      JSONObject inputJson = new JSONObject(new String(data));
      String betId = inputJson.getString("betId");
      String betTimestamp = inputJson.getString("betTimestamp");
      String currency = inputJson.getString("currency");
      double amount = inputJson.getDouble("amount");
      double acceptedBetOdd = inputJson.getDouble("acceptedBetOdd");
      double possiblePayout = inputJson.getDouble("possiblePayout");
      String playerId = inputJson.getString("playerId");
      String firstName = inputJson.getString("firstName");
      String lastName = inputJson.getString("lastName");
      String email = inputJson.getString("email");
      boolean isEmailVerified = inputJson.getBoolean("isEmailVerified");
      String defaultCurrency = inputJson.getString("defaultCurrency");
      String brand = inputJson.getString("brand");

      JSONArray items = inputJson.optJSONArray("items");
      List<ItemJoinEvent> itemJoinEventList = getListOfItems(items);

      return BetAccepted.builder()
        .betId(betId)
        .betTimestamp(betTimestamp)
        .currency(currency)
        .amount(amount)
        .acceptedBetOdd(acceptedBetOdd)
        .possiblePayout(possiblePayout)
        .playerId(playerId)
        .firstName(firstName)
        .lastName(lastName)
        .email(email)
        .isEmailVerified(isEmailVerified)
        .defaultCurrency(defaultCurrency)
        .brand(brand)
        .itemsList(itemJoinEventList)
        .build();
    } catch (JSONException e) {
      log.error("Skipping record with bad data: {} Error:", new String(data), e);

      return null;
    }
  }

  private List<ItemJoinEvent> getListOfItems(JSONArray items) {
    List<ItemJoinEvent> itemJoinEventList = new ArrayList<>();
    if (items != null) {
      for (int i = 0; i < items.length(); i++) {
        JSONObject item = items.getJSONObject(i);
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
        itemJoinEventList.add(itemJoinEvent);
      }
    }
    return itemJoinEventList;
  }

  @Override
  public void close() {
    Deserializer.super.close();
  }
}

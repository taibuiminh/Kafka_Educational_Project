package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.core.betAcceptPlayer.BetAcceptPlayer;

import java.util.Map;

@Slf4j
public class BetAcceptPlayerDeserializer implements Deserializer<BetAcceptPlayer> {


  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public BetAcceptPlayer deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      JSONObject inputJson = new JSONObject(new String(data));
      String betAcceptPlayerId = inputJson.getString("betAcceptPlayerId");
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

      return BetAcceptPlayer.builder()
        .betAcceptPlayerId(betAcceptPlayerId)
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
        .build();
    } catch (JSONException e) {
      log.error("Skipping record with bad data: {} Error:", new String(data), e);

      return null;
    }
  }

  @Override
  public void close() {
    Deserializer.super.close();
  }
}

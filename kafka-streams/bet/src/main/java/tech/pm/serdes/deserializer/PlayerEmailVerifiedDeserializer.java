package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.core.betAcceptPlayer.PlayerEmailVerified;

import java.util.Map;

@Slf4j
public class PlayerEmailVerifiedDeserializer implements Deserializer<PlayerEmailVerified> {


  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public PlayerEmailVerified deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      JSONObject inputJson = new JSONObject(new String(data));
      String playerId = inputJson.getString("playerId");
      String firstName = inputJson.optString("firstName", null);
      String lastName = inputJson.optString("lastName", null);
      String email = inputJson.getString("email");
      Boolean isEmailVerified = inputJson.getBoolean("isEmailVerified");
      String defaultCurrency = inputJson.getString("defaultCurrency");
      String brand = inputJson.getString("brand");

      return PlayerEmailVerified.builder()
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

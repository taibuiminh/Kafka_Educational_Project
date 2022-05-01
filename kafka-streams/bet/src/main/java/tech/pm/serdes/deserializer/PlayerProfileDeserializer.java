package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.raw.PlayerProfile;

import java.util.Map;

@Slf4j
public class PlayerProfileDeserializer implements Deserializer<PlayerProfile> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public PlayerProfile deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      JSONObject playerJson = new JSONObject(new String(data));
      String playerId = playerJson.getString("id");
      String brand = playerJson.getString("brand");
      JSONObject profile = playerJson.getJSONObject("profile");
      String firstName = profile.optString("firstName", null);
      String lastName = profile.optString("lastName", null);
      String email = profile.optString("email", null);
      String defaultCurrency = profile.optString("defaultCurrency", null);

      return PlayerProfile.builder()
        .playerId(playerId)
        .firstName(firstName)
        .lastName(lastName)
        .email(email)
        .defaultCurrency(defaultCurrency)
        .brand(brand)
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

package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.raw.Email;

import java.util.Map;

@Slf4j
public class EmailDeserializer implements Deserializer<Email> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public Email deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      JSONObject emailJson = new JSONObject(new String(data));

      String playerId = emailJson.getString("playerId");
      boolean verified = emailJson.getBoolean("verified");

      return Email.builder()
        .playerId(playerId)
        .verified(verified)
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

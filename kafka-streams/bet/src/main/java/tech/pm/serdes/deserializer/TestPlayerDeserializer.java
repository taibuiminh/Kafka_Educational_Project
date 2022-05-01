package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.raw.TestPlayer;

import java.util.Map;

@Slf4j
public class TestPlayerDeserializer implements Deserializer<TestPlayer> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public TestPlayer deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      JSONObject testPlayerJson = new JSONObject(new String(data));

      String id = testPlayerJson.getString("id");
      boolean testPlayer = testPlayerJson.getBoolean("testPlayer");

      return TestPlayer.builder()
        .id(id)
        .testPlayer(testPlayer)
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

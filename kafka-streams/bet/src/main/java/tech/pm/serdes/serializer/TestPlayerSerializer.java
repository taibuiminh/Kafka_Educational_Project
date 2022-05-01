package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.raw.TestPlayer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class TestPlayerSerializer implements Serializer<TestPlayer> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, TestPlayer testPlayer) {
    if (testPlayer == null)
      return null;

    JSONObject output = new JSONObject();
    output.put("id", testPlayer.getId());
    output.put("testPlayer", testPlayer.isTestPlayer());

    return JSONWriter.valueToString(output).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}


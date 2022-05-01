package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.core.betAcceptPlayer.Player;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PlayerSerializer implements Serializer<Player> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, Player playerEmailVerified) {
    if (playerEmailVerified == null)
      return null;

    JSONObject output = new JSONObject();
    output.put("playerId", playerEmailVerified.getPlayerId());
    output.put("firstName", playerEmailVerified.getFirstName());
    output.put("lastName", playerEmailVerified.getLastName());
    output.put("email", playerEmailVerified.getEmail());
    output.put("isEmailVerified", playerEmailVerified.getIsEmailVerified());
    output.put("defaultCurrency", playerEmailVerified.getDefaultCurrency());
    output.put("brand", playerEmailVerified.getBrand());
    output.put("testPlayer", playerEmailVerified.isTestPlayer());


    return JSONWriter.valueToString(output).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}

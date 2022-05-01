package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.raw.PlayerProfile;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class PlayerProfileSerializer implements Serializer<PlayerProfile> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, PlayerProfile playerProfile) {
    if (playerProfile == null)
      return null;

    JSONObject output = new JSONObject();
    output.put("id", playerProfile.getPlayerId());
    output.put("brand", playerProfile.getBrand());

    JSONObject profile = new JSONObject();
    profile.put("firstName", playerProfile.getFirstName());
    profile.put("lastName", playerProfile.getLastName());
    profile.put("email", playerProfile.getEmail());
    profile.put("defaultCurrency", playerProfile.getDefaultCurrency());

    output.put("profile", profile);

    return JSONWriter.valueToString(output).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}

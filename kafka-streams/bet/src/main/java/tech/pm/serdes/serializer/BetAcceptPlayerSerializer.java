package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.core.betAcceptPlayer.BetAcceptPlayer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class BetAcceptPlayerSerializer implements Serializer<BetAcceptPlayer> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, BetAcceptPlayer betAcceptedPlayer) {
    if (betAcceptedPlayer == null)
      return null;

    JSONObject output = new JSONObject();
    output.put("betAcceptPlayerId", betAcceptedPlayer.getBetAcceptPlayerId());
    output.put("betTimestamp", betAcceptedPlayer.getBetTimestamp());
    output.put("currency", betAcceptedPlayer.getCurrency());
    output.put("amount", betAcceptedPlayer.getAmount());
    output.put("acceptedBetOdd", betAcceptedPlayer.getAcceptedBetOdd());
    output.put("possiblePayout", betAcceptedPlayer.getPossiblePayout());
    output.put("playerId", betAcceptedPlayer.getPlayerId());
    output.put("firstName", betAcceptedPlayer.getFirstName());
    output.put("lastName", betAcceptedPlayer.getLastName());
    output.put("email", betAcceptedPlayer.getEmail());
    output.put("isEmailVerified", betAcceptedPlayer.isEmailVerified());
    output.put("defaultCurrency", betAcceptedPlayer.getDefaultCurrency());
    output.put("brand", betAcceptedPlayer.getBrand());


    return JSONWriter.valueToString(output).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}

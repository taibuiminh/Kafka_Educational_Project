package tech.pm.updator.impl;

import org.json.JSONObject;
import tech.pm.updator.JsonUpdater;

public class BetAcceptPlayerJsonUpdater implements JsonUpdater {

  @Override
  public String update(JSONObject json, int i) {
    String id = String.format("B-ID-%s", i);
    String playerId = String.format("P-ID-%s", i);

    json.put("betAcceptPlayerId", id);
    json.put("playerId", playerId);


    return id;
  }
}

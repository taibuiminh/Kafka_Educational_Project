package tech.pm.updator.impl;

import org.json.JSONObject;
import tech.pm.updator.JsonUpdater;

public class EmailJsonUpdater implements JsonUpdater {

  @Override
  public String update(JSONObject json, int i) {
    String id = String.format("P-ID-%s", i);

    json.put("playerId", id);

    return id;
  }
}

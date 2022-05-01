package tech.pm.updator.impl;

import org.json.JSONObject;
import tech.pm.updator.JsonUpdater;

public class TestPlayerJsonUpdater implements JsonUpdater {

  @Override
  public String update(JSONObject json, int i) {
    String id = String.format("P-ID-%s", i);

    json.put("id", id);

    return id;
  }
}

package tech.pm.updator.impl;

import org.json.JSONObject;
import tech.pm.updator.JsonUpdater;

public class EventJsonUpdater implements JsonUpdater {

  @Override
  public String update(JSONObject json, int i) {
    String id = String.format("E-ID-%s", i);

    json.put("Id", id);

    return id;
  }
}

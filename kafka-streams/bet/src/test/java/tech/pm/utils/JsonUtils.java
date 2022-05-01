package tech.pm.utils;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class JsonUtils {
  private final static Logger log = LoggerFactory.getLogger(JsonUtils.class);

  public static JSONObject getJson(String path) {
    JSONObject json = null;

    try (InputStream is = new FileInputStream(path)) {
      json = new JSONObject(IOUtils.toString(is, StandardCharsets.UTF_8));
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }

    return json;
  }
}

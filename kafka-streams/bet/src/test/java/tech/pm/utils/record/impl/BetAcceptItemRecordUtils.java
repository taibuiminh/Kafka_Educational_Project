package tech.pm.utils.record.impl;

import org.apache.kafka.streams.KeyValue;
import org.json.JSONArray;
import org.json.JSONObject;
import tech.pm.entities.core.betAcceptItem.BetAcceptItem;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;
import tech.pm.updator.JsonUpdater;
import tech.pm.utils.record.TestRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BetAcceptItemRecordUtils implements TestRecordUtils<BetAcceptItem> {

  @Override
  public List<KeyValue<String, BetAcceptItem>> generateExpectedKeyValueRecords(int idFrom, int idTo,
                                                                               JSONObject json,
                                                                               JsonUpdater updater) {
    List<KeyValue<String, BetAcceptItem>> expectedRecords = new ArrayList<>(idTo - idFrom);


    for (int i = idFrom; i < idTo; i++) {
      String key = updater.update(json, i);

      String betItemId = json.getString("betItemId");

      JSONArray itemsJsonArray = json.optJSONArray("items");
      List<ItemJoinEvent> items = getListOfItems(itemsJsonArray, betItemId);


      BetAcceptItem value = BetAcceptItem.builder()
        .betItemId(betItemId)
        .items(items)
        .build();

      expectedRecords.add(new KeyValue<>(key, value));

      log.info("Generate expected record: key - {}, value - {}", key, value);
    }

    Collections.shuffle(expectedRecords);

    return expectedRecords;
  }

  private List<ItemJoinEvent> getListOfItems(JSONArray itemsJsonArray, String betItemId) {
    List<ItemJoinEvent> items = new ArrayList<>();
    if (itemsJsonArray != null) {
      for (int i = 0; i < itemsJsonArray.length(); i++) {
        JSONObject item = itemsJsonArray.getJSONObject(i);
        int itemIndex = item.getInt("itemIndex");
        double itemAcceptedAmount = item.getDouble("itemAcceptedAmount");
        double itemAcceptedOdd = item.getDouble("itemAcceptedOdd");
        String eventId = "E-ID-" + i;
        String eventName = item.getString("eventName");
        String startTime = item.getString("startTime");
        ItemJoinEvent itemJoinEvent = ItemJoinEvent.builder()
          .itemIndex(itemIndex)
          .betId(betItemId)
          .itemAcceptedAmount(itemAcceptedAmount)
          .itemAcceptedOdd(itemAcceptedOdd)
          .eventId(eventId)
          .eventName(eventName)
          .startTime(startTime)
          .build();
        items.add(itemJoinEvent);
      }
    }
    return items;
  }
}

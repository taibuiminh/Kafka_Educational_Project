package tech.pm.serdes.deserializer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import tech.pm.entities.raw.betDetails.Item;
import tech.pm.entities.raw.betDetails.ItemEventDetails;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
public class ItemDeserializer implements Deserializer<Item> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public Item deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      JSONObject item = new JSONObject(new String(data));

      int itemIndex = item.getInt("itemIndex");
      String tradingType = item.getString("tradingType");
      String eventStage = item.getString("eventStage");
      double originalOdd = item.getDouble("originalOdd");
      double acceptedOdd = item.getDouble("acceptedOdd");
      double amount = item.getDouble("amount");
      double baseAmount = item.getDouble("baseAmount");
      double possiblePayout = item.getDouble("possiblePayout");
      double possibleBasePayout = item.getDouble("possibleBasePayout");
      String originalScore = item.getString("originalScore");
      String lineItemId = item.getString("lineItemId");
      String eventId = item.getString("eventId");
      String selectionKey = item.getString("selectionKey");
      String marketKey = item.optString("marketKey", null);
      int originalOutcomeVersion = item.getInt("originalOutcomeVersion");
      int acceptedOutcomeVersion = item.getInt("acceptedOutcomeVersion");
      String trader = item.getString("trader");
      double eventLimit = item.optDouble("eventLimit", -1.0);
      String eventLimitType = item.optString("eventLimitType", null);
      double defaultMarketLimit = item.optDouble("defaultMarketLimit", -1.0);
      double playerLimit = item.optDouble("playerLimit", -1.0);
      double marketTotalBaseAmount = item.optDouble("marketTotalBaseAmount", -1.0);
      boolean isOutdated = item.getBoolean("isOutdated");
      double defaultOdd = item.optDouble("defaultOdd", -1.0);
      JSONObject itemEventDetailsJson = item.getJSONObject("event");
      ItemEventDetails itemEventDetails = getItemEventDetails(itemEventDetailsJson);
      int changes = item.optInt("changes", -1);
      boolean isGoldbet = item.optBoolean("isGoldbet", false);

      return Item.builder()
        .itemIndex(itemIndex)
        .tradingType(tradingType)
        .eventStage(eventStage)
        .originalOdd(originalOdd)
        .acceptedOdd(acceptedOdd)
        .amount(amount)
        .baseAmount(baseAmount)
        .possiblePayout(possiblePayout)
        .possibleBasePayout(possibleBasePayout)
        .originalScore(originalScore)
        .lineItemId(lineItemId)
        .eventId(eventId)
        .selectionKey(selectionKey)
        .marketKey(marketKey)
        .originalOutcomeVersion(originalOutcomeVersion)
        .acceptedOutcomeVersion(acceptedOutcomeVersion)
        .trader(trader)
        .eventLimit(eventLimit)
        .eventLimitType(eventLimitType)
        .defaultMarketLimit(defaultMarketLimit)
        .playerLimit(playerLimit)
        .marketTotalBaseAmount(marketTotalBaseAmount)
        .isOutdated(isOutdated)
        .defaultOdd(defaultOdd)
        .itemEventDetails(itemEventDetails)
        .changes(changes)
        .isGoldbet(isGoldbet)
        .build();

    } catch (JSONException e) {
      log.error("Skipping record with bad data: {} Error:", new String(data), e);
    }
    return null;

  }

  public ItemEventDetails getItemEventDetails(JSONObject itemEventDetailsJson) {
    String id = itemEventDetailsJson.getString("id");
    String sportTypeKey = itemEventDetailsJson.getString("sportTypeKey");
    String categoryId = itemEventDetailsJson.getString("categoryId");
    String tournamentId = itemEventDetailsJson.getString("tournamentId");
    String acceptedStartTime = itemEventDetailsJson.getString("acceptedStartTime");
    String competitorType = itemEventDetailsJson.optString("competitorType", null);
    List<String> competitorsIds = new ArrayList<>();
    JSONArray competitorsIdsArray = itemEventDetailsJson.optJSONArray("competitorsIds");
    if (competitorsIdsArray != null) {
      for (Object o : competitorsIdsArray) {
        competitorsIds.add(o.toString());
      }
    }
    String type = itemEventDetailsJson.getString("type");
    return new ItemEventDetails(id, sportTypeKey, categoryId, tournamentId, acceptedStartTime, competitorType, competitorsIds, type);
  }

  @Override
  public void close() {
    Deserializer.super.close();
  }
}

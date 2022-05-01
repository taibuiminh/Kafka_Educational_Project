package tech.pm.utils.record.impl;

import org.apache.kafka.streams.KeyValue;
import org.json.JSONArray;
import org.json.JSONObject;
import tech.pm.entities.raw.Bet;
import tech.pm.entities.raw.betDetails.Item;
import tech.pm.entities.raw.betDetails.ItemEventDetails;
import tech.pm.updator.JsonUpdater;
import tech.pm.utils.record.TestRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BetRecordUtils implements TestRecordUtils<Bet> {

  @Override
  public List<KeyValue<String, Bet>> generateExpectedKeyValueRecords(int idFrom, int idTo,
                                                                     JSONObject betJson,
                                                                     JsonUpdater updater) {
    List<KeyValue<String, Bet>> expectedRecords = new ArrayList<>(idTo - idFrom);

    String createTime = betJson.getString("createTime");
    String acceptTime = betJson.getString("acceptTime");
    int playerSegmentId = betJson.getInt("playerSegmentId");
    int playerProfitStatus = betJson.getInt("playerProfitStatus");
    int playerBetNumber = betJson.getInt("playerBetNumber");
    String priceChangePolicy = betJson.getString("priceChangePolicy");
    String playerIp = betJson.getString("playerIp");
    double amount = betJson.getDouble("amount");
    double baseAmount = betJson.getDouble("baseAmount");
    String currencyId = betJson.optString("currencyId", null);
    double exchangeRate = betJson.getDouble("exchangeRate");
    String betType = betJson.getString("betType");
    int betSize = betJson.getInt("betSize");
    int systemSize = betJson.getInt("systemSize");
    double originalBetOdd = betJson.getDouble("originalBetOdd");
    double acceptedBetOdd = betJson.getDouble("acceptedBetOdd");
    String channel = betJson.getString("channel");
    String brandId = betJson.getString("brandId");
    String language = betJson.optString("language", null);

    JSONArray items = betJson.optJSONArray("items");
    List<Item> itemList = getListOfItems(items);
    boolean isBlocked = betJson.getBoolean("isBlocked");
    boolean isMaxBet = betJson.getBoolean("isMaxBet");
    boolean isOverask = betJson.getBoolean("isOverask");
    boolean isTestbet = betJson.getBoolean("isTestbet");
    boolean isFreebet = betJson.getBoolean("isFreebet");
    int platform = betJson.getInt("platform");
    String extraData = betJson.optString("extraData", null);
    boolean wasEdited = betJson.optBoolean("wasEdited", false);


    for (int i = idFrom; i < idTo; i++) {
      String key = updater.update(betJson, i);

      String betId = betJson.getString("betId");
      String playerId = betJson.getString("playerId");

      Bet value = Bet.builder()
        .betId(betId)
        .createTime(createTime)
        .acceptTime(acceptTime)
        .playerId(playerId)
        .playerSegmentId(playerSegmentId)
        .playerProfitStatus(playerProfitStatus)
        .playerBetNumber(playerBetNumber)
        .priceChangePolicy(priceChangePolicy)
        .playerIp(playerIp)
        .amount(amount)
        .baseAmount(baseAmount)
        .currencyId(currencyId)
        .exchangeRate(exchangeRate)
        .betType(betType)
        .betSize(betSize)
        .systemSize(systemSize)
        .originalBetOdd(originalBetOdd)
        .acceptedBetOdd(acceptedBetOdd)
        .channel(channel)
        .brandId(brandId)
        .language(language)
        .isBlocked(isBlocked)
        .isMaxBet(isMaxBet)
        .isOverask(isOverask)
        .isTestbet(isTestbet)
        .isFreebet(isFreebet)
        .platform(platform)
        .extraData(extraData)
        .wasEdited(wasEdited)
        .items(itemList)
        .build();

      expectedRecords.add(new KeyValue<>(key, value));

      log.info("Generate expected record: key - {}, value - {}", key, value);
    }

    Collections.shuffle(expectedRecords);

    return expectedRecords;
  }

  private List<Item> getListOfItems(JSONArray items) {
    List<Item> itemList = new ArrayList<>();
    if (items != null) {
      for (int i = 0; i < items.length(); i++) {
        JSONObject item = items.getJSONObject(i);
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
        String eventId = "E-ID-" + i;
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
        Item newItem = Item.builder()
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
        itemList.add(newItem);
      }
    }
    return itemList;
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
}

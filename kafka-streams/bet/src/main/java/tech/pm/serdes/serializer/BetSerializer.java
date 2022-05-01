package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.raw.Bet;
import tech.pm.entities.raw.betDetails.Item;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BetSerializer implements Serializer<Bet> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, Bet bet) {
    if (bet == null)
      return null;

    JSONObject output = new JSONObject();
    output.put("betId", bet.getBetId());
    output.put("createTime", bet.getCreateTime());
    output.put("acceptTime", bet.getAcceptTime());
    output.put("playerId", bet.getPlayerId());
    output.put("playerSegmentId", bet.getPlayerSegmentId());
    output.put("playerProfitStatus", bet.getPlayerProfitStatus());
    output.put("playerBetNumber", bet.getPlayerBetNumber());
    output.put("priceChangePolicy", bet.getPriceChangePolicy());
    output.put("playerIp", bet.getPlayerIp());
    output.put("amount", bet.getAmount());
    output.put("baseAmount", bet.getBaseAmount());
    output.put("currencyId", bet.getCurrencyId());
    output.put("exchangeRate", bet.getExchangeRate());
    output.put("betType", bet.getBetType());
    output.put("betSize", bet.getBetSize());
    output.put("systemSize", bet.getSystemSize());
    output.put("originalBetOdd", bet.getOriginalBetOdd());
    output.put("acceptedBetOdd", bet.getAcceptedBetOdd());
    output.put("channel", bet.getChannel());
    output.put("brandId", bet.getBrandId());
    output.put("language", bet.getLanguage());
    List<JSONObject> itemList = createItemJsonList(bet);
    output.put("items", itemList);

    output.put("isBlocked", bet.isBlocked());
    output.put("isMaxBet", bet.isMaxBet());
    output.put("isOverask", bet.isOverask());
    output.put("isTestbet", bet.isTestbet());
    output.put("isFreebet", bet.isFreebet());
    output.put("platform", bet.getPlatform());
    output.put("extraData", bet.getExtraData());
    output.put("wasEdited", bet.isWasEdited());


    return JSONWriter.valueToString(output).getBytes(StandardCharsets.UTF_8);
  }

  private List<JSONObject> createItemJsonList(Bet bet) {
    List<JSONObject> jsonObjectList = new ArrayList<>();
    List<Item> itemList = bet.getItems();
    for (Item item : itemList) {
      JSONObject jsonItem = new JSONObject();
      jsonItem.put("itemIndex", item.getItemIndex());
      jsonItem.put("tradingType", item.getTradingType());
      jsonItem.put("eventStage", item.getEventStage());
      jsonItem.put("originalOdd", item.getOriginalOdd());
      jsonItem.put("acceptedOdd", item.getAcceptedOdd());
      jsonItem.put("defaultOdd", item.getDefaultOdd());
      jsonItem.put("amount", item.getAmount());
      jsonItem.put("baseAmount", item.getBaseAmount());
      jsonItem.put("possiblePayout", item.getPossiblePayout());
      jsonItem.put("possibleBasePayout", item.getPossibleBasePayout());
      jsonItem.put("originalScore", item.getOriginalScore());
      jsonItem.put("lineItemId", item.getLineItemId());
      jsonItem.put("eventId", item.getEventId());
      jsonItem.put("selectionKey", item.getSelectionKey());
      jsonItem.put("originalOutcomeVersion", item.getOriginalOutcomeVersion());
      jsonItem.put("acceptedOutcomeVersion", item.getAcceptedOutcomeVersion());
      jsonItem.put("trader", item.getTrader());
      jsonItem.put("marketKey", item.getMarketKey());
      jsonItem.put("eventLimit", item.getEventLimit());
      jsonItem.put("eventLimitType", item.getEventLimitType());
      jsonItem.put("defaultMarketLimit", item.getDefaultMarketLimit());
      jsonItem.put("playerLimit", item.getPlayerLimit());
      jsonItem.put("marketTotalBaseAmount", item.getMarketTotalBaseAmount());
      jsonItem.put("isOutdated", item.isOutdated());
      jsonItem.put("isGoldBet", item.isGoldbet());
      jsonItem.put("changes", item.getChanges());

      JSONObject eventJson = new JSONObject();
      eventJson.put("id", item.getItemEventDetails().getId());
      eventJson.put("sportTypeKey", item.getItemEventDetails().getSportTypeKey());
      eventJson.put("categoryId", item.getItemEventDetails().getCategoryId());
      eventJson.put("tournamentId", item.getItemEventDetails().getTournamentId());
      eventJson.put("acceptedStartTime", item.getItemEventDetails().getAcceptedStartTime());
      eventJson.put("competitorType", item.getItemEventDetails().getCompetitorType());
      eventJson.put("competitorsIds", item.getItemEventDetails().getCompetitorsIds());
      eventJson.put("type", item.getItemEventDetails().getType());
      jsonItem.put("event", eventJson);

      jsonObjectList.add(jsonItem);
    }
    return jsonObjectList;
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}

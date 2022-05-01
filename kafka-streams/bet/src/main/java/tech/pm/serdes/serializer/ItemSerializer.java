package tech.pm.serdes.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.json.JSONObject;
import org.json.JSONWriter;
import tech.pm.entities.raw.betDetails.Item;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ItemSerializer implements Serializer<Item> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Serializer.super.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(String topic, Item item) {
    if (item == null)
      return null;

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

    return JSONWriter.valueToString(jsonItem).getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    Serializer.super.close();
  }
}



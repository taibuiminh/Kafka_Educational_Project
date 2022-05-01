package tech.pm.streams.deprecated;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import tech.pm.entities.raw.Bet;
import tech.pm.entities.core.betAcceptItem.BetAcceptItem;
import tech.pm.entities.raw.Event;
import tech.pm.entities.raw.betDetails.Item;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;
import tech.pm.entities.raw.betDetails.ModifiedItem;
import tech.pm.serdes.CustomSerde;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Slf4j
public class BetAcceptItemOldStream {
  @Deprecated
  public static KStream<String, BetAcceptItem> createBetAcceptItemStream(KStream<String, Bet> betKTable,
                                                                         KTable<String, Event> eventKTable) {

    ValueJoiner<Item, Bet, ModifiedItem> modifiedItemValueJoiner = (item, bet) -> ModifiedItem.builder()
      .itemIndex(item.getItemIndex())
      .betId(bet.getBetId())
      .eventId(item.getEventId())
      .itemAcceptedAmount(item.getAmount())
      .itemAcceptedOdd(item.getAcceptedOdd())
      .build();

    KStream<String, Item> itemKTable = betKTable.flatMapValues(Bet::getItems);
    KStream<String, ModifiedItem> modifiedItemKStream = itemKTable
      .join(betKTable, modifiedItemValueJoiner, JoinWindows.of(Duration.ofSeconds(5)),
            StreamJoined.with(
              Serdes.String(),
              CustomSerde.Item(),
              CustomSerde.Bet()
            ))
      .selectKey((key, value) -> value.getEventId());

    modifiedItemKStream.peek((key, value) -> log.info("MODIFIED_ITEM: key " + key + " value " + value));

    ValueJoiner<ModifiedItem, Event, ItemJoinEvent> itemEventItemJoinEventValueJoiner = (modifiedItem, event) -> ItemJoinEvent.builder()
      .itemIndex(modifiedItem.getItemIndex())
      .betId(modifiedItem.getBetId())
      .itemAcceptedAmount(modifiedItem.getItemAcceptedAmount())
      .itemAcceptedOdd(modifiedItem.getItemAcceptedOdd())
      .eventId(event.getId())
      .eventName(event.getName())
      .startTime(event.getStart_time())
      .build();

    KStream<String, ItemJoinEvent> itemJoinEventKStream = modifiedItemKStream
      .join(eventKTable.toStream(), itemEventItemJoinEventValueJoiner,
            JoinWindows.of(Duration.ofSeconds(5)),
            StreamJoined.with(
              Serdes.String(),
              CustomSerde.ModifiedItem(),
              CustomSerde.Event()
            ))
      .selectKey((key, value) -> value.getBetId())
      .peek((key, value) -> log.info("ITEM+EVENT: key " + key + " value " + value));


    ValueMapper<ItemJoinEvent, List<ItemJoinEvent>> itemJoinEventListValueMapper = List::of;
    Reducer<List<ItemJoinEvent>> reducer = (itemJoinEvent1, itemJoinEvent2) -> Stream.concat(itemJoinEvent1.stream(), itemJoinEvent2.stream()).collect(Collectors.toList());
    KStream<String, List<ItemJoinEvent>> stringListKStream = itemJoinEventKStream
      .mapValues(itemJoinEventListValueMapper)
      .groupByKey(Grouped.with(Serdes.String(), CustomSerde.ItemJoinEventList()))
      .reduce(reducer, Materialized.with(Serdes.String(), CustomSerde.ItemJoinEventList()))
      .toStream()
      .peek((key, value) -> log.info("ITEM_JOIN_EVENT_AFTER_MAP: key " + key + " value " + value));


    ValueMapper<List<ItemJoinEvent>, BetAcceptItem> itemJoinEventBetItemValueMapper = itemJoinEventList ->
      BetAcceptItem.builder()
        .betItemId(itemJoinEventList.get(0).getBetId())
        .items(itemJoinEventList)
        .build();

    return stringListKStream.mapValues(itemJoinEventBetItemValueMapper)
      .filter((key, value) -> value.getItems().size() == 3)  //todo: remove this
      .peek((key, value) -> log.info("BET_ITEM: key " + key + " value " + value));

  }
}

package tech.pm.streams.betAcceptEntities;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import tech.pm.entities.raw.Bet;
import tech.pm.entities.core.betAcceptItem.BetAcceptItem;
import tech.pm.entities.raw.Event;
import tech.pm.entities.raw.betDetails.Item;
import tech.pm.entities.core.betAcceptItem.ItemJoinEvent;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class BetAcceptNewItemStream {

  private static final String eventStateStoreName = "event-state-store";

  public static KStream<String, BetAcceptItem> createBetAcceptItemStream(KStream<String, Bet> betKTable) {

    return betKTable.transform(new TransformerSupplier<>() {
      @Override
      public Transformer<String, Bet, KeyValue<String, BetAcceptItem>> get() {
        return new Transformer<>() {
          private KeyValueStore<String, Event> eventStore;

          @Override
          public void init(ProcessorContext processorContext) {
            this.eventStore = processorContext.getStateStore(eventStateStoreName);
          }

          @Override
          public KeyValue<String, BetAcceptItem> transform(String s, Bet bet) {
            List<ItemJoinEvent> itemJoinEventList = new ArrayList<>();
            List<Item> itemList = bet.getItems();
            for (Item item : itemList) {
              if (eventStore.get(item.getEventId()) != null) {
                ItemJoinEvent itemJoinEvent = ItemJoinEvent.builder()
                  .itemIndex(item.getItemIndex())
                  .betId(bet.getBetId())
                  .itemAcceptedAmount(item.getAmount())
                  .itemAcceptedOdd(item.getAcceptedOdd())
                  .eventId(eventStore.get(item.getEventId()).getId())
                  .eventName(eventStore.get(item.getEventId()).getName())
                  .startTime(eventStore.get(item.getEventId()).getStart_time())
                  .build();
                itemJoinEventList.add(itemJoinEvent);
              }
            }
            return new KeyValue<>(bet.getBetId(), BetAcceptItem.builder()
              .betItemId(bet.getBetId())
              .items(itemJoinEventList)
              .build());
          }

          @Override
          public void close() {
          }

        };
      }
    });
  }
}

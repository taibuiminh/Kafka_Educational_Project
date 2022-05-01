package tech.pm.streams.event;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import tech.pm.entities.raw.Event;
import tech.pm.serdes.CustomSerde;

@Slf4j
public class EventStore {

  public static void createEventStore(String topic, StreamsBuilder streamsBuilder) {
    StoreBuilder<KeyValueStore<String, Event>> storeBuilder =
      Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("event-state-store"), Serdes.String(), CustomSerde.Event());
    streamsBuilder.addGlobalStore(storeBuilder, topic, Consumed.with(Serdes.String(), CustomSerde.Event()), new EventProcessorSupplier());
  }
}

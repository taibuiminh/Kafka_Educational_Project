package tech.pm.streams.deprecated;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import tech.pm.entities.raw.Event;
import tech.pm.serdes.CustomSerde;

@Slf4j
public class EventOldTable {

  @Deprecated
  public static KTable<String, Event> createEventTable(String topic, StreamsBuilder streamsBuilder) {
    KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore("event-store-name");
    KStream<String, Event> eventKStream = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), CustomSerde.Event()))
      .filter((key, value) -> value != null)
      .selectKey((key, value) -> value.getId());
    return eventKStream.toTable(Materialized.<String, Event>as(storeSupplier).withKeySerde(Serdes.String())
                                  .withValueSerde(CustomSerde.Event())
                                  .withCachingDisabled());
  }
}

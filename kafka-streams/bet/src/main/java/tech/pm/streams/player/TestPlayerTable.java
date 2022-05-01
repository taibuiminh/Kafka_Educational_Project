package tech.pm.streams.player;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import tech.pm.entities.raw.TestPlayer;
import tech.pm.serdes.CustomSerde;

@Slf4j
public class TestPlayerTable {

  public static KTable<String, TestPlayer> createTestPlayerTable(String topic, StreamsBuilder streamsBuilder) {
    KStream<String, TestPlayer> testPlayerKStream = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), CustomSerde.TestPlayer()))
      .filter((key, value) -> value != null)
      .filter((key, value) -> value.getId() != null)
      .filter((key, value) -> !value.isTestPlayer())
      .selectKey((key, value) -> value.getId());
    return testPlayerKStream.toTable(Materialized.with(Serdes.String(), CustomSerde.TestPlayer()));

  }
}

package tech.pm.streams.player;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import tech.pm.entities.raw.Email;
import tech.pm.serdes.CustomSerde;

public class EmailTable {

  public static KTable<String, Email> createEmailTable(String topic, StreamsBuilder streamsBuilder) {
    KStream<String, Email> emailKStream = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), CustomSerde.Email()))
      .filter((key, value) -> value != null)
      .filter((key, value) -> value.getPlayerId() != null)
      .selectKey((key, value) -> value.getPlayerId());
    return emailKStream.toTable(Materialized.with(Serdes.String(), CustomSerde.Email()));

  }
}

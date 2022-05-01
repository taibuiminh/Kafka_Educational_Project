package tech.pm.streams.player;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import tech.pm.entities.raw.PlayerProfile;
import tech.pm.serdes.CustomSerde;

import java.util.List;

public class PlayerProfileTable {

  public static KTable<String, PlayerProfile> createPlayerProfileTable(String topic, StreamsBuilder builder) {
    KStream<String, PlayerProfile> playerKStream = builder.stream(topic, Consumed.with(Serdes.String(), CustomSerde.PlayerProfile()))
      .filter((key, value) -> value.getEmail() != null)
      .filter((key, value) -> value.getPlayerId() != null)
      .filter((key, value) -> List.of("UAH", "USD", "EUR").contains(value.getDefaultCurrency()))
      .filter((key, value) -> value.getBrand().equals("COM"))
      .selectKey((key, value) -> value.getPlayerId());
    return playerKStream.toTable(Materialized.with(Serdes.String(), CustomSerde.PlayerProfile()));
  }
}


package tech.pm.streams.bet;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import tech.pm.entities.raw.Bet;
import tech.pm.serdes.CustomSerde;

public class BetStream {

  public static KStream<String, Bet> createBetStream(String topic, StreamsBuilder streamsBuilder) {
    return streamsBuilder.stream(topic, Consumed.with(Serdes.String(), CustomSerde.Bet()))
      .filter((key, value) -> value != null)
      .filter((key, value) -> value.getBetId() != null)
      .selectKey((key, value) -> value.getBetId());

  }
}

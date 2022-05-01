package tech.pm.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import tech.pm.entities.core.betAcceptItem.BetAcceptItem;
import tech.pm.entities.core.betAcceptPlayer.BetAcceptPlayer;
import tech.pm.entities.core.BetAccepted;
import tech.pm.serdes.CustomSerde;

import java.time.Duration;


@Slf4j
public class BetAcceptedStream {
  public static KStream<String, BetAccepted> createBetAcceptedStream(KStream<String, BetAcceptPlayer> betAcceptPlayerKStream,
                                                                     KStream<String, BetAcceptItem> betItemKStream) {

    ValueJoiner<BetAcceptPlayer, BetAcceptItem, BetAccepted> joiner = (betAcceptPlayer, betItem) -> BetAccepted.builder()
      .betId(betAcceptPlayer.getBetAcceptPlayerId())
      .betTimestamp(betAcceptPlayer.getBetTimestamp())
      .currency(betAcceptPlayer.getCurrency())
      .amount(betAcceptPlayer.getAmount())
      .acceptedBetOdd(betAcceptPlayer.getAcceptedBetOdd())
      .possiblePayout(betAcceptPlayer.getPossiblePayout())
      .playerId(betAcceptPlayer.getPlayerId())
      .firstName(betAcceptPlayer.getFirstName())
      .lastName(betAcceptPlayer.getLastName())
      .email(betAcceptPlayer.getEmail())
      .isEmailVerified(betAcceptPlayer.isEmailVerified())
      .defaultCurrency(betAcceptPlayer.getDefaultCurrency())
      .brand(betAcceptPlayer.getBrand())
      .itemsList(betItem.getItems())
      .build();


    return betAcceptPlayerKStream.join(betItemKStream, joiner, JoinWindows.of(Duration.ofSeconds(5)),
                                       StreamJoined.with(
                                         Serdes.String(),
                                         CustomSerde.BetAcceptPlayer(),
                                         CustomSerde.BetAcceptItem()))
      .peek((key, value) -> log.info("BET FINALLY WAS CREATED: Key {} Value {}", key, value));
  }

}

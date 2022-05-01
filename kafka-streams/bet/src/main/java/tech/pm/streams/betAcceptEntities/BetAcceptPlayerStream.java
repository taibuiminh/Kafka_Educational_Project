package tech.pm.streams.betAcceptEntities;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import tech.pm.entities.raw.Bet;
import tech.pm.entities.core.betAcceptPlayer.BetAcceptPlayer;
import tech.pm.entities.core.betAcceptPlayer.Player;


@Slf4j
public class BetAcceptPlayerStream {
  public static KStream<String, BetAcceptPlayer> createBetAcceptPlayerStream(KStream<String, Bet> betKTable,
                                                                             KTable<String, Player> playerKTable) {

    ValueJoiner<Bet, Player, BetAcceptPlayer> joiner = (bet, player) -> BetAcceptPlayer.builder()
      .betAcceptPlayerId(bet.getBetId())
      .betTimestamp(bet.getAcceptTime())
      .currency(bet.getCurrencyId())
      .amount(bet.getAmount())
      .acceptedBetOdd(bet.getAcceptedBetOdd())
      .possiblePayout(bet.getAmount() * bet.getAcceptedBetOdd())
      .playerId(player.getPlayerId())
      .firstName(player.getFirstName())
      .lastName(player.getLastName())
      .email(player.getEmail())
      .isEmailVerified(player.getIsEmailVerified())
      .defaultCurrency(player.getDefaultCurrency())
      .brand(player.getBrand())
      .build();


    return betKTable
      .selectKey((key, value) -> value.getPlayerId())
      .join(playerKTable, joiner)
      .selectKey((key, value) -> value.getBetAcceptPlayerId())
      .peek((key, value) -> log.info("Bet with player after join was created: Key {} Value {}", key, value));
  }

}

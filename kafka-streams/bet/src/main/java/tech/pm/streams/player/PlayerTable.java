package tech.pm.streams.player;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import tech.pm.entities.core.betAcceptPlayer.Player;
import tech.pm.entities.core.betAcceptPlayer.PlayerEmailVerified;
import tech.pm.entities.raw.TestPlayer;

@Slf4j
public class PlayerTable {
  public static KTable<String, Player> createPlayerTable(KTable<String, PlayerEmailVerified> playerEmailVerifiedKTable,
                                                         KTable<String, TestPlayer> testPlayerTableKTable) {
    ValueJoiner<PlayerEmailVerified, TestPlayer, Player> joiner = (playerEmailVerified, testPlayer) -> Player.builder()
      .playerId(playerEmailVerified.getPlayerId())
      .firstName(playerEmailVerified.getFirstName())
      .lastName(playerEmailVerified.getLastName())
      .email(playerEmailVerified.getEmail())
      .isEmailVerified(playerEmailVerified.getIsEmailVerified())
      .defaultCurrency(playerEmailVerified.getDefaultCurrency())
      .brand(playerEmailVerified.getBrand())
      .testPlayer(testPlayer.isTestPlayer())
      .build();

    KTable<String, Player> playerKTable = playerEmailVerifiedKTable.join(testPlayerTableKTable, joiner);
    playerKTable
      .toStream()
      .peek((key, value) -> log.info("Player was created: Key {} Value {}", key, value));
    return playerKTable;
  }
}




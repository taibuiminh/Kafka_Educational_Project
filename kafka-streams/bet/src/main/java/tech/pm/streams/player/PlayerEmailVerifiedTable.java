package tech.pm.streams.player;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import tech.pm.entities.raw.Email;
import tech.pm.entities.core.betAcceptPlayer.PlayerEmailVerified;
import tech.pm.entities.raw.PlayerProfile;

@Slf4j
public class PlayerEmailVerifiedTable {
  public static KTable<String, PlayerEmailVerified> createPlayerEmailVerifiedTable(KTable<String, PlayerProfile> playerProfileTable,
                                                                                   KTable<String, Email> emailVerifiedTable) {
    ValueJoiner<PlayerProfile, Email, PlayerEmailVerified> joiner = (playerProfile, emailVerified) -> PlayerEmailVerified.builder()
      .playerId(playerProfile.getPlayerId())
      .firstName(playerProfile.getFirstName())
      .lastName(playerProfile.getLastName())
      .email(playerProfile.getEmail())
      .isEmailVerified(emailVerified.isVerified())
      .defaultCurrency(playerProfile.getDefaultCurrency())
      .brand(playerProfile.getBrand())
      .build();

    KTable<String, PlayerEmailVerified> playerKTable = playerProfileTable.join(emailVerifiedTable, joiner);
    playerKTable
      .toStream()
      .peek((key, value) -> log.info("Player Profile was joined with Email Data: Key {} Value {}", key, value));
    return playerKTable;
  }
}




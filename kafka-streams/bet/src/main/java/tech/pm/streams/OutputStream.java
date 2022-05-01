package tech.pm.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import tech.pm.entities.core.BetAccepted;
import tech.pm.entities.core.betAcceptItem.BetAcceptItem;
import tech.pm.entities.core.betAcceptPlayer.BetAcceptPlayer;
import tech.pm.entities.core.betAcceptPlayer.Player;
import tech.pm.entities.core.betAcceptPlayer.PlayerEmailVerified;
import tech.pm.entities.raw.Bet;
import tech.pm.entities.raw.Email;
import tech.pm.entities.raw.PlayerProfile;
import tech.pm.entities.raw.TestPlayer;
import tech.pm.serdes.CustomSerde;
import tech.pm.streams.bet.BetStream;
import tech.pm.streams.betAcceptEntities.BetAcceptNewItemStream;
import tech.pm.streams.betAcceptEntities.BetAcceptPlayerStream;
import tech.pm.streams.event.EventStore;
import tech.pm.streams.player.EmailTable;
import tech.pm.streams.player.PlayerEmailVerifiedTable;
import tech.pm.streams.player.PlayerProfileTable;
import tech.pm.streams.player.PlayerTable;
import tech.pm.streams.player.TestPlayerTable;

import java.util.Properties;

@Slf4j
public class OutputStream {

  private final static String playerTopic = "event.player.profile-STAGE";
  private final static String eventTopic = "TradingFeed_Event-STAGE";
  private final static String betAcceptedTopic = "bet.accepted-STAGE";
  private final static String emailTopic = "event.channel-verification.email-STAGE";
  private final static String testPlayerTopic = "event.player.testplayer-STAGE";
  private final static String outputTopic = "dwh.interns.streams.bet-Test-2";
  private final static StreamsBuilder streamsBuilder = new StreamsBuilder();

  public static void main(String[] args) {
    Properties streamsProperties = new Properties();
    streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "dwh.interns.streams.bet-8");//consumer group which will be created
    streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.30.173:9092");
    streamsProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    streamsProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


    log.info("Start of Application");

    betAccepted();

    KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), streamsProperties);
    log.info("Start of the Kafka streams");
    kafkaStreams.start();
  }


  public static void betAccepted() {
    EventStore.createEventStore(eventTopic, streamsBuilder);

    KTable<String, PlayerProfile> playerProfileTable = PlayerProfileTable.createPlayerProfileTable(playerTopic, streamsBuilder);

    KTable<String, Email> emailVerifiedTable = EmailTable.createEmailTable(emailTopic, streamsBuilder);

    KTable<String, TestPlayer> testPlayerKTable = TestPlayerTable.createTestPlayerTable(testPlayerTopic, streamsBuilder);

    KTable<String, PlayerEmailVerified> playerEmailVerifiedTable = PlayerEmailVerifiedTable.createPlayerEmailVerifiedTable(playerProfileTable, emailVerifiedTable);

    KTable<String, Player> playerKTable = PlayerTable.createPlayerTable(playerEmailVerifiedTable, testPlayerKTable);

    KStream<String, Bet> betKTable = BetStream.createBetStream(betAcceptedTopic, streamsBuilder);

    KStream<String, BetAcceptItem> betItemKStream = BetAcceptNewItemStream.createBetAcceptItemStream(betKTable);

    KStream<String, BetAcceptPlayer> betAcceptPlayerKTable = BetAcceptPlayerStream.createBetAcceptPlayerStream(betKTable, playerKTable);

    KStream<String, BetAccepted> betAcceptedKStream = BetAcceptedStream.createBetAcceptedStream(betAcceptPlayerKTable, betItemKStream);

    betAcceptedKStream.to(outputTopic, Produced.with(Serdes.String(), CustomSerde.BetAccepted()));

  }


}

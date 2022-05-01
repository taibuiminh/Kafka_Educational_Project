package tech.pm.streams;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tech.pm.entities.raw.Bet;
import tech.pm.entities.core.betAcceptPlayer.BetAcceptPlayer;
import tech.pm.entities.raw.Email;
import tech.pm.entities.core.betAcceptPlayer.Player;
import tech.pm.entities.core.betAcceptPlayer.PlayerEmailVerified;
import tech.pm.entities.raw.PlayerProfile;
import tech.pm.entities.raw.TestPlayer;
import tech.pm.serdes.CustomSerde;
import tech.pm.serdes.deserializer.BetAcceptPlayerDeserializer;
import tech.pm.streams.bet.BetStream;
import tech.pm.streams.betAcceptEntities.BetAcceptPlayerStream;
import tech.pm.streams.player.EmailTable;
import tech.pm.streams.player.PlayerEmailVerifiedTable;
import tech.pm.streams.player.PlayerProfileTable;
import tech.pm.streams.player.PlayerTable;
import tech.pm.streams.player.TestPlayerTable;
import tech.pm.updator.JsonUpdater;
import tech.pm.updator.impl.BetAcceptPlayerJsonUpdater;
import tech.pm.updator.impl.BetJsonUpdater;
import tech.pm.updator.impl.EmailJsonUpdater;
import tech.pm.updator.impl.PlayerProfileJsonUpdater;
import tech.pm.updator.impl.TestPlayerJsonUpdater;
import tech.pm.utils.JsonUtils;
import tech.pm.utils.TopicUtils;
import tech.pm.utils.record.TestRecordUtils;
import tech.pm.utils.record.impl.BetAcceptPlayerRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class BetAcceptPlayerStreamTest {

  private static final Logger log = LoggerFactory.getLogger(BetAcceptPlayerStream.class);

  @Container
  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

  private final Properties consumerApiProperties = new Properties();
  private final Properties producerApiProperties = new Properties();
  private final Properties adminApiProperties = new Properties();
  private final Properties streamsApiProperties = new Properties();

  private final String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();

  private final Serde<String> keySerdes = Serdes.String();
  private final Serde<String> valueSerdes = Serdes.String();

  private final String inputPlayerProfileTopic = "input-player-profile-table-test";
  private final String inputEmailTopic = "input-email-verified-table-test";
  private final String inputTestPlayerTopic = "input-test-player-table-test";
  private final String inputBetTopic = "input-bet-table-test";
  private final String outputTopic = "output-bet-accept-player-join-test";

  private List<KeyValue<String, String>> playerProfileTestRecords;
  private List<KeyValue<String, String>> emailTestRecords;
  private List<KeyValue<String, String>> testPlayerTestRecords;
  private List<KeyValue<String, String>> betTestRecords;

  private JSONObject outputJson;


  private List<KeyValue<String, BetAcceptPlayer>> expectedRecords;


  private TestRecordUtils<BetAcceptPlayer> betAcceptPlayerRecordUtils;


  private JsonUpdater betAcceptPlayerUpdater;

  @BeforeEach
  void setUp() {
    log.info("Bootstrap servers: {}", bootstrapServers);

    consumerApiProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerApiProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "bet-player-join-test-group");
    consumerApiProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerApiProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BetAcceptPlayerDeserializer.class.getName());
    consumerApiProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    adminApiProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    producerApiProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerApiProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerApiProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    streamsApiProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsApiProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "dwh-interns-bet-player-join-stream-test-2");
    streamsApiProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerdes.getClass().getName());
    streamsApiProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerdes.getClass().getName());
    streamsApiProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    TopicUtils.createTopic(inputPlayerProfileTopic, adminApiProperties);
    TopicUtils.createTopic(inputEmailTopic, adminApiProperties);
    TopicUtils.createTopic(inputBetTopic, adminApiProperties);
    TopicUtils.createTopic(inputTestPlayerTopic, adminApiProperties);
    TopicUtils.createTopic(outputTopic, adminApiProperties);


    betAcceptPlayerRecordUtils = new BetAcceptPlayerRecordUtils();


    betAcceptPlayerUpdater = new BetAcceptPlayerJsonUpdater();

    JsonUpdater emailVerifiedUpdater = new EmailJsonUpdater();
    JsonUpdater playerProfileUpdater = new PlayerProfileJsonUpdater();
    JsonUpdater testPlayerJsonUpdater = new TestPlayerJsonUpdater();
    JsonUpdater betUpdater = new BetJsonUpdater();

    JSONObject playerProfileJson = JsonUtils.getJson("src/test/resources/player-profile/working-template.json");

    JSONObject emailJson = JsonUtils.getJson("src/test/resources/email/working-template.json");

    JSONObject testPlayerWorkingJson = JsonUtils.getJson("src/test/resources/test-player/working-template.json");

    JSONObject betJson = JsonUtils.getJson("src/test/resources/bet/working-template.json");

    outputJson = JsonUtils.getJson("src/test/resources/bet-accept-player/output-template.json");


    playerProfileTestRecords = new ArrayList<>();
    playerProfileTestRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(0, 100, playerProfileJson, playerProfileUpdater));
    Collections.shuffle(playerProfileTestRecords);

    emailTestRecords = new ArrayList<>();
    emailTestRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(0, 100, emailJson, emailVerifiedUpdater));
    Collections.shuffle(emailTestRecords);

    testPlayerTestRecords = new ArrayList<>();
    testPlayerTestRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(0, 100, testPlayerWorkingJson, testPlayerJsonUpdater));
    Collections.shuffle(testPlayerTestRecords);

    betTestRecords = new ArrayList<>();
    betTestRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(0, 100, betJson, betUpdater));
    Collections.shuffle(betTestRecords);


    expectedRecords = new ArrayList<>();


    betAcceptPlayerRecordUtils = new BetAcceptPlayerRecordUtils();
  }

  @Test
  void processBetAcceptPlayerJoin() throws Exception {
    expectedRecords.addAll(betAcceptPlayerRecordUtils.generateExpectedKeyValueRecords(0, 100, outputJson, betAcceptPlayerUpdater));

    StreamsBuilder builder = new StreamsBuilder();

    KTable<String, PlayerProfile> playerProfileTable = PlayerProfileTable.createPlayerProfileTable(inputPlayerProfileTopic, builder);

    KTable<String, Email> emailVerifiedTable = EmailTable.createEmailTable(inputEmailTopic, builder);

    KTable<String, TestPlayer> testPlayerKTable = TestPlayerTable.createTestPlayerTable(inputTestPlayerTopic, builder);

    KStream<String, Bet> betKTable = BetStream.createBetStream(inputBetTopic, builder);

    KTable<String, PlayerEmailVerified> playerEmailVerifiedTable = PlayerEmailVerifiedTable.createPlayerEmailVerifiedTable(playerProfileTable, emailVerifiedTable);

    KTable<String, Player> playerKTable = PlayerTable.createPlayerTable(playerEmailVerifiedTable, testPlayerKTable);

    KStream<String, BetAcceptPlayer> betAcceptPlayerKStream = BetAcceptPlayerStream.createBetAcceptPlayerStream(betKTable, playerKTable);

    betAcceptPlayerKStream.to(outputTopic, Produced.with(Serdes.String(), CustomSerde.BetAcceptPlayer()));

    Topology topology = builder.build();

    log.info(String.valueOf(topology.describe()));

    try (KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsApiProperties)) {
      kafkaStreams.start();

      IntegrationTestUtils.produceKeyValuesSynchronously(inputPlayerProfileTopic, playerProfileTestRecords, producerApiProperties, Time.SYSTEM);
      IntegrationTestUtils.produceKeyValuesSynchronously(inputEmailTopic, emailTestRecords, producerApiProperties, Time.SYSTEM);
      IntegrationTestUtils.produceKeyValuesSynchronously(inputTestPlayerTopic, testPlayerTestRecords, producerApiProperties, Time.SYSTEM);
      Thread.sleep(15000);
      IntegrationTestUtils.produceKeyValuesSynchronously(inputBetTopic, betTestRecords, producerApiProperties, Time.SYSTEM);

      List<KeyValue<String, BetAcceptPlayer>> actualRecords = IntegrationTestUtils
        .waitUntilMinKeyValueRecordsReceived(consumerApiProperties, outputTopic, 100);

      assertThat(actualRecords).hasSameElementsAs(expectedRecords);
      kafkaStreams.close();
      kafkaStreams.cleanUp();
    }
  }
}

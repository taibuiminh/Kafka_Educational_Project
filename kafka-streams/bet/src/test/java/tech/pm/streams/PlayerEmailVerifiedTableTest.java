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
import tech.pm.entities.raw.Email;
import tech.pm.entities.core.betAcceptPlayer.PlayerEmailVerified;
import tech.pm.entities.raw.PlayerProfile;
import tech.pm.serdes.CustomSerde;
import tech.pm.serdes.deserializer.PlayerEmailVerifiedDeserializer;
import tech.pm.streams.player.EmailTable;
import tech.pm.streams.player.PlayerEmailVerifiedTable;
import tech.pm.streams.player.PlayerProfileTable;
import tech.pm.updator.JsonUpdater;
import tech.pm.updator.impl.EmailJsonUpdater;
import tech.pm.updator.impl.PlayerEmailVerifiedJsonUpdater;
import tech.pm.updator.impl.PlayerProfileJsonUpdater;
import tech.pm.utils.JsonUtils;
import tech.pm.utils.TopicUtils;
import tech.pm.utils.record.TestRecordUtils;
import tech.pm.utils.record.impl.PlayerEmailVerifiedRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class PlayerEmailVerifiedTableTest {

  private static final Logger log = LoggerFactory.getLogger(PlayerEmailVerifiedTableTest.class);

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
  private final String inputEmailVerifiedTopic = "input-email-verified-table-test";
  private final String outputTopic = "output-player-join-test";

  private List<KeyValue<String, String>> playerProfileTestRecords;
  private List<KeyValue<String, String>> emailVerifiedTestRecords;

  private JSONObject outputJson;
  private JSONObject optionalOutputJson;

  private List<KeyValue<String, PlayerEmailVerified>> expectedRecords;

  private TestRecordUtils<PlayerEmailVerified> playerEmailVerifiedTestRecordUtils;

  private JsonUpdater playerEmailVerifiedUpdater;

  @BeforeEach
  void setUp() {
    log.info("Bootstrap servers: {}", bootstrapServers);

    consumerApiProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerApiProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "player-join-test-group");
    consumerApiProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerApiProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PlayerEmailVerifiedDeserializer.class.getName());
    consumerApiProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    adminApiProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    producerApiProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerApiProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerApiProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    streamsApiProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsApiProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "dwh-interns-bet-stream-test");
    streamsApiProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerdes.getClass().getName());
    streamsApiProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerdes.getClass().getName());
    streamsApiProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    TopicUtils.createTopic(inputPlayerProfileTopic, adminApiProperties);
    TopicUtils.createTopic(inputEmailVerifiedTopic, adminApiProperties);
    TopicUtils.createTopic(outputTopic, adminApiProperties);

    playerEmailVerifiedTestRecordUtils = new PlayerEmailVerifiedRecordUtils();

    playerEmailVerifiedUpdater = new PlayerEmailVerifiedJsonUpdater();

    JsonUpdater emailVerifiedUpdater = new EmailJsonUpdater();
    JsonUpdater playerProfileJsonUpdater = new PlayerProfileJsonUpdater();

    JSONObject playerProfileWorkingJson = JsonUtils.getJson("src/test/resources/player-profile/working-template.json");
    JSONObject playerProfileOptionalWorkingJson = JsonUtils.getJson("src/test/resources/player-profile/optional-working-template.json");
    JSONObject playerProfileNotWorkingJson = JsonUtils.getJson("src/test/resources/player-profile/not-working-template.json");

    JSONObject emailVerifiedWorkingJson = JsonUtils.getJson("src/test/resources/email/working-template.json");
    JSONObject emailVerifiedNotWorkingJson = JsonUtils.getJson("src/test/resources/email/not-working-template.json");

    outputJson = JsonUtils.getJson("src/test/resources/player-email-verified/output-template.json");
    optionalOutputJson = JsonUtils.getJson("src/test/resources/player-email-verified/output-optional-template.json");

    playerProfileTestRecords = new ArrayList<>();

    playerProfileTestRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(0, 25, playerProfileWorkingJson, playerProfileJsonUpdater));
    playerProfileTestRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(25, 50, playerProfileOptionalWorkingJson, playerProfileJsonUpdater));
    playerProfileTestRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(50, 100, playerProfileNotWorkingJson, playerProfileJsonUpdater));

    Collections.shuffle(playerProfileTestRecords);

    emailVerifiedTestRecords = new ArrayList<>();

    emailVerifiedTestRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(0, 50, emailVerifiedWorkingJson, emailVerifiedUpdater));
    emailVerifiedTestRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(50, 100, emailVerifiedNotWorkingJson, emailVerifiedUpdater));

    Collections.shuffle(emailVerifiedTestRecords);

    expectedRecords = new ArrayList<>();

    playerEmailVerifiedTestRecordUtils = new PlayerEmailVerifiedRecordUtils();
  }

  @Test
  void processPlayerEmailVerifiedJoin() throws Exception {
    expectedRecords.addAll(playerEmailVerifiedTestRecordUtils.generateExpectedKeyValueRecords(0, 25, outputJson, playerEmailVerifiedUpdater));
    expectedRecords.addAll(playerEmailVerifiedTestRecordUtils.generateExpectedKeyValueRecords(25, 50, optionalOutputJson, playerEmailVerifiedUpdater));

    StreamsBuilder builder = new StreamsBuilder();

    KTable<String, PlayerProfile> playerProfileTable = PlayerProfileTable.createPlayerProfileTable(inputPlayerProfileTopic, builder);

    KTable<String, Email> emailVerifiedTable = EmailTable.createEmailTable(inputEmailVerifiedTopic, builder);

    KTable<String, PlayerEmailVerified> playerTable = PlayerEmailVerifiedTable.createPlayerEmailVerifiedTable(playerProfileTable, emailVerifiedTable);

    playerTable.toStream().to(outputTopic, Produced.with(Serdes.String(), CustomSerde.PlayerEmailVerified()));

    Topology topology = builder.build();

    log.info(String.valueOf(topology.describe()));

    try (KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsApiProperties)) {
      kafkaStreams.start();

      IntegrationTestUtils.produceKeyValuesSynchronously(inputPlayerProfileTopic, playerProfileTestRecords, producerApiProperties, Time.SYSTEM);
      IntegrationTestUtils.produceKeyValuesSynchronously(inputEmailVerifiedTopic, emailVerifiedTestRecords, producerApiProperties, Time.SYSTEM);

      List<KeyValue<String, PlayerEmailVerified>> actualRecords = IntegrationTestUtils
        .waitUntilMinKeyValueRecordsReceived(consumerApiProperties, outputTopic, 50);

      assertThat(actualRecords).hasSameElementsAs(expectedRecords);
      kafkaStreams.close();
      kafkaStreams.cleanUp();
    }
  }
}

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
import tech.pm.entities.raw.PlayerProfile;
import tech.pm.serdes.CustomSerde;
import tech.pm.serdes.deserializer.PlayerProfileDeserializer;
import tech.pm.streams.player.PlayerProfileTable;
import tech.pm.updator.JsonUpdater;
import tech.pm.updator.impl.PlayerProfileJsonUpdater;
import tech.pm.utils.JsonUtils;
import tech.pm.utils.TopicUtils;
import tech.pm.utils.record.TestRecordUtils;
import tech.pm.utils.record.impl.PlayerProfileRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class PlayerProfileTableTest {

  private static final Logger log = LoggerFactory.getLogger(PlayerProfileTableTest.class);

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

  private final String inputTopic = "input-player-profile-table-test";
  private final String outputTopic = "output-player-profile-table-test";

  private List<KeyValue<String, String>> testRecords;

  private JSONObject outputJson;
  private JSONObject outputOptionalJson;

  private List<KeyValue<String, PlayerProfile>> expectedRecords;

  private TestRecordUtils<PlayerProfile> playerProfileRecordUtils;

  private JsonUpdater updater;

  @BeforeEach
  void setUp() {
    log.info("Bootstrap servers: {}", bootstrapServers);

    consumerApiProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerApiProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "player-profile-table-test-group");
    consumerApiProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerApiProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PlayerProfileDeserializer.class.getName());
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

    TopicUtils.createTopic(inputTopic, adminApiProperties);
    TopicUtils.createTopic(outputTopic, adminApiProperties);

    JSONObject workingJson = JsonUtils.getJson("src/test/resources/player-profile/working-template.json");
    JSONObject optionalWorkingJson = JsonUtils.getJson("src/test/resources/player-profile/optional-working-template.json");
    JSONObject notWorkingJson = JsonUtils.getJson("src/test/resources/player-profile/not-working-template.json");

    outputJson = JsonUtils.getJson("src/test/resources/player-profile/output-template.json");
    outputOptionalJson = JsonUtils.getJson("src/test/resources/player-profile/output-optional-template.json");

    updater = new PlayerProfileJsonUpdater();

    testRecords = new ArrayList<>();

    testRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(0, 50, workingJson, updater));
    testRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(50, 75, optionalWorkingJson, updater));
    testRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(75, 100, notWorkingJson, updater));

    Collections.shuffle(testRecords);

    expectedRecords = new ArrayList<>();

    playerProfileRecordUtils = new PlayerProfileRecordUtils();
  }

  @Test
  void processPlayerProfileTable() throws Exception {
    expectedRecords.addAll(playerProfileRecordUtils.generateExpectedKeyValueRecords(0, 50, outputJson, updater));
    expectedRecords.addAll(playerProfileRecordUtils.generateExpectedKeyValueRecords(50, 75, outputOptionalJson, updater));

    StreamsBuilder builder = new StreamsBuilder();

    KTable<String, PlayerProfile> playerProfileTable = PlayerProfileTable.createPlayerProfileTable(inputTopic, builder);

    playerProfileTable.toStream().to(outputTopic, Produced.with(Serdes.String(), CustomSerde.PlayerProfile()));

    Topology topology = builder.build();

    log.info(String.valueOf(topology.describe()));

    try (KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsApiProperties)) {
      kafkaStreams.start();


      IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, testRecords, producerApiProperties, Time.SYSTEM);

      List<KeyValue<String, PlayerProfile>> actualRecords = IntegrationTestUtils
        .waitUntilMinKeyValueRecordsReceived(consumerApiProperties, outputTopic, 75);

      assertThat(actualRecords).hasSameSizeAs(expectedRecords).hasSameElementsAs(expectedRecords);
      kafkaStreams.close();
      kafkaStreams.cleanUp();
    }
  }
}

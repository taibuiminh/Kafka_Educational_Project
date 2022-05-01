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
import tech.pm.serdes.CustomSerde;
import tech.pm.serdes.deserializer.EmailDeserializer;
import tech.pm.streams.player.EmailTable;
import tech.pm.updator.JsonUpdater;
import tech.pm.updator.impl.EmailJsonUpdater;
import tech.pm.utils.JsonUtils;
import tech.pm.utils.TopicUtils;
import tech.pm.utils.record.TestRecordUtils;
import tech.pm.utils.record.impl.EmailRecordUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class EmailTableTest {

  private static final Logger log = LoggerFactory.getLogger(EmailTableTest.class);

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

  private final String inputTopic = "input-email-verified-table-test";
  private final String outputTopic = "output-email-verified-table-test";

  private List<KeyValue<String, String>> testRecords;

  private JsonUpdater updater;

  private JSONObject outputJson;

  private List<KeyValue<String, Email>> expectedRecords;

  private TestRecordUtils<Email> emailVerifiedRecordUtils;

  @BeforeEach
  void setUp() {
    log.info("Bootstrap servers: {}", bootstrapServers);

    consumerApiProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerApiProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "email-table-test-group");
    consumerApiProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerApiProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EmailDeserializer.class.getName());
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

    JSONObject workingJson = JsonUtils.getJson("src/test/resources/email/working-template.json");
    JSONObject notWorkingJson = JsonUtils.getJson("src/test/resources/email/not-working-template.json");

    outputJson = JsonUtils.getJson("src/test/resources/email/output-template.json");

    updater = new EmailJsonUpdater();

    testRecords = new ArrayList<>();
    testRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(0, 50, workingJson, updater));
    testRecords.addAll(TestRecordUtils.generateTestKeyValueRecords(50, 75, notWorkingJson, updater));

    Collections.shuffle(testRecords);

    expectedRecords = new ArrayList<>();

    emailVerifiedRecordUtils = new EmailRecordUtils();
  }

  @Test
  void processEmailTable() throws Exception {
    expectedRecords.addAll(emailVerifiedRecordUtils.generateExpectedKeyValueRecords(0, 50, outputJson, updater));

    StreamsBuilder builder = new StreamsBuilder();

    KTable<String, Email> emailVerifiedTable = EmailTable.createEmailTable(inputTopic, builder);

    emailVerifiedTable.toStream().to(outputTopic, Produced.with(Serdes.String(), CustomSerde.Email()));

    Topology topology = builder.build();

    log.info(String.valueOf(topology.describe()));

    try (KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsApiProperties)) {
      kafkaStreams.start();

      IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, testRecords, producerApiProperties, Time.SYSTEM);

      List<KeyValue<String, Email>> actualRecords = IntegrationTestUtils
        .waitUntilMinKeyValueRecordsReceived(consumerApiProperties, outputTopic, 50);

      assertThat(actualRecords).hasSameSizeAs(expectedRecords).hasSameElementsAs(expectedRecords);
      kafkaStreams.close();
      kafkaStreams.cleanUp();
    }
  }
}

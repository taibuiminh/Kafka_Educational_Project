package tech.pm.kafka.service.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.pm.kafka.controller.ConsumerApiController;
import tech.pm.kafka.exceptions.admin.ConsumerGroupNotCreateException;
import tech.pm.kafka.exceptions.admin.ConsumerGroupNotFound;
import tech.pm.kafka.exceptions.admin.OffsetNotFoundException;
import tech.pm.kafka.exceptions.admin.OffsetsAndMetadataNotFoundException;
import tech.pm.kafka.exceptions.topic.InvalidConfigFileException;
import tech.pm.kafka.exceptions.topic.InvalidPartitionsValueException;
import tech.pm.kafka.exceptions.topic.InvalidReplicationFactorValueException;
import tech.pm.kafka.property.ServiceProperty;
import tech.pm.kafka.service.consumer.ConsumerApiService;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AdminApiService {

  private static final Logger log = LoggerFactory.getLogger(AdminApiService.class);

  private static final int timeout = 15;
  private static final TimeUnit timeUnit = TimeUnit.SECONDS;

  public static void createTopic(Properties apiProperties, Map<ServiceProperty, String> serviceProperties) {
    try (Admin admin = Admin.create(apiProperties)) {
      validateServiceProperties(serviceProperties);

      String topicName = serviceProperties.get(ServiceProperty.TOPIC_NAME);
      String configFile = serviceProperties.get(ServiceProperty.CONFIG_FILE);

      NewTopic newTopic = new NewTopic(topicName, Integer.parseInt(serviceProperties.get(ServiceProperty.PARTITIONS)), Short.parseShort(serviceProperties.get(ServiceProperty.REPLICATION_FACTOR)));

      CreateTopicsResult result;

      if (configFile != null) {
        newTopic.configs(createTopicConfigs(configFile));

        result = admin.createTopics(Collections.singleton(newTopic), createTopicsOptions(configFile));
      } else {
        result = admin.createTopics(Collections.singleton(newTopic));
      }

      KafkaFuture<Void> future = result.values().get(topicName);

      future.get();
    } catch (ExecutionException | InterruptedException e) {
      log.error(e.getMessage(), e);
    }
  }

  public static void deleteTopic(Properties adminApiProperties, Map<ServiceProperty, String> serviceProperties) {
    try (Admin admin = Admin.create(adminApiProperties)) {
      DeleteTopicsResult result = admin.deleteTopics(Collections.singleton(serviceProperties.get(ServiceProperty.TOPIC_NAME)));

      KafkaFuture<Void> future = result.all();
      future.get();
    } catch (ExecutionException | InterruptedException e) {
      log.error(e.getMessage(), e);
    }
  }

  public static boolean clearTopic(Properties apiProperties, Map<ServiceProperty, String> serviceProperties) {
    String topicName = serviceProperties.get(ServiceProperty.TOPIC_NAME);

    ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

    boolean isCleanupPolicyChanged = checkCleanupPolicy(apiProperties, topicName, configResource);

    try (Admin admin = Admin.create(apiProperties)) {
      Map<TopicPartition, RecordsToDelete> recordsToDelete = getRecordsToDelete(serviceProperties, admin, topicName);

      DeleteRecordsResult result = admin.deleteRecords(recordsToDelete);

      Map<TopicPartition, KafkaFuture<DeletedRecords>> lowWatermarks = result.lowWatermarks();

      deleteRecords(lowWatermarks);

      if (isCleanupPolicyChanged) {
        changeCleanupPolicyToCompact(configResource, topicName, admin);
      }

      return true;
    } catch (ExecutionException | InterruptedException e) {
      log.error(e.getMessage(), e);
      return false;
    }
  }

  private static boolean checkCleanupPolicy(Properties apiProperties, String topicName, ConfigResource configResource) {
    try (Admin admin = Admin.create(apiProperties)) {
      Map<ConfigResource, Config> configs = admin.describeConfigs(Collections.singleton(configResource))
        .all()
        .get();

      if (configs.get(configResource).get("cleanup.policy").value().equals("compact")) {
        changeCleanupPolicyToDelete(configResource, topicName, admin);
        return true;
      }
    } catch (ExecutionException | InterruptedException e) {
      log.error(e.getMessage(), e);
    }

    return false;
  }

  private static void deleteRecords(Map<TopicPartition, KafkaFuture<DeletedRecords>> lowWatermarks)
    throws InterruptedException, ExecutionException {
    for (Map.Entry<TopicPartition, KafkaFuture<DeletedRecords>> entry : lowWatermarks.entrySet()) {
      entry.getValue().get().lowWatermark();

      log.info(String.format("Successfully delete message from topic: %s, partition: %s",
                             entry.getKey().topic(),
                             entry.getKey().partition()));
    }
  }

  private static void changeCleanupPolicyToDelete(ConfigResource configResource, String topic, Admin admin)
    throws ExecutionException, InterruptedException {
    ConfigEntry cleanupPolicyEntry = new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);

    updateCleanupPolicyConfig(admin, configResource, cleanupPolicyEntry);
  }

  private static void changeCleanupPolicyToCompact(ConfigResource configResource, String topic, Admin admin)
    throws ExecutionException, InterruptedException {
    ConfigEntry cleanupPolicyEntry = new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);

    updateCleanupPolicyConfig(admin, configResource, cleanupPolicyEntry);
  }

  private static void updateCleanupPolicyConfig(Admin admin, ConfigResource configResource, ConfigEntry cleanupPolicyEntry)
    throws ExecutionException, InterruptedException {
    AlterConfigOp config = new AlterConfigOp(cleanupPolicyEntry, AlterConfigOp.OpType.SET);

    Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
    configs.put(configResource, List.of(config));

    AlterConfigsResult alterConfigsResult = admin.incrementalAlterConfigs(configs);
    alterConfigsResult.all().get();

    Map<ConfigResource, Config> updatedConfigs = admin.describeConfigs(Collections.singleton(configResource))
      .all()
      .get();

    log.info("Cleanup policy after updating: " + updatedConfigs.get(configResource).get("cleanup.policy").value());
  }

  private static Map<TopicPartition, RecordsToDelete> getRecordsToDelete(Map<ServiceProperty, String> serviceProperties, Admin admin, String topicName) {
    Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();

    KafkaFuture<TopicDescription> topicDescription = admin.describeTopics(Collections.singleton(topicName)).values().get(topicName);

    int partitions = 0;
    try {
      partitions = topicDescription.get().partitions().size();
    } catch (InterruptedException | ExecutionException e) {
      log.error(e.getMessage(), e);
    }

    Properties consumerProperties = ConsumerApiController.createConsumerProperties(serviceProperties);

    for (int partition = 0; partition < partitions; partition++) {
      TopicPartition topicPartition = new TopicPartition(topicName, partition);
      recordsToDelete.put(topicPartition, RecordsToDelete.beforeOffset(ConsumerApiService.getLastOffsetOfPartition(topicPartition, consumerProperties)));
    }
    return recordsToDelete;
  }

  private static CreateTopicsOptions createTopicsOptions(String pathToFile) {
    CreateTopicsOptions topicsOptions = null;

    try (InputStream input = new FileInputStream(pathToFile)) {
      Properties properties = new Properties();
      properties.load(input);

      topicsOptions = new CreateTopicsOptions()
        .retryOnQuotaViolation(Boolean.parseBoolean(properties.getProperty("retry.on.quota.violation")))
        .timeoutMs(Integer.valueOf(properties.getProperty("timeout.ms")))
        .validateOnly(Boolean.parseBoolean(properties.getProperty("validate.only")));
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    return topicsOptions;
  }

  private static Map<String, String> createTopicConfigs(String pathToFile) {
    Map<String, String> topicConfigs = null;

    try (InputStream input = new FileInputStream(pathToFile)) {
      Properties properties = new Properties();
      properties.load(input);

      topicConfigs = new HashMap<>();
      topicConfigs.put(TopicConfig.SEGMENT_BYTES_CONFIG, properties.getProperty("segment.bytes"));
      topicConfigs.put(TopicConfig.SEGMENT_MS_CONFIG, properties.getProperty("segment.ms"));
      topicConfigs.put(TopicConfig.SEGMENT_JITTER_MS_CONFIG, properties.getProperty("segment.jitter.ms"));
      topicConfigs.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, properties.getProperty("segment.index.bytes"));
      topicConfigs.put(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, properties.getProperty("flush.messages"));
      topicConfigs.put(TopicConfig.FLUSH_MS_CONFIG, properties.getProperty("flush.ms"));
      topicConfigs.put(TopicConfig.RETENTION_BYTES_CONFIG, properties.getProperty("retention.bytes"));
      topicConfigs.put(TopicConfig.RETENTION_MS_CONFIG, properties.getProperty("retention.ms"));
      topicConfigs.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, properties.getProperty("max.message.bytes"));
      topicConfigs.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, properties.getProperty("index.interval.bytes"));
      topicConfigs.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, properties.getProperty("file.delete.delay.ms"));
      topicConfigs.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, properties.getProperty("delete.retention.ms"));
      topicConfigs.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, properties.getProperty("min.compaction.lag.ms"));
      topicConfigs.put(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, properties.getProperty("max.compaction.lag.ms"));
      topicConfigs.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, properties.getProperty("min.cleanable.dirty.ratio"));
      topicConfigs.put(TopicConfig.CLEANUP_POLICY_CONFIG, properties.getProperty("cleanup.policy"));
      topicConfigs.put(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, properties.getProperty("unclean.leader.election.enable"));
      topicConfigs.put(TopicConfig.COMPRESSION_TYPE_CONFIG, properties.getProperty("compression.type"));
      topicConfigs.put(TopicConfig.PREALLOCATE_CONFIG, properties.getProperty("preallocate"));
      topicConfigs.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, properties.getProperty("message.timestamp.type"));
      topicConfigs.put(TopicConfig.MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, properties.getProperty("message.timestamp.difference.max.ms"));
      topicConfigs.put(TopicConfig.MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, properties.getProperty("message.downconversion.enable"));
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    return topicConfigs;
  }

  private static void validateServiceProperties(Map<ServiceProperty, String> serviceProperties) {
    String partitionsString = serviceProperties.get(ServiceProperty.PARTITIONS);
    String replicationFactorString = serviceProperties.get(ServiceProperty.REPLICATION_FACTOR);
    String configFile = serviceProperties.get(ServiceProperty.CONFIG_FILE);

    int partitions = Integer.parseInt(partitionsString);
    short replicationFactor = Short.parseShort(replicationFactorString);

    if (partitions < 1) {
      throw new InvalidPartitionsValueException(partitionsString);
    }

    if (replicationFactor < 1) {
      throw new InvalidReplicationFactorValueException(replicationFactorString);
    }

    if (configFile != null && !Files.isRegularFile(Paths.get(configFile))) {
      throw new InvalidConfigFileException(configFile);
    }
  }

  public static long getOffsetForConsumerGroup(Properties adminApiProperties, Map<ServiceProperty, String> serviceProperties) {
    long offset = -1;
    int partition = Integer.parseInt(serviceProperties.get(ServiceProperty.PARTITION));
    String topic = serviceProperties.get(ServiceProperty.TOPIC_NAME);
    String consumerGroup = serviceProperties.get(ServiceProperty.CONSUMER_GROUP);

    try (AdminClient adminClient = KafkaAdminClient.create(adminApiProperties)) {
      ListConsumerGroupOffsetsResult results = adminClient.listConsumerGroupOffsets(consumerGroup);

      Map<TopicPartition, OffsetAndMetadata> partitionsToOffsets = results.partitionsToOffsetAndMetadata().get();

      for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : partitionsToOffsets.entrySet()) {
        if (entry.getKey().topic().equals(topic) && entry.getKey().partition() == partition) {
          offset = entry.getValue().offset();
        }
      }

      if (offset == -1) {
        throw new OffsetNotFoundException(consumerGroup);
      }
    } catch (final ExecutionException | InterruptedException e) {
      log.error(e.getMessage(), e);
    }
    return offset;
  }

  public static void setOffsetForConsumerGroup(Properties adminApiProperties, Map<ServiceProperty, String> serviceProperties) {
    long newOffset = Long.parseLong(serviceProperties.get(ServiceProperty.OFFSET));
    String topic = serviceProperties.get(ServiceProperty.TOPIC_NAME);
    String groupId = serviceProperties.get(ServiceProperty.CONSUMER_GROUP);

    try (AdminClient adminClient = KafkaAdminClient.create(adminApiProperties)) {
      ListConsumerGroupOffsetsResult results = adminClient.listConsumerGroupOffsets(groupId);

      Map<TopicPartition, OffsetAndMetadata> partitionsToOffsets = results.partitionsToOffsetAndMetadata().get();

      for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : partitionsToOffsets.entrySet()) {
        if (entry.getKey().topic().equals(topic)) {
          entry.setValue(new OffsetAndMetadata(newOffset));
        }
      }

      adminClient.alterConsumerGroupOffsets(groupId, partitionsToOffsets).all().get();
    } catch (final ExecutionException | InterruptedException e) {
      log.error(e.getMessage(), e);
    }
  }

  public static void setOffsetsForConsumerGroup(Map<TopicPartition, OffsetAndMetadata> oldOffsets,
                                                Map<ServiceProperty, String> serviceProperties,
                                                Properties adminApiProperties) {
    String groupId = serviceProperties.get(ServiceProperty.CONSUMER_GROUP);

    try (AdminClient adminClient = KafkaAdminClient.create(adminApiProperties)) {

      adminClient.alterConsumerGroupOffsets(groupId, oldOffsets)
        .all()
        .get(timeout, timeUnit);

      ListConsumerGroupOffsetsResult results = adminClient.listConsumerGroupOffsets(groupId);

      Map<TopicPartition, OffsetAndMetadata> newOffsets = results.partitionsToOffsetAndMetadata()
        .get(timeout, timeUnit);

      if (newOffsets.isEmpty()) {
        log.error(String.format("Offsets did not set for consumer group %s with timeout: %s %s",
                                groupId, timeout, timeUnit.name()));

        throw new OffsetsAndMetadataNotFoundException(groupId);
      }
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      log.error(String.format("Exception was thrown while setting offsets for consumer group %s!",
                              groupId), e);
    }
  }

  public static Map<TopicPartition, OffsetAndMetadata> getOffsetsForConsumerGroup(Map<ServiceProperty, String> serviceProperties,
                                                                                  Properties adminApiProperties) {

    String groupId = serviceProperties.get(ServiceProperty.CONSUMER_GROUP);

    try (AdminClient adminClient = KafkaAdminClient.create(adminApiProperties)) {
      ListConsumerGroupOffsetsResult results = adminClient.listConsumerGroupOffsets(groupId);

      Map<TopicPartition, OffsetAndMetadata> map = results.partitionsToOffsetAndMetadata()
        .get(timeout, timeUnit);

      if (map.isEmpty()) {
        log.error(String.format("Offsets not found for consumer group: %s with timeout: %s %s",
                                groupId, timeout, timeUnit.name()));
      } else {
        return map;
      }
    } catch (final ExecutionException | InterruptedException | TimeoutException e) {
      log.error(String.format("Exception was thrown while getting offsets for consumer group: %s",
                              groupId), e);
    }

    throw new OffsetsAndMetadataNotFoundException(groupId);
  }

  public static List<String> getTopics(Properties adminApiProperties) {
    List<String> listOfTopics = new ArrayList<>();

    try (AdminClient adminClient = KafkaAdminClient.create(adminApiProperties)) {
      ListTopicsOptions listTopicsOptions = new ListTopicsOptions().listInternal(true);

      Collection<TopicListing> results = adminClient.listTopics(listTopicsOptions).listings().get();

      for (final TopicListing entry : results) {
        listOfTopics.add(entry.name());
      }

    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
    return listOfTopics;
  }

  public static List<String> getConsumerGroups(Properties adminApiProperties) {
    List<String> listOfConsumers = new ArrayList<>();

    try (AdminClient adminClient = KafkaAdminClient.create(adminApiProperties)) {
      ListConsumerGroupsResult results = adminClient.listConsumerGroups();

      results.all().get().forEach((result) -> listOfConsumers.add(result.groupId()));
    } catch (ExecutionException | InterruptedException e) {
      e.printStackTrace();
    }
    return listOfConsumers;
  }

  public static void waitUntilConsumerGroupWasCreated(Properties adminApiProperties, Map<ServiceProperty, String> serviceProperties) {
    String consumerGroup = serviceProperties.get(ServiceProperty.CONSUMER_GROUP);

    int timeoutWhile = 30000;
    long start = System.currentTimeMillis() + timeoutWhile;

    while (System.currentTimeMillis() < start) {
      if (getConsumerGroups(adminApiProperties).contains(consumerGroup)) {
        return;
      }
    }

    log.error(String.format("Consumer group %s was not create with timeout: %s",
                            consumerGroup, timeoutWhile + " millis"));

    throw new ConsumerGroupNotCreateException(consumerGroup);
  }

  public static List<String> getTopicsByConsumerGroup(Properties adminApiProperties, Map<ServiceProperty, String> serviceProperties) {
    List<String> listOfTopicsByConsumerGroup = new ArrayList<>();
    String consumerGroup = serviceProperties.get(ServiceProperty.CONSUMER_GROUP);

    try (AdminClient adminClient = KafkaAdminClient.create(adminApiProperties)) {
      ListConsumerGroupOffsetsResult results = adminClient.listConsumerGroupOffsets(consumerGroup);

      Map<TopicPartition, OffsetAndMetadata> map = results.partitionsToOffsetAndMetadata()
        .get(timeout, timeUnit);

      if (map.isEmpty()) {
        log.error(String.format("Offsets not found for consumer group: %s with timeout: %s %s",
                                consumerGroup, timeout, timeUnit.name()));
        throw new ConsumerGroupNotFound(consumerGroup);
      } else {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : map.entrySet()) {
          if (!listOfTopicsByConsumerGroup.contains(entry.getKey().topic())) {
            listOfTopicsByConsumerGroup.add(entry.getKey().topic());
          }
        }
      }
    } catch (final ExecutionException | InterruptedException | TimeoutException e) {
      log.error(String.format("Exception was thrown while getting offsets for consumer group: %s",
                              consumerGroup), e);
    }

    return listOfTopicsByConsumerGroup;
  }

  public static List<String> getTopicConfig(Properties apiProperties, Map<ServiceProperty, String> serviceProperties) {
    String topicName = serviceProperties.get(ServiceProperty.TOPIC_NAME);
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    List<String> configItems = new ArrayList<>();

    try (AdminClient adminClient = KafkaAdminClient.create(apiProperties)) {
      DescribeConfigsResult result = adminClient.describeConfigs(Collections.singleton(configResource));
      Map<ConfigResource, Config> configMap = result.all().get();

      Config config = configMap.get(configResource);
      for (ConfigEntry configEntry : config.entries()) {
        if (configEntry.isSensitive()) {
          continue;
        }
        configItems.add(configEntry.name() + configEntry.value() + configEntry.isDefault());
      }

    } catch (ExecutionException | InterruptedException e) {
      log.error(String.format("Exception was thrown while getting topic configs: %s", topicName), e);
    }
    return configItems;
  }

  public static Map<String, Object> getTopicDetails(Properties apiProperties, Map<ServiceProperty, String> serviceProperties) {
    Map<String, Object> mapTopicDetails = new HashMap<>();
    String topicName = serviceProperties.get(ServiceProperty.TOPIC_NAME);
    try (AdminClient adminClient = KafkaAdminClient.create(apiProperties)) {

      final Map<String, TopicDescription> topicDescriptionMap = adminClient.describeTopics(Collections.singleton(topicName)).all().get();
      for (final Map.Entry<String, TopicDescription> entry : topicDescriptionMap.entrySet()) {
        TopicDescription topicDescription = entry.getValue();

        mapTopicDetails.put("topic-name", topicDescription.name());
        mapTopicDetails.put("partitions", topicDescription.partitions());
        mapTopicDetails.put("topic-id", topicDescription.topicId());
      }
    } catch (final ExecutionException | InterruptedException e) {
      log.error(String.format("Exception was thrown while getting topic details: %s", topicName), e);
    }
    return mapTopicDetails;
  }

  public static void deleteConsumerGroup(Properties adminApiProperties, Map<ServiceProperty, String> serviceProperties) {
    String consumerGroup = serviceProperties.get(ServiceProperty.CONSUMER_GROUP);
    try (AdminClient adminClient = KafkaAdminClient.create(adminApiProperties)) {
      DeleteConsumerGroupsResult deleteConsumerGroupsResult = adminClient.deleteConsumerGroups(Collections.singleton(consumerGroup));
      KafkaFuture<Void> future = deleteConsumerGroupsResult.all();
      future.get();
      if (deleteConsumerGroupsResult.deletedGroups() == null) {
        throw new ConsumerGroupNotFound(consumerGroup);
      }
    } catch (ExecutionException | InterruptedException | ConsumerGroupNotFound e) {
      log.error(String.format("Exception was thrown while deleting consumer group: %s", consumerGroup), e);
    }
  }

}

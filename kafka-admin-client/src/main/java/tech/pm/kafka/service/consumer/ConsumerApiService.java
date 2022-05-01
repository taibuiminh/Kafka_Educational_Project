package tech.pm.kafka.service.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.pm.kafka.property.ServiceProperty;
import tech.pm.kafka.service.consumer.listener.SeekToTimeOnRebalanceListener;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerApiService {

  private static final Logger log = LoggerFactory.getLogger(ConsumerApiService.class);

  public static String readMessageInTopicByTimestamp(Properties apiProperties, Map<ServiceProperty, String> serviceProperties) {
    String resultRecord = null;

    String topic = serviceProperties.get(ServiceProperty.TOPIC_NAME);
    long startTimestamp = Long.parseLong(serviceProperties.get(ServiceProperty.TIMESTAMP));

    Consumer<String, String> consumer = new KafkaConsumer<>(apiProperties);
    ConsumerRebalanceListener timestampListener = new SeekToTimeOnRebalanceListener(consumer, startTimestamp);

    consumer.subscribe(Collections.singleton(topic), timestampListener);

    boolean isPolled = true;

    while (isPolled) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records) {
        if (record.timestamp() < startTimestamp) {
          continue;
        }

        isPolled = false;

        resultRecord = String.format("Record: topic=%s key=%s, value=%s, timestamp=%s, offset=%s, partition=%s",
                                     record.topic(), record.key(), record.value(),
                                     record.timestamp(), record.offset(), record.partition());

        break;
      }
    }
    consumer.close();

    return resultRecord;
  }

  public static String readMessageInTopicPartitionByOffset(Properties apiProperties, Map<ServiceProperty, String> serviceProperties) {
    String topic = serviceProperties.get(ServiceProperty.TOPIC_NAME);
    int partition = Integer.parseInt(serviceProperties.get(ServiceProperty.PARTITION));
    long offsetToFinding = Long.parseLong(serviceProperties.get(ServiceProperty.OFFSET));
    String foundedMessage;

    Consumer<String, String> consumer = new KafkaConsumer<>(apiProperties);

    TopicPartition topicPartition = new TopicPartition(topic, partition);
    consumer.assign(Collections.singletonList(topicPartition));
    consumer.seek(topicPartition, offsetToFinding);
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      if (!records.isEmpty()) {
        ConsumerRecord<String, String> foundedRecord = records.iterator().next();
        foundedMessage = String.format("Record: topic=%s, key=%s, value=%s, timestamp=%s, offset=%s, partition=%s",
                                       foundedRecord.topic(), foundedRecord.key(), foundedRecord.value(),
                                       foundedRecord.timestamp(), foundedRecord.offset(), foundedRecord.partition());
        break;
      }
    }

    consumer.close();

    return foundedMessage;
  }

  public static long getLastOffsetOfPartition(TopicPartition topicPartition, Properties consumerProperties) {
    Consumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
    consumer.assign(Collections.singleton(topicPartition));
    consumer.seekToEnd(Collections.singleton(topicPartition));

    long position = consumer.position(topicPartition);

    consumer.close();

    return position;
  }

  public static Consumer<String, String> createConsumer(Properties apiProperties, Map<ServiceProperty, String> serviceProperties) {
    String topic = serviceProperties.get(ServiceProperty.TOPIC_NAME);
    Consumer<String, String> consumer = new KafkaConsumer<>(apiProperties);
    consumer.subscribe(Collections.singleton(topic));
    return consumer;
  }

  public static Consumer<String, String> createConsumerWithListOfTopics(Properties apiProperties, List<String> topics) {
    Consumer<String, String> consumer = new KafkaConsumer<>(apiProperties);
    consumer.subscribe(topics);
    consumer.poll(Duration.ofMillis(30000));

    return consumer;
  }

  public static void stopConsumer(Consumer<String, String> consumer) {
    consumer.close();
  }
}

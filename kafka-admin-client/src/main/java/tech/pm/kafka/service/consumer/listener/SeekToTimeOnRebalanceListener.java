package tech.pm.kafka.service.consumer.listener;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SeekToTimeOnRebalanceListener implements ConsumerRebalanceListener {

  private static final Logger logger = LoggerFactory.getLogger(SeekToTimeOnRebalanceListener.class);

  private final Consumer<?, ?> consumer;
  private final long startTimestamp;

  public SeekToTimeOnRebalanceListener(Consumer<?, ?> consumer, long startTimestamp) {
    this.consumer = consumer;
    this.startTimestamp = startTimestamp;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    Map<TopicPartition, Long> timestampToSearch = new HashMap<>();

    for (TopicPartition partition : partitions) {
      timestampToSearch.put(partition, startTimestamp);
    }

    Map<TopicPartition, OffsetAndTimestamp> outOffsets = consumer.offsetsForTimes(timestampToSearch);

    for (TopicPartition partition : partitions) {
      try {
        Long seekOffset = outOffsets.get(partition).offset();
        Long currentPosition = consumer.position(partition);

        if (seekOffset.compareTo(currentPosition) == 0) {
          consumer.seek(partition, seekOffset);
        }
      } catch (KafkaException e) {
        logger.error(e.getMessage(), e);
        consumer.close();
        System.exit(e.hashCode());
      }
    }
  }
}

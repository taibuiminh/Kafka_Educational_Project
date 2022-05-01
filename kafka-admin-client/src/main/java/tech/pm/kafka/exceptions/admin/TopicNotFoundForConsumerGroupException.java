package tech.pm.kafka.exceptions.admin;

public class TopicNotFoundForConsumerGroupException extends RuntimeException {
  public TopicNotFoundForConsumerGroupException(String consumerGroup, String topicName) {
    super(String.format("Topic %s is not found for %s", topicName, consumerGroup));
  }
}

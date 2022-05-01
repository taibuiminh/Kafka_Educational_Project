package tech.pm.kafka;

import java.util.Arrays;

public enum KafkaCommand {
  CREATE_TOPIC("create_topic"),
  DELETE_TOPIC("delete_topic"),
  PURGE_TOPIC("purge_topic"),
  GET_CURRENT_OFFSET_FOR_CONSUMER_GROUP("get_offset_consumer_group"),
  SET_CURRENT_OFFSET_FOR_CONSUMER_GROUP("set_offset_consumer_group"),
  READ_MESSAGE_IN_TOPIC_BY_TIME_OR_OFFSET("read_message"),
  GET_TOPICS_NAME("get_topics"),
  GET_CONSUMERS_NAME("get_consumers"),
  WRITE_FROM_FILE("write_from_file"),
  CREATE_CONSUMER("create_consumer"),
  GET_LIST_TOPICS_BY_CONSUMER_GROUP("get_topics_for_consumer_group"),
  RENAME_KAFKA_CONNECTOR("rename_kafka_connector"),
  MOVE_TOPIC_TO_ANOTHER_CONNECTOR("move_topic_to_connector"),
  GET_TOPIC_CONFIGS("get_topic_configs"),
  GET_TOPIC_DETAILS("get_topic_details"),
  DELETE_CONSUMER_GROUP("delete_consumer_group");

  private final String command;

  KafkaCommand(String command) {
    this.command = command;
  }

  public static boolean checkCommand(String command) {
    return Arrays.stream(KafkaCommand.values()).anyMatch(kafkaCommand -> kafkaCommand.getCommand().equals(command));
  }

  public String getCommand() {
    return command;
  }
}

package tech.pm.utils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TopicUtils {
  private final static Logger log = LoggerFactory.getLogger(TopicUtils.class);

  public static void createTopic(String topic, Properties adminApiProperties) {
    try (Admin admin = Admin.create(adminApiProperties)) {

      NewTopic newTopic = new NewTopic(topic, 1, (short) 1);

      CreateTopicsResult result;

      result = admin.createTopics(Collections.singleton(newTopic));

      KafkaFuture<Void> future = result.values().get(topic);

      future.get();

      log.info("Created topic: {}", topic);
    } catch (ExecutionException | InterruptedException e) {
      log.error(e.getMessage(), e);
    }
  }
}

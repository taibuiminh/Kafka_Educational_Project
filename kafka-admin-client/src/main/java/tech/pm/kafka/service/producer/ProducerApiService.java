package tech.pm.kafka.service.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.pm.kafka.property.ServiceProperty;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class ProducerApiService {

  private static final Logger logger = LoggerFactory.getLogger(ProducerApiService.class);

  public static void writeToTopicFromFile(Properties apiProperties, Map<ServiceProperty, String> serviceProperties) {

    String topicName = serviceProperties.get(ServiceProperty.TOPIC_NAME);
    String sourceFile = serviceProperties.get(ServiceProperty.SOURCE_FILE);

    try (Scanner scanner = new Scanner(new File(sourceFile)); Producer<String, String> producer = new KafkaProducer<>(apiProperties)) {

      while (scanner.hasNextLine()) {
        String message = scanner.nextLine();
        ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);
        producer.send(record).get();
        logger.info(String.format("Content from file %s: %s", sourceFile, message));
      }

    } catch (InterruptedException | ExecutionException | FileNotFoundException e) {
      logger.error(e.getMessage(), e);
    }
  }
}

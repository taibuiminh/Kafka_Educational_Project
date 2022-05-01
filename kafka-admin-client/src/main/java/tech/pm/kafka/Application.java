package tech.pm.kafka;

import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.pm.kafka.controller.AdminApiController;
import tech.pm.kafka.controller.ConnectRestApiController;
import tech.pm.kafka.controller.ConsumerApiController;
import tech.pm.kafka.controller.ProducerApiController;
import tech.pm.kafka.exceptions.input.InvalidArgumentException;
import tech.pm.kafka.exceptions.input.InvalidCommandException;
import tech.pm.kafka.exceptions.input.WrongArgumentSymbolException;
import tech.pm.kafka.property.ServiceProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Application {

  private static final Logger log = LoggerFactory.getLogger(Application.class);

  private static final Map<ServiceProperty, String> kafkaArguments = new HashMap<>();
  private static String command;

  public static void main(String[] args) throws InvalidArgumentException, WrongArgumentSymbolException {
    BasicConfigurator.configure();

    validateArguments(args);
    processArguments(args);
    kafkaServicesRunner(command);
  }

  private static void processArguments(String[] arguments) throws InvalidArgumentException {
    if (!KafkaCommand.checkCommand(arguments[0])) {
      throw new InvalidCommandException(arguments[0]);
    }

    command = arguments[0];

    for (int i = 1; i < arguments.length; i++) {
      String[] flagValue = arguments[i].split("=");

      String flag = flagValue[0];
      String value = flagValue[1];

      switch (flag) {
        case "--bootstrap-server":
          kafkaArguments.put(ServiceProperty.BOOTSTRAP_SERVER, value);
          break;
        case "--topic":
          kafkaArguments.put(ServiceProperty.TOPIC_NAME, value);
          break;
        case "--partitions":
          kafkaArguments.put(ServiceProperty.PARTITIONS, value);
          break;
        case "--partition":
          kafkaArguments.put(ServiceProperty.PARTITION, value);
          break;
        case "--replication-factor":
          kafkaArguments.put(ServiceProperty.REPLICATION_FACTOR, value);
          break;
        case "--config-file":
          kafkaArguments.put(ServiceProperty.CONFIG_FILE, value);
          break;
        case "--source-file":
          kafkaArguments.put(ServiceProperty.SOURCE_FILE, value);
          break;
        case "--timestamp":
          kafkaArguments.put(ServiceProperty.TIMESTAMP, value);
          break;
        case "--offset":
          kafkaArguments.put(ServiceProperty.OFFSET, value);
          break;
        case "--group":
          kafkaArguments.put(ServiceProperty.CONSUMER_GROUP, value);
          break;
        case "--new-group":
          kafkaArguments.put(ServiceProperty.NEW_CONSUMER_GROUP, value);
          break;
        case "--new-connector":
          kafkaArguments.put(ServiceProperty.NEW_CONNECTOR_NAME, value);
          break;
        case "--old-connector":
          kafkaArguments.put(ServiceProperty.OLD_CONNECTOR_NAME, value);
          break;
        case "--connectors-url":
          kafkaArguments.put(ServiceProperty.CONNECTORS_URL, value);
          break;
        default:
          throw new InvalidArgumentException(arguments[i]);
      }
    }
  }

  private static void kafkaServicesRunner(String command) {
    switch (command) {
      case ("create_topic"):
        log.info(String.format("Creating topic... %s", kafkaArguments.get(ServiceProperty.TOPIC_NAME)));
        AdminApiController.createTopic(kafkaArguments);
        break;
      case ("delete_topic"):
        log.info(String.format("Deleting topic... %s", kafkaArguments.get(ServiceProperty.TOPIC_NAME)));
        AdminApiController.deleteTopic(kafkaArguments);
        break;
      case ("purge_topic"):
        log.info(String.format("Purging topic... %s", kafkaArguments.get(ServiceProperty.TOPIC_NAME)));
        AdminApiController.purgeTopic(kafkaArguments);
        break;
      case ("get_offset_consumer_group"):
        log.info("Getting current offset for consumer group...");
        System.out.println(AdminApiController.getCurrentOffsetForConsumerGroup(kafkaArguments));
        break;
      case ("set_offset_consumer_group"):
        log.info("Setting current offset for consumer group...");
        AdminApiController.setCurrentOffsetForConsumerGroup(kafkaArguments);
        break;
      case ("read_message"):
        log.info("Reading message in topic by time or offset...");
        System.out.println(ConsumerApiController.readMessageInTopicByTimestampOrOffset(kafkaArguments));
        break;
      case ("get_topics"):
        log.info("Getting topics...");
        System.out.println(AdminApiController.getTopicsNames(kafkaArguments));
        break;
      case ("get_consumers"):
        log.info("Getting consumers...");
        System.out.println(AdminApiController.getConsumers(kafkaArguments));
        break;
      case ("write_from_file"):
        log.info("Writing to topic from file...");
        ProducerApiController.writeToTopicFromFile(kafkaArguments);
        break;
      case ("create_consumer"):
        log.info("Creating consumer...");
        ConsumerApiController.createConsumer(kafkaArguments);
        break;
      case ("get_topics_for_consumer_group"):
        log.info("Getting topics for consumer group...");
        System.out.println(AdminApiController.getTopicsByConsumerGroup(kafkaArguments));
        break;
      case ("rename_kafka_connector"):
        log.info("Renaming kafka connector...");
        ConnectRestApiController.renameKafkaConnector(kafkaArguments);
        break;
      case ("move_topic_to_connector"):
        log.info("Moving kafka topic from one to another connector...");
        ConnectRestApiController.moveTopicToAnotherConnector(kafkaArguments);
        break;
      case ("get_topic_configs"):
        log.info("Getting topic configs...");
        System.out.println(AdminApiController.getTopicConfig(kafkaArguments));
        break;
      case ("get_topic_details"):
        log.info("Getting topic details...");
        System.out.println(AdminApiController.getTopicDetails(kafkaArguments));
        break;
      case ("delete_consumer_group"):
        log.info("Deleting consumer group...");
        AdminApiController.deleteConsumerGroup(kafkaArguments);
        break;
    }
  }

  private static void validateArguments(String[] arguments) throws WrongArgumentSymbolException {
    Pattern pattern = Pattern.compile("[^A-Za-z0-9:_./=-]");
    for (String arg : arguments) {
      Matcher matcher = pattern.matcher(arg);
      if (matcher.find()) {
        throw new WrongArgumentSymbolException(arg);
      }
    }
  }
}

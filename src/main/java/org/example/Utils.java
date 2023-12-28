package org.example;

import org.apache.kafka.clients.admin.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;

public class Utils {

    public static Logger logger = LoggerFactory.getLogger(Utils.class);

    public static void recreateTopics(String host, int numPartitions, int replicationFactor, List<String> topics) throws Exception {
        Admin admin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, host));

        List<String> nameTopics = admin.listTopics().listings().get()
                .stream().map(TopicListing::name).toList();

        logger.info("удаление топиков в брокере: " + nameTopics);
        DeleteTopicsResult deleteTopicsResult = admin.deleteTopics(nameTopics);
        while (!deleteTopicsResult.all().isDone()) {
            //logger.info("...");
        }

        CreateTopicsResult createTopicsResult = admin.createTopics(
                topics.stream()
                        .map(it -> new NewTopic(it, numPartitions, (short) replicationFactor))
                        .toList()
        );

        logger.info("создание топиков "+topics+" с параметрами: numPartitions="+numPartitions+" replicationFactor="+replicationFactor);
        while (!createTopicsResult.all().isDone()) {
            //logger.info("...");
        }
    }

}
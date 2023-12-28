package org.example;

import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        final String host = "localhost:9092";
        final List<String> topics = List.of("topic1", "topic2");

        //Создать два топика: topic1 и topic2
        Utils.recreateTopics(host, 1, 1, topics);

        Consumer.process(host, topics);

        Producer.process(host);
    }
}
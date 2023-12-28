package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Consumer {

    public static void process(String host, List<String> topics) {

        new Thread(new Runnable() {
            @Override
            public void run() {
                KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host,
                        ConsumerConfig.GROUP_ID_CONFIG, "consumer-"+UUID.randomUUID(),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));

                try {
                    consumer.subscribe(topics);

                    while (true) {
                        var read = consumer.poll(Duration.ofSeconds(1));
                        for (var record : read) {
                            Utils.logger.info("Receive:{} at {}", record.value(), record.offset());
                        }
                    }

                } finally {
                    if(consumer!=null){
                        consumer.close();
                    }
                }
            }
        }).start();
    }

}
package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Map;

public class Producer {

    public static void process(String host){

        new Thread(new Runnable() {
            @Override
            public void run() {
                KafkaProducer<String, String> producer = null;

                try {
                    producer = new KafkaProducer<String, String>(Map.of(
                            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host,
                            ProducerConfig.ACKS_CONFIG, "all",
                            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                            ProducerConfig.TRANSACTIONAL_ID_CONFIG, "producer-app"));

                    producer.initTransactions();


                    for (int i = 0; i < 5; ++i) {
                        //открываем транзакцию
                        //отправляем по 5 сообщений в каждый топик
                        producer.beginTransaction();

                        producer.send(new ProducerRecord<>("topic1", "fist-" + i));
                        producer.send(new ProducerRecord<>("topic2", "second-" + i));
                        //коммит
                        producer.commitTransaction();
                    }


                    for (int i = 0; i < 2; ++i) {
                        //открываем транзакцию
                        //отправляем по 2 сообщений в каждый топик
                        producer.beginTransaction();
                        producer.send(new ProducerRecord<>("topic1", "third-" + i));
                        producer.send(new ProducerRecord<>("topic2", "fourth-" + i));
                        //прерываем
                        producer.abortTransaction();
                    }

                } finally {
                    if(producer!=null){
                        producer.close();
                    }
                }
            }

        }).start();
    }
}
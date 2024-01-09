package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;

public class Main {

    public static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        final String host = "localhost:9092";
        final List<String> topics = List.of("event-source");  //источник

        Utils.recreateTopics(host, 3, 1, topics);

        StreamsBuilder streamsBuilder = new StreamsBuilder();


        streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("event-store-count"), Serdes.String(), Serdes.Integer()));  //key-store

        //читаем из источника текстовый сообщения
        KStream<Void, String> kStream = streamsBuilder
                .stream(topics, Consumed.with(Serdes.Void(), Serdes.String())
                        .withTimestampExtractor(new WallclockTimestampExtractor())); // считаем началом прохождения события по конвееру

        //создаем новый поток
        //навсякий случай фильтруем если сообщение пустое
        //добавялем ключ и
        //!!!условие для repartition - без этого в key-store даже с ключами добавляет в патришины с RoundRobin (не то что нужно)
        KStream<String, String> keyedStream =
                kStream
                .filter((key, value) -> !value.isEmpty())
                .selectKey((key, value) -> value)
                .repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withStreamPartitioner(new Partitioner()));


        keyedStream
                .peek((k, v) -> logger.info("Event: {} -> {}", k, v))
                .processValues(CountProcessor::new, "event-store-count") //подсчет событий с одинаковыми клчючами
                .peek((k, v) -> logger.info("Count: {} -> {}", k, v));


        logger.info("{}", streamsBuilder.build().describe());


        KafkaStreams kafkaStreams = null;
        try{
            kafkaStreams = new KafkaStreams(streamsBuilder.build(), new StreamsConfig(new HashMap<>(){{
                    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, host);
                    put(StreamsConfig.APPLICATION_ID_CONFIG, "hm12");
            }}));
            kafkaStreams.start();


            Thread.sleep(200000);
        } finally {
            if(kafkaStreams!=null){
                kafkaStreams.close();
            }
        }

    }


    public static class Partitioner implements StreamPartitioner<String, String> {
        @Override
        public Integer partition(String topic, String key, String value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }


    static class CountProcessor implements FixedKeyProcessor<String, String, Integer> {

        private FixedKeyProcessorContext<String, Integer> context;
        private KeyValueStore<String, Integer> store;

        @Override
        public void init(FixedKeyProcessorContext<String, Integer> context) {
            this.context = context;

            //сеанс опционально
            Duration duration = Duration.ofSeconds(30);

            context.schedule(duration, PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                try (final KeyValueIterator<String, Integer> iter = store.all()) {
                    while (iter.hasNext()) {
                        final KeyValue<String, Integer> entry = iter.next();
                        store.delete(entry.key);
                    }
                    logger.info("Сеанс {} сек завершен ", duration.getSeconds());
                }
            });

            this.store = context.getStateStore("event-store-count");
        }

        @Override
        public void process(FixedKeyRecord<String, String> record) {
            var key = record.key();
            var cnt = store.get(key) == null ? 1 : store.get(key) + 1;

            store.put(key, cnt);

            context.forward(record.withValue(cnt));
        }
    }
}
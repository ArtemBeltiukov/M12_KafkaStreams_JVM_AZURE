package com.epam.bd201;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KStreamsApplication {

    public static void main(String[] args) {


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstreams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // If needed
        props.put("schema.registry.url", "schemaregistry:8081");

        final String INPUT_TOPIC_NAME = "expedia";
        final String OUTPUT_TOPIC_NAME = "expedia-ext";

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> input_records = builder.stream(INPUT_TOPIC_NAME, Consumed.with(Serdes.String(), Serdes.String()));


        input_records.mapValues(value -> {
            JSONObject json = new JSONObject(value);

            try {
                Date ci = new SimpleDateFormat("yyyy-MM-dd").parse(json.getString("srch_ci"));
                Date co = new SimpleDateFormat("yyyy-MM-dd").parse(json.getString("srch_co"));
                long duration = (co.getTime() - ci.getTime()) / 1000 / 60 / 60 / 24;

                String cat = "Erroneous data";

                if (duration > 0 && duration <= 4)
                    cat = "Short stay";
                if (duration > 4 && duration <= 10)
                    cat = "Standard stay";
                if (duration > 10 && duration <= 14)
                    cat = "Standard extended stay";
                if (duration > 14)
                    cat = "Long stay";

                json.put("stay_cat", cat);
            } catch (Exception e) {
                json.put("stay_cat", "Erroneous data");
            }
            return json.toString();
        }).to(OUTPUT_TOPIC_NAME);

        final Topology topology = builder.build();
        System.out.println(topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
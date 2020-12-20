package bwnetflow.application;


import bwnetflow.mptcp.aggregator.Aggregator;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.log4j.Logger;

import java.util.Properties;


public class Main {

    static Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        Properties properties = createProperties();
        Aggregator aggregator = new Aggregator();

        KafkaStreams streams = new KafkaStreams(aggregator.createAggregatorTopology(), properties);

        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Error occurred: ", throwable);
        });
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


   private static Properties createProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "mptcp-aggregator-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        // TODO find most optimal properties
        return properties;
    }
}

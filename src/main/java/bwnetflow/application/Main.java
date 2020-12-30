package bwnetflow.application;


import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.mptcp.aggregator.Aggregator;
import bwnetflow.mptcp.aggregator.DeduplicationProcessor;
import bwnetflow.serdes.proto.KafkaProtobufDeserializer;
import bwnetflow.serdes.proto.KafkaProtobufSerde;
import bwnetflow.serdes.proto.KafkaProtobufSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.Logger;

import java.util.Properties;

import static bwnetflow.mptcp.aggregator.Topics.OUTPUT_TOPIC;


public class Main {

    private final static Logger log = Logger.getLogger(Main.class.getName());

    private final static Serde<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowsSerde =
            new KafkaProtobufSerde<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser());

    private final static  KafkaProtobufDeserializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowMessageDeserializer
            = new KafkaProtobufDeserializer<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser());

    private final static  KafkaProtobufSerializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowSerializer
            = new KafkaProtobufSerializer<>();

    public static void main(String[] args) {
        Properties properties = createProperties();
        Aggregator aggregator = new Aggregator();

        StreamsBuilder builder = new StreamsBuilder();
        aggregator.create(builder);

        Topology topology = builder.build();

        var contributorStoreSupplier = Stores.persistentKeyValueStore("deduplication-store");

        var store = Stores.keyValueStoreBuilder(contributorStoreSupplier,
                Serdes.String(), mptcpFlowsSerde)
                .withCachingEnabled();

        topology
                .addSource("Source", new StringDeserializer(), mptcpFlowMessageDeserializer, OUTPUT_TOPIC)
                .addProcessor("DeduplicationProcessor", DeduplicationProcessor::new, "Source")
                .addStateStore(store, "DeduplicationProcessor")
                .addSink("Sink", "FINAL-OUTPUT", new StringSerializer(), mptcpFlowSerializer,"DeduplicationProcessor");

        KafkaStreams streams = new KafkaStreams(topology, properties);

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
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/state");

        // TODO find most optimal properties
        return properties;
    }
}

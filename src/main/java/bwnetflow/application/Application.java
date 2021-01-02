package bwnetflow.application;


import bwnetflow.configuration.impl.CLIConfigurator;
import bwnetflow.configuration.Configuration;
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
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.Logger;

import java.util.Properties;

import static bwnetflow.mptcp.aggregator.InternalTopic.AGGREGATOR_OUTPUT;


public class Application {

    private final static Logger log = Logger.getLogger(Application.class.getName());

    private final static Serde<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowsSerde =
            new KafkaProtobufSerde<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser());

    private final static  KafkaProtobufDeserializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowMessageDeserializer
            = new KafkaProtobufDeserializer<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser());

    private final static  KafkaProtobufSerializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowSerializer
            = new KafkaProtobufSerializer<>();

    public static void main(String[] args) {
        Configuration config = new CLIConfigurator().parseCLIArguments(args);

        Properties properties = config.createKafkaProperties();
        Aggregator aggregator = new Aggregator(config.getBwNetFlowflowInputTopic(),
                config.getMptcpFlowflowInputTopic(), config.getJoinWindow());

        StreamsBuilder builder = new StreamsBuilder();
        aggregator.create(builder);
        Topology topology = builder.build();

        var dedupProcessor = DeduplicationProcessor.supplier(config.getJoinWindow());

        var contributorStoreSupplier = Stores.persistentKeyValueStore("deduplication-store");

        var store = Stores.keyValueStoreBuilder(contributorStoreSupplier,
                Serdes.String(), mptcpFlowsSerde)
                .withCachingEnabled();

        topology
                .addSource("Source", new StringDeserializer(), mptcpFlowMessageDeserializer, AGGREGATOR_OUTPUT)
                .addProcessor("DeduplicationProcessor", dedupProcessor, "Source")
                .addStateStore(store, "DeduplicationProcessor")
                .addSink("Sink", config.getOutputTopic(), new StringSerializer(), mptcpFlowSerializer,"DeduplicationProcessor");

        KafkaStreams streams = new KafkaStreams(topology, properties);

        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Error occurred: ", throwable);
        });
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

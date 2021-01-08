package bwnetflow.application;

import bwnetflow.aggregator.Aggregator;
import bwnetflow.aggregator.DeduplicationProcessorNode;
import bwnetflow.configuration.Configuration;
import bwnetflow.configuration.impl.CLIConfigurator;
import bwnetflow.serdes.KafkaDeserializerFactories;
import bwnetflow.serdes.KafkaSerializerFactories;
import bwnetflow.serdes.SerdeFactories;
import bwnetflow.topology.KafkaStreamsAppBuilder;
import org.apache.log4j.Logger;

import java.util.Properties;

public class Application {

    private final static Logger log = Logger.getLogger(Application.class.getName());

    private final Configuration config;

    private Application (Configuration config) {
        this.config = config;
    }

    public static void main(String[] args) {
        setupFactories();
        Configuration config = new CLIConfigurator().parseCLIArguments(args);
        Application app = new Application(config);
        app.runKafkaStreamsApp();
    }

    private void runKafkaStreamsApp() {
        log.info("Start up Stream Topology...");
        Properties kafkaProperties = config.createKafkaProperties();
        new KafkaStreamsAppBuilder(kafkaProperties)
                .addStreamNode(createAggregator())
                .create()
                .addProcessorNode(createDeduplicationProcessorNode())
                .run();
    }

    private Aggregator createAggregator() {
        return new Aggregator(config.getBwNetFlowflowInputTopic(),
                config.getMptcpFlowflowInputTopic(), config.getJoinWindow());
    }

    private DeduplicationProcessorNode createDeduplicationProcessorNode() {
        return new DeduplicationProcessorNode(config);
    }

    private static void setupFactories() {
        SerdeFactories.setupSerdeFactories();
        KafkaSerializerFactories.setupKafkaSerializerFactories();
        KafkaDeserializerFactories.setupKafkaDeserializerFactories();
    }
}

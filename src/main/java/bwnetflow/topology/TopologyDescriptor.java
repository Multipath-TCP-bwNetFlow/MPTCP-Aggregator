package bwnetflow.topology;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Properties;
import org.apache.log4j.Logger;

public class TopologyDescriptor {

    private final static Logger log = Logger.getLogger(TopologyDescriptor.class.getName());

    private final Properties properties;
    private final Topology topology;

    public TopologyDescriptor(Properties properties, Topology topology) {
        this.properties = properties;
        this.topology = topology;
    }

    public TopologyDescriptor addProcessorNode(ProcessorNode processorNode) {
        processorNode.create(topology);
        return this;
    }

    public void run() {
        KafkaStreams  streams = new KafkaStreams(topology, properties);

        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Error occurred: ", throwable);
        });
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}

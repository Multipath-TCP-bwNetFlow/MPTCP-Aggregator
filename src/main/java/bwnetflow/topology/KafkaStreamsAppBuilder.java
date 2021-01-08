package bwnetflow.topology;

import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;
import org.apache.log4j.Logger;

public class KafkaStreamsAppBuilder {

    private final static Logger log = Logger.getLogger(KafkaStreamsAppBuilder.class.getName());

    private final StreamsBuilder streamsBuilder;
    private final Properties properties;

    public KafkaStreamsAppBuilder(Properties properties) {
        this.streamsBuilder = new StreamsBuilder();
        this.properties = properties;
    }

    public KafkaStreamsAppBuilder addStreamNode(StreamNode streamNode) {
        streamNode.create(this.streamsBuilder);
        return this;
    }

    public TopologyDescriptor create() {
        var topology = streamsBuilder.build();
        return new TopologyDescriptor(properties, topology);
    }
}

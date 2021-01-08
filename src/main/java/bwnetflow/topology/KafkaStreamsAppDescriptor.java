package bwnetflow.topology;

import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;
import org.apache.log4j.Logger;

public class KafkaStreamsAppDescriptor {

    private final static Logger log = Logger.getLogger(KafkaStreamsAppDescriptor.class.getName());

    private final StreamsBuilder streamsBuilder;
    private final Properties properties;

    public KafkaStreamsAppDescriptor(Properties properties) {
        this.streamsBuilder = new StreamsBuilder();
        this.properties = properties;
    }

    public KafkaStreamsAppDescriptor addStreamNode(StreamNode streamNode) {
        streamNode.create(this.streamsBuilder);
        return this;
    }

    public TopologyDescriptor create() {
        var topology = streamsBuilder.build();
        return new TopologyDescriptor(properties, topology);
    }
}

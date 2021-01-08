package bwnetflow.topology;

import org.apache.kafka.streams.Topology;

public interface ProcessorNode {
    void create(Topology topology);
}

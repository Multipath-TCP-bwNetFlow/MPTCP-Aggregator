package bwnetflow.topology;

import org.apache.kafka.streams.StreamsBuilder;

public interface StreamNode {
    void create(StreamsBuilder sb);
}

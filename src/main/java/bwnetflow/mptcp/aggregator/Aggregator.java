package bwnetflow.mptcp.aggregator;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import bwnetflow.serdes.proto.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.Logger;

import java.time.Duration;

public class Aggregator {

    public static final String MPTCP_TOPIC = "mptcp-packets";
    public static final String FLOWS_ENRICHED_TOPIC = "flows-enriched";
    public static final String OUTPUT_TOPIC = "mptcp-flows-joined";

    private final Logger log = Logger.getLogger(Aggregator.class.getName());

    private final Serde<MPTCPMessageProto.MPTCPMessage> mptcpMessageSerde =
            new KafkaProtobufSerde<>(MPTCPMessageProto.MPTCPMessage.parser());

    private final Serde<FlowMessageEnrichedPb.FlowMessage> enrichedFlowsSerde =
            new KafkaProtobufSerde<>(FlowMessageEnrichedPb.FlowMessage.parser());

    private final Serde<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowsSerde =
            new KafkaProtobufSerde<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser());

    private final FlowJoiner flowJoiner = new FlowJoiner();

    public Topology createAggregatorTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        var mptcpPacketsStream = builder.stream(MPTCP_TOPIC,
                Consumed.with(Serdes.String(), mptcpMessageSerde));
        var flowsEnrichedStream = builder.stream(FLOWS_ENRICHED_TOPIC,
                Consumed.with(Serdes.String(), enrichedFlowsSerde));


        // dedpulication: https://github.com/confluentinc/kafka-streams-examples/blob/6.0.1-post/src/test/java/io/confluent/examples/streams/EventDeduplicationLambdaIntegrationTest.java
        // or exactly once semantics

        /* Works only in rable rep: No gurantee that a client reads data as Tables
                        .groupByKey(Grouped.with(Serdes.String(), mptcpFlowsSerde))
                .reduce((accumulator, newValue) -> newValue.getIsMPTCPFlow() ? newValue : accumulator, // Does not work. when returned to stream representation duplicates are still there
                        Materialized.with(Serdes.String(), mptcpFlowsSerde))
                .toStream()
         */

        var mptcpStreamWithKeys = mptcpPacketsStream
                .map((k, v) -> new KeyValue<>(keyBuilderMPTCP(v), v));

        flowsEnrichedStream
                .map((k,v) -> new KeyValue<>(keyBuilderFlow(v), v))
                .leftJoin(mptcpStreamWithKeys,
                        flowJoiner::join,
                        JoinWindows.of(Duration.ofSeconds(3)),
                        StreamJoined.with(Serdes.String(), enrichedFlowsSerde, mptcpMessageSerde))
                .map((k, v) -> new KeyValue<>(v.getSrcAddr().toStringUtf8(), v))
                .peek((k,v) -> {
                    System.out.println(k);
                    System.out.println("---------");
                    System.out.println(v);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), mptcpFlowsSerde));

        return builder.build();
    }

    private String keyBuilderFlow(FlowMessageEnrichedPb.FlowMessage flowMessage) {
        String sourceAddr = flowMessage.getSrcAddr().toStringUtf8();
        String destAddr = flowMessage.getDstAddr().toStringUtf8();
        int seqNum = flowMessage.getSequenceNum();
        return String.format("%s:%s;seq=%d", sourceAddr, destAddr, seqNum);
    }

    private String keyBuilderMPTCP(MPTCPMessageProto.MPTCPMessage mptcpMessage) {
        String sourceAddr = mptcpMessage.getSrcAddr();
        String destAddr = mptcpMessage.getDstAddr();
        int seqNum = mptcpMessage.getSeqNum();
        return String.format("%s:%s;seq=%d", sourceAddr, destAddr, seqNum);
    }
}

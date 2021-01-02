package bwnetflow.mptcp.aggregator;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import bwnetflow.serdes.proto.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.log4j.Logger;

import java.time.Duration;

import static bwnetflow.mptcp.aggregator.InternalTopic.AGGREGATOR_OUTPUT;

public class Aggregator {

    private final Logger log = Logger.getLogger(Aggregator.class.getName());

    private final Serde<MPTCPMessageProto.MPTCPMessage> mptcpMessageSerde =
            new KafkaProtobufSerde<>(MPTCPMessageProto.MPTCPMessage.parser());

    private final Serde<FlowMessageEnrichedPb.FlowMessage> enrichedFlowsSerde =
            new KafkaProtobufSerde<>(FlowMessageEnrichedPb.FlowMessage.parser());

    private final Serde<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowsSerde =
            new KafkaProtobufSerde<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser());

    private final FlowJoiner flowJoiner = new FlowJoiner();

    private final String flowInputTopic;
    private final String mptcpInputTopic;
    private final int joinWindow;

    public Aggregator(String flowInputTopic, String mptcpInputTopic, int joinWindow) {
        this.flowInputTopic = flowInputTopic;
        this.mptcpInputTopic = mptcpInputTopic;
        this.joinWindow = joinWindow;
    }


    public void create(StreamsBuilder builder) {
        var mptcpPacketsStream = builder.stream(mptcpInputTopic,
                Consumed.with(Serdes.String(), mptcpMessageSerde));
        var flowsEnrichedStream = builder.stream(flowInputTopic,
                Consumed.with(Serdes.String(), enrichedFlowsSerde));

        var mptcpStreamWithKeys = mptcpPacketsStream
                .map((k, v) -> new KeyValue<>(keyBuilderMPTCP(v), v));

        flowsEnrichedStream
                .map((k,v) -> new KeyValue<>(keyBuilderFlow(v), v))
                .leftJoin(mptcpStreamWithKeys,
                        flowJoiner::join,
                        JoinWindows.of(Duration.ofSeconds(joinWindow)),
                        StreamJoined.with(Serdes.String(), enrichedFlowsSerde, mptcpMessageSerde))
                .to(AGGREGATOR_OUTPUT, Produced.with(Serdes.String(), mptcpFlowsSerde));
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

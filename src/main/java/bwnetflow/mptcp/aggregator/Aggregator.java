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

import java.net.InetAddress;
import java.net.UnknownHostException;
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

    // TODO seq num is not what was expected, we look at FLOWS. It is more like a flow counter which is incremented with each sended flow
    // TODO remove seq num in keyBuilderFlow
    // TODO work with ports instead
    public void create(StreamsBuilder builder) {
        var mptcpPacketsStream = builder.stream(mptcpInputTopic,
                Consumed.with(Serdes.String(), mptcpMessageSerde));
        var flowsEnrichedStream = builder.stream(flowInputTopic,
                Consumed.with(Serdes.String(), enrichedFlowsSerde));

        var mptcpStreamWithKeys = mptcpPacketsStream
                .map((k, v) -> new KeyValue<>(keyBuilderMPTCP(v), v));

        mptcpStreamWithKeys.peek((k,v) -> {
            System.out.println(k);
            System.out.println(v);
        });

        flowsEnrichedStream
                .map((k,v) -> new KeyValue<>(keyBuilderFlow(v), v))
                .leftJoin(mptcpStreamWithKeys,
                        flowJoiner::join,
                      //  JoinWindows.of(Duration.ofSeconds(joinWindow)),
                        JoinWindows.of(Duration.ofMinutes(3)), // TODO smaller value
                        StreamJoined.with(Serdes.String(), enrichedFlowsSerde, mptcpMessageSerde))
                .to(AGGREGATOR_OUTPUT, Produced.with(Serdes.String(), mptcpFlowsSerde));
    }

    private String keyBuilderFlow(FlowMessageEnrichedPb.FlowMessage flowMessage) {
        String sourceAddr = "N/A";
        String destAddr = "N/A";
        try {
            sourceAddr = InetAddress.getByAddress(flowMessage.getSrcAddr().toByteArray()).toString().substring(1);
            destAddr = InetAddress.getByAddress(flowMessage.getDstAddr().toByteArray()).toString().substring(1);
        } catch (UnknownHostException e) {
            log.warn("Could not convert read ip address", e);
        }
      //  int seqNum = flowMessage.getSequenceNum();
      //  return String.format("%s:%s;seq=%d", sourceAddr, destAddr, seqNum);
        return String.format("%s:%s", sourceAddr, destAddr);
    }

    private String keyBuilderMPTCP(MPTCPMessageProto.MPTCPMessage mptcpMessage) {
        String sourceAddr = mptcpMessage.getSrcAddr();
        String destAddr = mptcpMessage.getDstAddr();
       // int seqNum = mptcpMessage.getSeqNum();
       // return String.format("%s:%s;seq=%d", sourceAddr, destAddr, seqNum);
        return String.format("%s:%s", sourceAddr, destAddr);
    }
}

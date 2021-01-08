package bwnetflow.aggregator;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import bwnetflow.serdes.SerdeFactories;
import bwnetflow.topology.StreamNode;
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

import static bwnetflow.aggregator.InternalTopic.AGGREGATOR_OUTPUT;

public class Aggregator implements StreamNode {

    private final Logger log = Logger.getLogger(Aggregator.class.getName());

    private final FlowJoiner flowJoiner = new FlowJoiner();

    private final String flowInputTopic;
    private final String mptcpInputTopic;
    private final int joinWindow; // TODO use instead of hard coded value

    public Aggregator(String flowInputTopic, String mptcpInputTopic, int joinWindow) {
        this.flowInputTopic = flowInputTopic;
        this.mptcpInputTopic = mptcpInputTopic;
        this.joinWindow = joinWindow;
    }

    @Override
    public void create(StreamsBuilder builder) {
        var mptcpMessageSerde = SerdeFactories.MPTCPMessageSerdeFactory.get();
        var enrichedFlowsSerde = SerdeFactories.EnrichedFlowsSerdeFactory.get();
        var mptcpFlowsSerde = SerdeFactories.MPTCPFlowsSerdeFactory.get();

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
                      //  JoinWindows.of(Duration.ofSeconds(joinWindow)),
                        JoinWindows.of(Duration.ofMinutes(3)), // TODO smaller value
                        StreamJoined.with(Serdes.String(), enrichedFlowsSerde, mptcpMessageSerde))
                .to(AGGREGATOR_OUTPUT, Produced.with(Serdes.String(), mptcpFlowsSerde));
    }


    // seq num is not what was expected, we look at FLOWS. It is more like a flow counter which is incremented with each sended flow
    // remove seq num in keyBuilderFlow
    //  work with ports instead

    // must be passed to this class
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

    // must be passed to this class
    private String keyBuilderMPTCP(MPTCPMessageProto.MPTCPMessage mptcpMessage) {
        String sourceAddr = mptcpMessage.getSrcAddr();
        String destAddr = mptcpMessage.getDstAddr();
       // int seqNum = mptcpMessage.getSeqNum();
       // return String.format("%s:%s;seq=%d", sourceAddr, destAddr, seqNum);
        return String.format("%s:%s", sourceAddr, destAddr);
    }
}

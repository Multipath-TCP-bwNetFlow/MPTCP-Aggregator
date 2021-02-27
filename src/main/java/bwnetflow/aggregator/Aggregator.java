package bwnetflow.aggregator;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import bwnetflow.messages.prettyprinter.MessagePrettyPrinter;
import bwnetflow.serdes.SerdeFactories;
import bwnetflow.topology.StreamNode;
import com.google.protobuf.ByteString;
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
import java.util.List;

import static bwnetflow.aggregator.InternalTopic.AGGREGATOR_OUTPUT;

public class Aggregator implements StreamNode {

    private final Logger log = Logger.getLogger(Aggregator.class.getName());

    private final FlowJoiner flowJoiner = new FlowJoiner();

    private final String flowInputTopic;
    private final String mptcpInputTopic;
    private final int joinWindow;
    private final boolean logMPTCP;
    private final boolean logFlows;

    private final List<String> addressWhitelist;

    public Aggregator(String flowInputTopic, String mptcpInputTopic, int joinWindow,
                      boolean logMPTCP, boolean logFlows, List<String> addressWhitelist) {
        this.flowInputTopic = flowInputTopic;
        this.mptcpInputTopic = mptcpInputTopic;
        this.joinWindow = joinWindow;
        this.logMPTCP = logMPTCP;
        this.logFlows = logFlows;
        this.addressWhitelist = addressWhitelist;
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
                .filter((k,v) -> isInWhitelist(v.getSrcAddr(), v.getDstAddr()))
                .map((k, v) -> new KeyValue<>(keyBuilderMPTCP(v), v));

        flowsEnrichedStream
                .filter((k,v) -> isInWhitelist(v.getSrcAddr(), v.getDstAddr()))
                .map((k,v) -> new KeyValue<>(keyBuilderFlow(v), v))
                .leftJoin(mptcpStreamWithKeys,
                        flowJoiner::join,
                        JoinWindows.of(Duration.ofSeconds(joinWindow)),
                        StreamJoined.with(Serdes.String(), enrichedFlowsSerde, mptcpMessageSerde))
                .to(AGGREGATOR_OUTPUT, Produced.with(Serdes.String(), mptcpFlowsSerde));
    }

    // seq num is not what was expected, we look at FLOWS. It is more like a flow counter which is incremented with each sended flow
    // remove seq num in keyBuilderFlow
    //  work with ports instead

    private String keyBuilderFlow(FlowMessageEnrichedPb.FlowMessage flowMessage) {
        if (logFlows) {
            MessagePrettyPrinter.prettyPrint(flowMessage);
        }
        String sourceAddr = "N/A";
        String destAddr = "N/A";
        try {
            sourceAddr = InetAddress.getByAddress(flowMessage.getSrcAddr().toByteArray()).toString().substring(1);
            destAddr = InetAddress.getByAddress(flowMessage.getDstAddr().toByteArray()).toString().substring(1);
        } catch (UnknownHostException e) {
            log.warn("Could not convert read ip address", e);
        }
        String srcPort = Integer.toString(flowMessage.getSrcPort());
        String dstPort = Integer.toString(flowMessage.getDstPort());
        return String.format("%s:%s;%s:%s", sourceAddr, destAddr, srcPort, dstPort);
    }

    private String keyBuilderMPTCP(MPTCPMessageProto.MPTCPMessage mptcpMessage) {
        if (logMPTCP) {
            MessagePrettyPrinter.prettyPrint(mptcpMessage);
        }
        String sourceAddr = mptcpMessage.getSrcAddr();
        String destAddr = mptcpMessage.getDstAddr();
        String srcPort = Integer.toString(mptcpMessage.getSrcPort());
        String dstPort = Integer.toString(mptcpMessage.getDstPort());

        return String.format("%s:%s;%s:%s", sourceAddr, destAddr, srcPort, dstPort);
    }

    private boolean isInWhitelist(String src, String dst) {
        if (addressWhitelist.isEmpty()) return true;
        return addressWhitelist.contains(src) && addressWhitelist.contains(dst);
    }

    private boolean isInWhitelist(ByteString src, ByteString dst) {
        try {
            String sourceAddr = InetAddress.getByAddress(src.toByteArray()).toString().substring(1);
            String destAddr = InetAddress.getByAddress(dst.toByteArray()).toString().substring(1);
            return isInWhitelist(sourceAddr, destAddr);
        } catch (UnknownHostException e) {
            log.warn("Could not convert read ip address", e);
            return false;
        }
    }
}

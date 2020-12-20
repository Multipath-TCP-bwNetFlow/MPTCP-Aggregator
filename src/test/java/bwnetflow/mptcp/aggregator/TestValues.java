package bwnetflow.mptcp.aggregator;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import com.google.protobuf.ByteString;

public class TestValues {

    public static FlowMessageEnrichedPb.FlowMessage enrichedFlowMessage(String srcIp, String dstIp, int srcPort, int dstPort) {
        return FlowMessageEnrichedPb.FlowMessage.newBuilder()
                .setSrcAddr(ByteString.copyFromUtf8(srcIp))
                .setDstAddr(ByteString.copyFromUtf8(dstIp))
                .setSrcPort(srcPort)
                .setDstPort(dstPort)
                .build();
    }

    public static MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage mptcpFlowMessage(String srcIp, String dstIp, int srcPort, int dstPort) {
        return MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.newBuilder()
                .setSrcAddr(ByteString.copyFromUtf8(srcIp))
                .setDstAddr(ByteString.copyFromUtf8(dstIp))
                .setSrcPort(srcPort)
                .setDstPort(dstPort)
                .build();

    }
}

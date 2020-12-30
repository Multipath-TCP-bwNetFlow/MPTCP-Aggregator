package bwnetflow.mptcp.aggregator;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import com.google.protobuf.ByteString;

public class TestValues {

    public static FlowMessageEnrichedPb.FlowMessage enrichedFlowMessage(String srcIp, String dstIp, int srcPort,
                                                                        int dstPort, int seqNum) {
        return FlowMessageEnrichedPb.FlowMessage.newBuilder()
                .setSrcAddr(ByteString.copyFromUtf8(srcIp))
                .setDstAddr(ByteString.copyFromUtf8(dstIp))
                .setSrcPort(srcPort)
                .setDstPort(dstPort)
                .setSequenceNum(seqNum)
                .build();
    }

    public static MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage mptcpFlowMessage(String srcIp, String dstIp, int srcPort,
                                                                               int dstPort, int seqNum) {
        return MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.newBuilder()
                .setSrcAddr(ByteString.copyFromUtf8(srcIp))
                .setDstAddr(ByteString.copyFromUtf8(dstIp))
                .setSrcPort(srcPort)
                .setDstPort(dstPort)
                .setSequenceNum(seqNum)
                .build();

    }

    public static MPTCPMessageProto.MPTCPMessage mptcpMessage(String srcIp, String dstIp, int srcPort,
                                                              int dstPort, int seqNum) {
        return MPTCPMessageProto.MPTCPMessage.newBuilder()
                .setSrcAddr(srcIp)
                .setDstAddr(dstIp)
                .setSrcPort(srcPort)
                .setDstPort(dstPort)
                .setSeqNum(seqNum)
                .build();
    }

    public static final FlowMessageEnrichedPb.FlowMessage FLOW_MSG = TestValues.enrichedFlowMessage("111.111.111",
            "222.222.222", 1111,2222, 6000);

    public static final MPTCPMessageProto.MPTCPMessage MPTCP_MESSAGE = TestValues.mptcpMessage("111.111.111",
            "222.222.222", 1111,2222, 6000);


    public static final FlowMessageEnrichedPb.FlowMessage FLOW_MSG2 = TestValues.enrichedFlowMessage("111.111.111",
            "555.555.555", 1111,2222, 6000);

    public static final MPTCPMessageProto.MPTCPMessage MPTCP_MESSAGE2 = TestValues.mptcpMessage("111.111.111",
            "555.555.555", 1111,2222, 6000);

    public static final FlowMessageEnrichedPb.FlowMessage FLOW_MSG3 = TestValues.enrichedFlowMessage("111.111.111",
            "333.333.333", 1111,2222, 6000);
}

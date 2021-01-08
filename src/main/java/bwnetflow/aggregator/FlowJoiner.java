package bwnetflow.aggregator;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import org.apache.log4j.Logger;

public class FlowJoiner {

    private final Logger log = Logger.getLogger(FlowJoiner.class.getName());

    public MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage join(FlowMessageEnrichedPb.FlowMessage flowMessage,
                                                             MPTCPMessageProto.MPTCPMessage mptcpMessage) {
        if(mptcpMessage == null) {
            return buildWithoutMPTCP(flowMessage);
        } else if (flowMessage == null) {
            log.warn("Invalid case: flow message is NULL");
            return MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.newBuilder().build();
        } else {
            return buildJoined(flowMessage, mptcpMessage);
        }
    }

    private MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage buildWithoutMPTCP(FlowMessageEnrichedPb.FlowMessage other) {
        // TODO check if complete
        return MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.newBuilder()
                .setTypeValue(other.getTypeValue())
                .setTimeReceived(other.getTimeReceived())
                .setSequenceNum(other.getSequenceNum())
                .setSamplingRate(other.getSamplingRate())
                .setFlowDirection(other.getFlowDirection())
                .setSamplerAddress(other.getSamplerAddress())
                .setTimeFlowStart(other.getTimeFlowStart())
                .setTimeFlowEnd(other.getTimeFlowEnd())
                .setBytes(other.getBytes())
                .setPackets(other.getPackets())
                .setSrcAddr(other.getSrcAddr())
                .setDstAddr(other.getDstAddr())
                .setEtype(other.getEtype())
                .setProto(other.getProto())
                .setSrcPort(other.getSrcPort())
                .setDstPort(other.getDstPort())
                .setInIf(other.getInIf())
                .setOutIf(other.getOutIf())
                .setSrcMac(other.getSrcMac())
                .setDstMac(other.getDstMac())
                .setSrcVlan(other.getSrcVlan())
                .setDstVlan(other.getDstVlan())
                .setVlanId(other.getVlanId())
                .setIngressVrfID(other.getIngressVrfID())
                .setEgressVrfID(other.getEgressVrfID())
                .setIPTos(other.getIPTos())
                .setForwardingStatus(other.getForwardingStatus())
                .setIPTTL(other.getIPTTL())
                .setTCPFlags(other.getTCPFlags())
                .setIcmpType(other.getIcmpType())
                .setIcmpCode(other.getIcmpCode())
                .setIPv6FlowLabel(other.getIPv6FlowLabel())
                .setFragmentId(other.getFragmentId())
                .setFragmentOffset(other.getFragmentOffset())
                .setBiFlowDirection(other.getBiFlowDirection())
                .setSrcAS(other.getSrcAS())
                .setDstAS(other.getDstAS())
                .setNextHop(other.getNextHop())
                .setNextHopAS(other.getNextHopAS())
                .setSrcNet(other.getSrcNet())
                .setDstNet(other.getDstNet())
                .setHasEncap(other.getHasEncap())
                .setSrcAddrEncap(other.getSrcAddrEncap())
                .setDstAddrEncap(other.getDstAddrEncap())
                .setProtoEncap(other.getProtoEncap())
                .setEtypeEncap(other.getEtypeEncap())
                .setIPTosEncap(other.getIPTosEncap())
                .setIPTTLEncap(other.getIPTTLEncap())
                .setIPv6FlowLabelEncap(other.getIPv6FlowLabelEncap())
                .setFragmentIdEncap(other.getFragmentIdEncap())
                .setFragmentOffsetEncap(other.getFragmentOffsetEncap())
                .setHasMPLS(other.getHasMPLS())
                .setMPLSCount(other.getMPLSCount())
                .setMPLS1TTL(other.getMPLS1TTL())
                .setMPLS1Label(other.getMPLS1Label())
                .setMPLS2TTL(other.getMPLS2TTL())
                .setMPLS2Label(other.getMPLS2Label())
                .setMPLS3TTL(other.getMPLS3TTL())
                .setMPLS3Label(other.getMPLS3Label())
                .setMPLSLastTTL(other.getMPLSLastTTL())
                .setMPLSLastLabel(other.getMPLSLastLabel())
                .setHasPPP(other.getHasPPP())
                .setPPPAddressControl(other.getPPPAddressControl())
                .setCid(other.getCid())
                .setNormalizedValue(other.getNormalizedValue())
                .setSrcIfSpeed(other.getSrcIfSpeed())
                .setDstIfSpeed(other.getDstIfSpeed())
                .setIsMPTCPFlow(false)
                .build();
    }

    private MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage buildJoined(FlowMessageEnrichedPb.FlowMessage flowMessage,
                                                                    MPTCPMessageProto.MPTCPMessage mptcpMessage) {
        var withFlowMessage = buildWithoutMPTCP(flowMessage);
        return MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.newBuilder(withFlowMessage)
                .setIsMPTCPFlow(true)
                .build();
    }
}

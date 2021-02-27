package bwnetflow.messages.prettyprinter;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.stream.Collectors;

public class MessagePrettyPrinter {

    private static final String SRC_ADDR = "SrcAddr";
    private static final String DST_ADDR = "DstAddr";
    private static final String SAMPLER_ADDR = "SamplerAddress";

    public static void prettyPrint(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage message) {
        System.out.println("MPTCP flow message:");
        prettyPrint(message.getAllFields());
    }

    public static void prettyPrint(FlowMessageEnrichedPb.FlowMessage message) {
        System.out.println("flow message:");
        prettyPrint(message.getAllFields());
    }

    public static void prettyPrint(MPTCPMessageProto.MPTCPMessage message) {
        System.out.println("MPTCP message:");
        message.getAllFields()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(field -> field.getKey().getName(), Map.Entry::getValue))
                .forEach((key, value) -> System.out.println(key + ":" + value));
        System.out.println("\n");
    }

    private static void prettyPrint(Map<Descriptors.FieldDescriptor, Object> message) {
        var fields = message
                .entrySet()
                .stream()
                .collect(Collectors.toMap(field -> field.getKey().getName(), Map.Entry::getValue));

        fields.computeIfPresent(SRC_ADDR, (k, v) -> byteStringAddrToStringAddr((ByteString) v));
        fields.computeIfPresent(DST_ADDR, (k, v) -> byteStringAddrToStringAddr((ByteString) v));
        fields.computeIfPresent(SAMPLER_ADDR, (k, v) -> byteStringAddrToStringAddr((ByteString) v));

        fields.forEach((key, value) -> System.out.println(key + ":" + value));
        System.out.println("\n");
    }

    private static String byteStringAddrToStringAddr(ByteString addr) {
        try {
            return InetAddress.getByAddress(addr.toByteArray()).toString().substring(1);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return "N/A";
        }
    }
}

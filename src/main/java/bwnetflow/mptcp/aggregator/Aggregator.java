package bwnetflow.mptcp.aggregator;


import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import bwnetflow.serdes.proto.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Properties;


public class Aggregator {

    static Logger log = Logger.getLogger(Aggregator.class.getName());
    static Serde<MPTCPMessageProto.MPTCPMessage> mptcpMessageSerde =
            new KafkaProtobufSerde<>(MPTCPMessageProto.MPTCPMessage.parser());
    static Serde<FlowMessageEnrichedPb.FlowMessage> enrichedFlowsSerde =
            new KafkaProtobufSerde<>(FlowMessageEnrichedPb.FlowMessage.parser());

    public static void main(String[] args) {
        Properties properties = createProperties();
        StreamsBuilder builder = new StreamsBuilder();
        var mptcpPackets = builder.stream("mptcp-packets", Consumed.with(Serdes.String(), mptcpMessageSerde));
        var flowsEnriched = builder.stream("flows-enriched", Consumed.with(Serdes.String(), enrichedFlowsSerde));

      /*  flowsEnriched
                .peek((k, v) -> {
                    System.out.println("------" + k);
                    System.out.println(v.toString());
                });*/

    /*    mptcpPacketsToFlow(mptcpPackets)
                .toStream()
                .peek((k, v) -> {
                    System.out.println("---------" + k.key());
                    System.out.println(v.toString());
                });*/

/*
KStream<String, String> joined = left.outerJoin(right,
    (leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, // ValueJoiner
        JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
                Joined.with(
                        Serdes.String(), // key
                        Serdes.Long(),   // left value
                        Serdes.Double())  // right value
  );
 */

    KStream<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> flow =
            mptcpPackets
                    .outerJoin(flowsEnriched,
                    Aggregator::createJoined,
                    JoinWindows.of(Duration.ofSeconds(120)));



    flow.peek((k,v )-> System.out.println(v.toString()))
            .to("joined");

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.setUncaughtExceptionHandler((thread, throwable) -> {
            log.error("Error occurred: ", throwable);
        });
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static KTable<Windowed<String>, MPTCPMessageProto.MPTCPMessage> mptcpPacketsToFlow(KStream<String,
            MPTCPMessageProto.MPTCPMessage> packets) {
        return packets
                .groupBy((key, value) -> value.getSrcAddr() + /*value.getSrcPort() +*/ value.getDstAddr() /*+ value.getDstPort()*/)
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
                .aggregate(
                        () -> MPTCPMessageProto.MPTCPMessage.newBuilder().build(),
                        (key, value, accumulator) ->
                             MPTCPMessageProto.MPTCPMessage
                                .newBuilder(accumulator)
                                .setSrcPort(value.getSrcPort())
                                .setDstPort(value.getDstPort())
                                .setSrcAddr(value.getSrcAddr())
                                .setDstAddr(value.getDstAddr())
                                .addAllMptcpOptions(value.getMptcpOptionsList())
                                .build(),
                        Materialized.with(Serdes.String(), mptcpMessageSerde)
                );
    }

    private static MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage createJoined(MPTCPMessageProto.MPTCPMessage mptcpMessage,
                                                                             FlowMessageEnrichedPb.FlowMessage flowMessage) {

        log.info("CALLED joiner");
        if(mptcpMessage == null) {
            System.out.println("MPTCP message is NULL");
        } else if (flowMessage == null) {
            System.out.println("FLOW message is NULL");
        } else {
            System.out.println("NOTHING is null");
        }

        return MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.newBuilder().build();
    }

   private static Properties createProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "mptcp-aggregator-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        // TODO find most optimal properties
        return properties;
    }
}

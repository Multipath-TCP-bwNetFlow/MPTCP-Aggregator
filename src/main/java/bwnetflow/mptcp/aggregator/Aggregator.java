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


    public Topology createAggregatorTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        var mptcpPacketsStream = builder.stream(MPTCP_TOPIC,
                Consumed.with(Serdes.String(), mptcpMessageSerde));
        var flowsEnrichedStream = builder.stream(FLOWS_ENRICHED_TOPIC,
                Consumed.with(Serdes.String(), enrichedFlowsSerde));

        /*
        KStream<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> flow =
                mptcpPackets
                        .outerJoin(flowsEnriched,
                                this::createJoined,
                                JoinWindows.of(Duration.ofSeconds(120)));

        flow.peek((k,v )-> System.out.println(v.toString()))
                .to(OUTPUT_TOPIC); // TODO topic MUST be manually created before usage
        */

        // left stream- global table join looks interesting <-- global topics only recommended for almost static data
        // or inner kstream ktable join and delay kstream (flowsenriched) a bit ()if possible
        // what is better regarding performance ?
        // or left kstream kstream join with sufficient window <- Most fitting option
        //and after joining do an aggregate/merge, to merge same keys (see case G on https://www.confluent.io/blog/crossing-streams-joins-apache-kafka/)

        flowsEnrichedStream.peek((k,v) -> System.out.println(v)).to(OUTPUT_TOPIC);

/*
        flowsEnrichedStream
                .map((k,v) -> new KeyValue<>(v.getSrcAddr().toStringUtf8(), v))
                .leftJoin(mptcpPacketsStream,
                        (flows, mptcp) -> MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.newBuilder().build(),
                        JoinWindows.of(Duration.ofSeconds(10)))
                .peek((k,v) -> System.out.println(v))
                .to(OUTPUT_TOPIC);
*/

        return builder.build();
    }

    private MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage createJoined(FlowMessageEnrichedPb.FlowMessage flowMessage,
                                                                     MPTCPMessageProto.MPTCPMessage mptcpMessage) {
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


    private KTable<Windowed<String>, MPTCPMessageProto.MPTCPMessage> mptcpPacketsToFlow(KStream<String,
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

}

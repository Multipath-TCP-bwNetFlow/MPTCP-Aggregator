package bwnetflow.serdes;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import bwnetflow.serdes.proto.KafkaProtobufSerde;
import org.apache.kafka.common.serialization.Serde;

/**
 * Through usage of Java 10 local type inference (var) Aggregator is decoupled from .
 * Through changing types in this file type of references are changed at compile time.
 */
public class SerdeFactories {
    public static class MPTCPMessageSerdeFactory {
        private static Serde<MPTCPMessageProto.MPTCPMessage> serde;

        public static void set(Serde<MPTCPMessageProto.MPTCPMessage> serde) {
            MPTCPMessageSerdeFactory.serde = serde;
        }

        public static Serde<MPTCPMessageProto.MPTCPMessage> get() {
            if (serde == null) {
                throw new RuntimeException("No MPTCPMessageSerde is specified");
            }
            return serde;
        }
    }

    public static class EnrichedFlowsSerdeFactory {
        private static Serde<FlowMessageEnrichedPb.FlowMessage> serde;

        public static void set(Serde<FlowMessageEnrichedPb.FlowMessage> serde) {
            EnrichedFlowsSerdeFactory.serde = serde;
        }

        public static Serde<FlowMessageEnrichedPb.FlowMessage> get() {
            if (serde == null) {
                throw new RuntimeException("No EnrichedFlowSerde is specified");
            }
            return serde;
        }
    }

    public static class MPTCPFlowsSerdeFactory {
        private static Serde<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> serde;

        public static void set(Serde<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> serde) {
            MPTCPFlowsSerdeFactory.serde = serde;
        }

        public static Serde<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> get() {
            if (serde == null) {
                throw new RuntimeException("No MPTCPFlowSerde is specified");
            }
            return serde;
        }
    }

    public static void setupSerdeFactories() {
        MPTCPMessageSerdeFactory.set(new KafkaProtobufSerde<>(MPTCPMessageProto.MPTCPMessage.parser()));
        EnrichedFlowsSerdeFactory.set(new KafkaProtobufSerde<>(FlowMessageEnrichedPb.FlowMessage.parser()));
        MPTCPFlowsSerdeFactory.set(new KafkaProtobufSerde<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser()));
    }
}

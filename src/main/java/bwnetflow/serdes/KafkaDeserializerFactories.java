package bwnetflow.serdes;

import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.serdes.proto.KafkaProtobufDeserializer;

public class KafkaDeserializerFactories {

    public static class MPTCPFlowDeserializer {

        private static KafkaProtobufDeserializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> deserializer;

        public static void set(KafkaProtobufDeserializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> deserializer) {
            MPTCPFlowDeserializer.deserializer = deserializer;
        }

        public static KafkaProtobufDeserializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> get() {
            if (deserializer == null) {
                throw new RuntimeException("No MPTCPFlowDeserializer is specified");
            }
            return deserializer;
        }

    }

    public static void setupKafkaDeserializerFactories() {
        MPTCPFlowDeserializer.set(new KafkaProtobufDeserializer<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser()));
    }
}

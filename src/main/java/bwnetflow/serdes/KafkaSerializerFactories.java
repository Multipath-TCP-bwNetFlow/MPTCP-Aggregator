package bwnetflow.serdes;

import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.serdes.proto.KafkaProtobufSerializer;

public class KafkaSerializerFactories {

    public static class MPTCPFlowSerializer {
        private static KafkaProtobufSerializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> serializer;

        public static void set(KafkaProtobufSerializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> deserializer) {
            MPTCPFlowSerializer.serializer = deserializer;
        }

        public static KafkaProtobufSerializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> get() {
            if (serializer == null) {
                throw new RuntimeException("No MPTCPFlowSerializer is specified");
            }
            return serializer;
        }
    }

    public static void setupKafkaSerializerFactories() {
        MPTCPFlowSerializer.set(new KafkaProtobufSerializer<>());
    }
}

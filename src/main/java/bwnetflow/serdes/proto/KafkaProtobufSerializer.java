package bwnetflow.serdes.proto;

import com.google.protobuf.MessageLite;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaProtobufSerializer<T extends MessageLite> implements Serializer<T> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, T data) {
        // if (data == null) return new byte[0];
        return data.toByteArray();
    }

    @Override
    public void close() {

    }
}

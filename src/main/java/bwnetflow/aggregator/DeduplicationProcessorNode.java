package bwnetflow.aggregator;

import bwnetflow.configuration.Configuration;
import bwnetflow.serdes.KafkaDeserializerFactories;
import bwnetflow.serdes.KafkaSerializerFactories;
import bwnetflow.serdes.SerdeFactories;
import bwnetflow.topology.ProcessorNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import static bwnetflow.aggregator.InternalTopic.AGGREGATOR_OUTPUT;

public class DeduplicationProcessorNode implements ProcessorNode {

    private final Configuration configuration;

    public DeduplicationProcessorNode(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void create(Topology topology) {
        var mptcpFlowsSerde = SerdeFactories.MPTCPFlowsSerdeFactory.get();
        var mptcpFlowDeserializer = KafkaDeserializerFactories.MPTCPFlowDeserializer.get();
        var mptcpFlowSerializer = KafkaSerializerFactories.MPTCPFlowSerializer.get();

        var contributorStoreSupplier = Stores.persistentKeyValueStore("deduplication-store");
        var store = Stores.keyValueStoreBuilder(contributorStoreSupplier,
                Serdes.String(), mptcpFlowsSerde)
                .withCachingEnabled();
        var dedupProcessor = DeduplicationProcessor.supplier(configuration.getJoinWindow());

        topology
                .addSource("Source", new StringDeserializer(), mptcpFlowDeserializer, AGGREGATOR_OUTPUT)
                .addProcessor("DeduplicationProcessor", dedupProcessor, "Source")
                .addStateStore(store, "DeduplicationProcessor")
                .addSink("Sink", configuration.getOutputTopic(), new StringSerializer(), mptcpFlowSerializer,"DeduplicationProcessor");

    }
}

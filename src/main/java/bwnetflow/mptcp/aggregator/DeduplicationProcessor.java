package bwnetflow.mptcp.aggregator;

import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class DeduplicationProcessor implements Processor<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> {

    private ProcessorContext context;
    private KeyValueStore<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> store;

    @SuppressWarnings({"unchecked"})
    @Override
    public void init(ProcessorContext context) {
        this.context = context;

        this.store = (KeyValueStore<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage>) context.getStateStore("deduplication-store");

        this.context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, (timestamp -> {
            var iter = store.all();
            iter.forEachRemaining((msg) -> {
                store.delete(msg.key);
                context.forward(msg.key, msg.value);
            });
            iter.close();
            context.commit();
        }));
    }

    @Override
    public void process(String key, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage value) {
        this.store.put(key, value);
    }

    @Override
    public void close() {
        // intentionally left empty
    }
}

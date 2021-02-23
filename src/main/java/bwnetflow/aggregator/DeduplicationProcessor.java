package bwnetflow.aggregator;

import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class DeduplicationProcessor implements Processor<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> {

    private KeyValueStore<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> store;
    private final int joinWindowDuration;

    public static ProcessorSupplier<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> supplier(int joinWindowDuration) {
        return () -> new DeduplicationProcessor(joinWindowDuration);
    }

    private DeduplicationProcessor(int joinWindowDuration) {
        this.joinWindowDuration = joinWindowDuration;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void init(ProcessorContext context) {
        this.store = (KeyValueStore<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage>) context.getStateStore("deduplication-store");
        
        // PunctuationType MUST be Stream time !
        context.schedule(Duration.ofSeconds(this.joinWindowDuration), PunctuationType.WALL_CLOCK_TIME, (timestamp -> {
            var iter = store.all();
            iter.forEachRemaining((msg) -> {
                /*System.out.println(msg.key);
                System.out.println(msg.value.toString());
                System.out.println(msg.value.getIsMPTCPFlow());*/
                store.delete(msg.key);
                context.forward(msg.key, msg.value);
            });
            iter.close();
            context.commit();
        }));
    }

    @Override
    public void process(String key, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage value) {
        var inStore = this.store.get(key);
        if (inStore == null) {
            this.store.put(key, value);
            return;
        }
        if (inStore.getIsMPTCPFlow()) {
            return; // Do not overwrite mptcp flow with same redundant flow without mptcp information.
        }
        this.store.put(key, value);
    }

    @Override
    public void close() {
        // intentionally left empty
    }
}

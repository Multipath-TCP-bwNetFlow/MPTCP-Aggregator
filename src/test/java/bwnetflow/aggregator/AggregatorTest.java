package bwnetflow.aggregator;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import static bwnetflow.aggregator.TestFixtures.*;
import static bwnetflow.aggregator.InternalTopic.AGGREGATOR_OUTPUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Disabled
public class AggregatorTest {

    private TopologyTestDriver testDriver;

    private TestInputTopic<String, FlowMessageEnrichedPb.FlowMessage> flowMessageInputTopic;
    private TestInputTopic<String, MPTCPMessageProto.MPTCPMessage> mptcpMessageInputTopic;
    private TestOutputTopic<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowOutputTopic;
    private TestOutputTopic<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> finalOutput;

    @BeforeEach
    public void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        int joinWindow = 3;

        Aggregator aggregator = new Aggregator(FLOWS_ENRICHED_TOPIC, MPTCP_TOPIC, joinWindow);
        StreamsBuilder builder = new StreamsBuilder();

        aggregator.create(builder);
        Topology topology = builder.build();

        var contributorStoreSupplier = Stores.persistentKeyValueStore("deduplication-store");

        var store = Stores.keyValueStoreBuilder(contributorStoreSupplier,
                Serdes.String(), mptcpFlowsSerde)
                .withCachingEnabled();

        topology
                .addSource("Source", stringDeserializer, mptcpFlowMessageDeserializer, AGGREGATOR_OUTPUT)
                .addProcessor("DeduplicationProcessor", DeduplicationProcessor.supplier(joinWindow), "Source")
                .addStateStore(store, "DeduplicationProcessor")
                .addSink("Sink", "FOO-OUTPUT", stringSerializer, mptcpFlowSerializer,"DeduplicationProcessor");

        this.testDriver = new TopologyTestDriver(topology, config);

        initializeTestInOutTopics();
    }

    @AfterEach
    public void closeTestDriver() {
        this.testDriver.close();
    }

    @Test
    public void shouldGetResultWithoutMPTCPInformation() {
        flowMessageInputTopic.pipeInput(FLOW_MSG);
        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        var outputs = finalOutput.readValuesToList();
        assertEquals(1, outputs.size());
    }

    @Test
    public void shouldNotGetMPTCPInformationAlone() {
        mptcpMessageInputTopic.pipeInput(MPTCP_MESSAGE);
        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        var outputs = finalOutput.readValuesToList();
        assertEquals(0, outputs.size());
    }

    @Test
    public void shouldJoin() {
        flowMessageInputTopic.pipeInput(FLOW_MSG, Instant.now());
        mptcpMessageInputTopic.pipeInput(MPTCP_MESSAGE, Instant.now().plus(1, ChronoUnit.SECONDS));

        testDriver.advanceWallClockTime(Duration.ofSeconds(6));
        var outputs = finalOutput.readValuesToList();
        assertEquals(1, outputs.size());
    }

    @Test
    public void shouldJoinMultiple() {
        flowMessageInputTopic.pipeInput(FLOW_MSG, Instant.now());
        flowMessageInputTopic.pipeInput(FLOW_MSG2, Instant.now().plus(1, ChronoUnit.SECONDS));
        mptcpMessageInputTopic.pipeInput(MPTCP_MESSAGE2, Instant.now().plus(1, ChronoUnit.SECONDS));
        flowMessageInputTopic.pipeInput(FLOW_MSG3, Instant.now().plus(1, ChronoUnit.SECONDS));
        mptcpMessageInputTopic.pipeInput(MPTCP_MESSAGE, Instant.now().plus(1, ChronoUnit.SECONDS));
        testDriver.advanceWallClockTime(Duration.ofSeconds(10));
        var outputs = finalOutput.readValuesToList();
        assertEquals(3, outputs.size());
    }

    @Test
    public void shouldNotJoinBecauseNotInWindow() {
        flowMessageInputTopic.pipeInput(FLOW_MSG, Instant.now());
        mptcpMessageInputTopic.pipeInput(MPTCP_MESSAGE, Instant.now().plus(5, ChronoUnit.SECONDS));

        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        var outputs = finalOutput.readValuesToList();
        assertEquals(1, outputs.size());
        assertFalse(outputs.get(0).getIsMPTCPFlow());
    }


    private void initializeTestInOutTopics() {
        this.flowMessageInputTopic = testDriver.createInputTopic(FLOWS_ENRICHED_TOPIC,
                stringSerializer,
                flowMessageSerializer);
        this.mptcpMessageInputTopic = testDriver.createInputTopic(MPTCP_TOPIC,
                stringSerializer,
                mptcpMessageSerializer);
        this.mptcpFlowOutputTopic = testDriver.createOutputTopic(AGGREGATOR_OUTPUT,
                stringDeserializer,
                mptcpFlowMessageDeserializer);

        this.finalOutput = testDriver.createOutputTopic("FOO-OUTPUT",
                stringDeserializer,
                mptcpFlowMessageDeserializer);
    }
}

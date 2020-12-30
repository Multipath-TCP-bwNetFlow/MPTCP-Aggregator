package bwnetflow.mptcp.aggregator;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import bwnetflow.serdes.proto.KafkaProtobufDeserializer;
import bwnetflow.serdes.proto.KafkaProtobufSerde;
import bwnetflow.serdes.proto.KafkaProtobufSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import static bwnetflow.mptcp.aggregator.TestValues.*;
import static bwnetflow.mptcp.aggregator.Topics.FLOWS_ENRICHED_TOPIC;
import static bwnetflow.mptcp.aggregator.Topics.MPTCP_TOPIC;
import static bwnetflow.mptcp.aggregator.Topics.OUTPUT_TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class AggregatorTest {

    private TopologyTestDriver testDriver;

    private final StringSerializer stringSerializer = new StringSerializer();
    private final StringDeserializer stringDeserializer = new StringDeserializer();

    private final KafkaProtobufSerializer<FlowMessageEnrichedPb.FlowMessage> flowMessageSerializer
            = new KafkaProtobufSerializer<>();

    private final KafkaProtobufSerializer<MPTCPMessageProto.MPTCPMessage> mptcpMessageSerializer
            = new KafkaProtobufSerializer<>();

    private final KafkaProtobufSerializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowSerializer
            = new KafkaProtobufSerializer<>();


    private final Serde<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowsSerde =
            new KafkaProtobufSerde<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser());

    private final KafkaProtobufDeserializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowMessageDeserializer
            = new KafkaProtobufDeserializer<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser());

    private TestInputTopic<String, FlowMessageEnrichedPb.FlowMessage> flowMessageInputTopic;
    private TestInputTopic<String, MPTCPMessageProto.MPTCPMessage> mptcpMessageInputTopic;
    private TestOutputTopic<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowOutputTopic;
    private TestOutputTopic<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> finalOutput;

    @BeforeEach
    public void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        Aggregator aggregator = new Aggregator();
        StreamsBuilder builder = new StreamsBuilder();

        aggregator.create(builder);
        Topology topology = builder.build();

        var contributorStoreSupplier = Stores.persistentKeyValueStore("deduplication-store");

        var store = Stores.keyValueStoreBuilder(contributorStoreSupplier,
                Serdes.String(), mptcpFlowsSerde)
                .withCachingEnabled();

        topology
                .addSource("Source", stringDeserializer, mptcpFlowMessageDeserializer, OUTPUT_TOPIC)
                .addProcessor("DeduplicationProcessor", DeduplicationProcessor::new, "Source")
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
        this.mptcpFlowOutputTopic = testDriver.createOutputTopic(OUTPUT_TOPIC,
                stringDeserializer,
                mptcpFlowMessageDeserializer);

        this.finalOutput = testDriver.createOutputTopic("FOO-OUTPUT",
                stringDeserializer,
                mptcpFlowMessageDeserializer);
    }
}

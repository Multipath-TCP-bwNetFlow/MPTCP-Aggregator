package bwnetflow.mptcp.aggregator;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPMessageProto;
import bwnetflow.serdes.proto.KafkaProtobufDeserializer;
import bwnetflow.serdes.proto.KafkaProtobufSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;

import static bwnetflow.mptcp.aggregator.TestValues.FLOW_MSG;
import static bwnetflow.mptcp.aggregator.TestValues.MPTCP_MESSAGE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class AggregatorTest {

    private TopologyTestDriver testDriver;

    private final StringSerializer stringSerializer = new StringSerializer();
    private final StringDeserializer stringDeserializer = new StringDeserializer();

    private final KafkaProtobufSerializer<FlowMessageEnrichedPb.FlowMessage> flowMessageSerializer
            = new KafkaProtobufSerializer<>();

    private final KafkaProtobufSerializer<MPTCPMessageProto.MPTCPMessage> mptcpMessageSerializer
            = new KafkaProtobufSerializer<>();

    private final KafkaProtobufDeserializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowMessageDeserializer
            = new KafkaProtobufDeserializer<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser());

    private TestInputTopic<String, FlowMessageEnrichedPb.FlowMessage> flowMessageInputTopic;
    private TestInputTopic<String, MPTCPMessageProto.MPTCPMessage> mptcpMessageInputTopic;
    private TestOutputTopic<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowOutputTopic;

    @BeforeEach
    public void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        Aggregator aggregator = new Aggregator();
        Topology topology = aggregator.createAggregatorTopology();

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
        var outputs = mptcpFlowOutputTopic.readValuesToList();
        assertEquals(1, outputs.size());
    }

    @Test
    public void shouldNotGetMPTCPInformationAlone() {
        mptcpMessageInputTopic.pipeInput(MPTCP_MESSAGE);
        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        var outputs = mptcpFlowOutputTopic.readValuesToList();
        assertEquals(0, outputs.size());
    }

    @Test
    public void shouldJoin() {
        flowMessageInputTopic.pipeInput(FLOW_MSG, Instant.now());
        mptcpMessageInputTopic.pipeInput(MPTCP_MESSAGE, Instant.now().plus(1, ChronoUnit.SECONDS));

        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        // first is MPTCP null, second is joined
        var outputs = mptcpFlowOutputTopic.readValuesToList();
        assertEquals(2, outputs.size());
    }

    @Test
    public void shouldJoin2() {
        flowMessageInputTopic.pipeInput(FLOW_MSG, Instant.now());
        flowMessageInputTopic.pipeInput(FLOW_MSG, Instant.now());
        mptcpMessageInputTopic.pipeInput(MPTCP_MESSAGE, Instant.now().plus(1, ChronoUnit.SECONDS));
        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        // first is MPTCP null, second is joined
        var outputs = mptcpFlowOutputTopic.readValuesToList();
        assertEquals(2, outputs.size());
    }

    @Test
    public void shouldNotJoinBecauseNotInWindow() {
        flowMessageInputTopic.pipeInput(FLOW_MSG, Instant.now());
        mptcpMessageInputTopic.pipeInput(MPTCP_MESSAGE, Instant.now().plus(5, ChronoUnit.SECONDS));

        testDriver.advanceWallClockTime(Duration.ofSeconds(60));
        var outputs = mptcpFlowOutputTopic.readValuesToList();
        assertEquals(1, outputs.size());
        assertFalse(outputs.get(0).getIsMPTCPFlow());
    }


    private void initializeTestInOutTopics() {
        this.flowMessageInputTopic = testDriver.createInputTopic(Aggregator.FLOWS_ENRICHED_TOPIC,
                stringSerializer,
                flowMessageSerializer);
        this.mptcpMessageInputTopic = testDriver.createInputTopic(Aggregator.MPTCP_TOPIC,
                stringSerializer,
                mptcpMessageSerializer);
        this.mptcpFlowOutputTopic = testDriver.createOutputTopic(Aggregator.OUTPUT_TOPIC,
                stringDeserializer,
                mptcpFlowMessageDeserializer);
    }
}

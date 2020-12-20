package bwnetflow.mptcp.aggregator;

import bwnetflow.messages.FlowMessageEnrichedPb;
import bwnetflow.messages.MPTCPFlowMessageEnrichedPb;
import bwnetflow.serdes.proto.KafkaProtobufDeserializer;
import bwnetflow.serdes.proto.KafkaProtobufSerializer;
import org.apache.kafka.common.serialization.Serdes;
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

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AggregatorTest {

    private TopologyTestDriver testDriver;

    private final StringSerializer stringSerializer = new StringSerializer();
    private final StringDeserializer stringDeserializer = new StringDeserializer();

    private final KafkaProtobufSerializer<FlowMessageEnrichedPb.FlowMessage> flowMessageSerializer
            = new KafkaProtobufSerializer<>();

    private final KafkaProtobufDeserializer<MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowMessageDeserializer
            = new KafkaProtobufDeserializer<>(MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage.parser());

    private TestInputTopic<String, FlowMessageEnrichedPb.FlowMessage> flowMessageInputTopic;
    private TestOutputTopic<String, MPTCPFlowMessageEnrichedPb.MPTCPFlowMessage> mptcpFlowOutputTopic;

    @BeforeEach
    public void setUp() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        Aggregator aggregator = new Aggregator();
        Topology topology = aggregator.createAggregatorTopology();

        this.testDriver = new TopologyTestDriver(topology, config);
        this.flowMessageInputTopic = testDriver.createInputTopic(Aggregator.FLOWS_ENRICHED_TOPIC, stringSerializer, flowMessageSerializer);
        this.mptcpFlowOutputTopic = testDriver.createOutputTopic(Aggregator.OUTPUT_TOPIC, stringDeserializer, mptcpFlowMessageDeserializer);
    }

    @AfterEach
    public void closeTestDriver() {
        this.testDriver.close();
    }

    @Test
    public void shouldGetResultWithoutMPTCPInformation() {
        var msg = TestValues.enrichedFlowMessage("111.111.111",
                "222.222.222", 1111,2222);

        flowMessageInputTopic.pipeInput(msg);

        var outputs = mptcpFlowOutputTopic.readValuesToList();

        assertEquals(1, outputs.size());

    }


}

package bwnetflow.configuration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class Configuration {
    private final String kafkaBrokerAddress;
    private final String bwNetFlowflowInputTopic;
    private final String mptcpFlowflowInputTopic;
    private final String outputTopic;
    private final int joinWindow;

    public Configuration(String kafkaBrokerAddress, String bwNetFlowflowInputTopic, String mptcpFlowflowInputTopic,String outputTopic, int joinWindow) {
        this.kafkaBrokerAddress = kafkaBrokerAddress;
        this.bwNetFlowflowInputTopic = bwNetFlowflowInputTopic;
        this.mptcpFlowflowInputTopic = mptcpFlowflowInputTopic;
        this.outputTopic = outputTopic;
        this.joinWindow = joinWindow;
    }

    public String getKafkaBrokerAddress() {
        return kafkaBrokerAddress;
    }

    public String getBwNetFlowflowInputTopic() {
        return bwNetFlowflowInputTopic;
    }

    public String getMptcpFlowflowInputTopic() {
        return mptcpFlowflowInputTopic;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public int getJoinWindow() {
        return joinWindow;
    }

    public Properties createKafkaProperties() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "mptcp-aggregator-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokerAddress); // "localhost:9092"
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        // properties.put(StreamsConfig.STATE_DIR_CONFIG, "/state");
        return properties;
    }
}

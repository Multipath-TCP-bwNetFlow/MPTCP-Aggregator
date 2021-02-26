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
    private final boolean logMPTCP;
    private final boolean logFlows;
    private final boolean logJoined;

    public Configuration(String kafkaBrokerAddress, String bwNetFlowflowInputTopic,
                         String mptcpFlowflowInputTopic, String outputTopic, int joinWindow,
                         boolean logMPTCP, boolean logFlows, boolean logJoined) {
        this.kafkaBrokerAddress = kafkaBrokerAddress;
        this.bwNetFlowflowInputTopic = bwNetFlowflowInputTopic;
        this.mptcpFlowflowInputTopic = mptcpFlowflowInputTopic;
        this.outputTopic = outputTopic;
        this.joinWindow = joinWindow;
        this.logMPTCP = logMPTCP;
        this.logFlows = logFlows;
        this.logJoined = logJoined;
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

    public boolean isLogMPTCP() {
        return logMPTCP;
    }

    public boolean isLogFlows() {
        return logFlows;
    }

    public boolean isLogJoined() {
        return logJoined;
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

    @Override
    public String toString() {
        return "Configuration{" +
                "kafkaBrokerAddress='" + kafkaBrokerAddress + '\'' +
                ", bwNetFlowflowInputTopic='" + bwNetFlowflowInputTopic + '\'' +
                ", mptcpFlowflowInputTopic='" + mptcpFlowflowInputTopic + '\'' +
                ", outputTopic='" + outputTopic + '\'' +
                ", joinWindow=" + joinWindow +
                '}';
    }
}

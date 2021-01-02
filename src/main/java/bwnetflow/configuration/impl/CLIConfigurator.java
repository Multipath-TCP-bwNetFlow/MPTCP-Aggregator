package bwnetflow.configuration.impl;

import bwnetflow.configuration.Configuration;
import bwnetflow.configuration.Configurator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CLIConfigurator implements Configurator {

    @Override
    public Configuration parseCLIArguments(String[] args) {
        Options options = new Options();

        Option kafkaServerOption = new Option("k", "kafkaBrokerAddress", true, "kafka broker ip address");
        kafkaServerOption.setRequired(true);
        options.addOption(kafkaServerOption);

        Option bwNetFlowInputOption = new Option("f", "bwNetFlowInputTopic", true, "kafka input topic for bwNetFlow flows");
        bwNetFlowInputOption.setRequired(true);
        options.addOption(bwNetFlowInputOption);

        Option mptcpFlowInputOption = new Option("m", "mptcpFlowInputTopic", true, "kafka input topic for mptcp flows");
        mptcpFlowInputOption.setRequired(true);
        options.addOption(mptcpFlowInputOption);

        Option outputOption = new Option("o", "outputTopic", true, "kafka ouptut topic");
        outputOption.setRequired(true);
        options.addOption(outputOption);

        Option joinWindowOption = new Option("w", "joinWindow", true, "time in seconds for join window");
        joinWindowOption.setRequired(true);
        joinWindowOption.setType(Integer.class);
        options.addOption(joinWindowOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);
            String kafkaBrokerAddress = cmd.getOptionValue("kafkaBrokerAddress");
            String bwNetFlowInputTopic = cmd.getOptionValue("bwNetFlowInputTopic");
            String mptcpFlowInputTopic = cmd.getOptionValue("mptcpFlowInputTopic");
            String outputTopic = cmd.getOptionValue("outputTopic");
            Integer joinWindow = (Integer) cmd.getParsedOptionValue("joinWindow");
            return new Configuration(kafkaBrokerAddress, bwNetFlowInputTopic, mptcpFlowInputTopic,
                    outputTopic, joinWindow);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("mptcp_aggregator", options);
            System.exit(1);
        }
        return null;
    }

}

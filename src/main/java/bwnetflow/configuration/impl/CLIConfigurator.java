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
import org.apache.log4j.Logger;

public class CLIConfigurator implements Configurator {

    private final static Logger log = Logger.getLogger(CLIConfigurator.class.getName());

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

        Option logMPTCPOption = new Option("lm", "logMPTCP", false, "log incoming MPTCP packets");
        logMPTCPOption.setRequired(false);
        logMPTCPOption.setType(Boolean.class);
        options.addOption(logMPTCPOption);

        Option logFlowPOption = new Option("lf", "logFlows", false, "log incoming flows");
        logFlowPOption.setRequired(false);
        logFlowPOption.setType(Boolean.class);
        options.addOption(logFlowPOption);

        Option logJoinedPOption = new Option("lj", "logJoined", false, "log joined packets");
        logJoinedPOption.setRequired(false);
        logJoinedPOption.setType(Boolean.class);
        options.addOption(logJoinedPOption);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);
            String kafkaBrokerAddress = cmd.getOptionValue("kafkaBrokerAddress");
            String bwNetFlowInputTopic = cmd.getOptionValue("bwNetFlowInputTopic");
            String mptcpFlowInputTopic = cmd.getOptionValue("mptcpFlowInputTopic");
            String outputTopic = cmd.getOptionValue("outputTopic");
            int joinWindow = Integer.parseInt(cmd.getOptionValue("joinWindow"));

            boolean logMPTCP = cmd.hasOption("lm");
            boolean logFlow = cmd.hasOption("lf");
            boolean logJoined = cmd.hasOption("lj");

            Configuration config =  new Configuration(kafkaBrokerAddress,
                    bwNetFlowInputTopic, mptcpFlowInputTopic,
                    outputTopic, joinWindow, logMPTCP, logFlow, logJoined);

            logArguments(config);
            return config;
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("mptcp_aggregator", options);
            System.exit(1);
        } catch (NumberFormatException e) {
            System.out.println("joinWindow must be an integer");
            System.exit(1);
        }
        return null;
    }

    private void logArguments(Configuration config) {
        log.info(config.toString());
    }

}

# MPTCP Aggregator

Joins Multipath TCP Flows from MPTCP Sniffer and Flows from bwNetFlow.
Outputs joined flow in configurable topic.

## Usage

| Flag                      | Description |
| --------------------------| ----------- |
| kafkaBrokerAddress        | kafka broker ip address                                                                    |
| bwNetFlowInputTopic       | kafka input topic for bwNetFlow flows                                                      |
| mptcpFlowInputTopic       | kafka input topic for mptcp flows                                                          |
| outputTopic               | kafka ouptut topic                                                                         |
| joinWindow                | time in seconds for join window                                                            |
| logMPTCP                  | log incoming MPTCP packets                                                                 |
| logFlows                  | log incoming flows                                                                         |
| logJoined                 | log joined packets                                                                         |
| addressWhitelist          | filter out all flows with addresses not in this list. Use a comma separated string         |


MPTCP Aggregator needs an intermediary topic: 

    mptcp-bwnetflow-aggregator-output

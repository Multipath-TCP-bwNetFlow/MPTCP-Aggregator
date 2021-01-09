# Build container
FROM maven:3.6.3-openjdk-16 AS builder
COPY ./pom.xml ./pom.xml
COPY ./src ./src
RUN mvn clean package

# Execution container
FROM openjdk:16
COPY --from=builder /target/mptcp_aggregator-1.0-SNAPSHOT-jar-with-dependencies.jar mptcp_aggregator.jar
ENTRYPOINT java -jar mptcp_aggregator.jar -f ${BWNETFLOW_INPUT_TOPIC} \
    -k ${KAFKA_ADDRESS} \
    -m ${MPTCP_FLOW_INPUT_TOPIC}  \
    -o ${OUTPUT_TOPIC} \
    -w ${JOIN_WINDOW_TIME}

FROM quay.io/debezium/connect:2.7
USER root
RUN mkdir -p /kafka/connect/mongodb
COPY mongo-kafka-connect-1.13.0-all.jar /kafka/connect/mongodb/
RUN chown -R kafka:kafka /kafka/connect/mongodb
USER kafka

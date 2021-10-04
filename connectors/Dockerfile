FROM confluentinc/cp-server-connect-operator:6.1.0.0 AS base

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-azure-blob-storage:1.6.2 \
    && confluent-hub install --no-prompt confluentinc/kafka-connect-azure-blob-storage-source:1.4.5

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-avro-converter:6.2.0


USER user
// Copyright (c) 2025 Skyflow, Inc.

package util;

public interface Constants {
  String DEFAULT_PROPERTY_FILE = "template.properties";
  String PROPERTY_SPARK_LOG_LEVEL = "log.level";
  String PROPERTY_KAFKA_AWAIT_TERMINATION_TIMEOUT = "kafka.await.termination.timeout.ms";
  String PROPERTY_KAFKA_MESSAGE_FORMAT = "kafka.message.format";

  String PROPERTY_KAFKA_BOOTSTRAP_SERVERS = "kafka.bootstrap.servers";
  String PROPERTY_KAFKA_TOPIC = "kafka.topic";
  String PROPERTY_KAFKA_OUTPUT_TOPIC_NAME = "kafka.output.topic.name";
  String PROPERTY_KAFKA_STARTING_OFFSET = "kafka.starting.offset";
  String PROPERTY_KAFKA_SCHEMA_URL = "kafka.schema.url";

  String PROPERTY_SKYFLOW_VAULT_URL = "skyflow.vault.url";
  String PROPERTY_SKYFLOW_VAULT_ID = "skyflow.vault.id";
  String PROPERTY_SKYFLOW_VAULT_SERVICE_ACCOUNT = "skyflow.vault.sa";
  String PROPERTY_SKYFLOW_TABLE_NAME = "skyflow.vault.table.name";
  String PROPERTY_SKYFLOW_INSERT_BATCH_SIZE = "skyflow.insert.batch.size";
}

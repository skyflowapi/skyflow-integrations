// Copyright (c) 2025 Skyflow, Inc.

package kafka;

import static util.Constants.PROPERTY_KAFKA_AWAIT_TERMINATION_TIMEOUT;
import static util.Constants.PROPERTY_KAFKA_BOOTSTRAP_SERVERS;
import static util.Constants.PROPERTY_KAFKA_MESSAGE_FORMAT;
import static util.Constants.PROPERTY_KAFKA_OUTPUT_TOPIC_NAME;
import static util.Constants.PROPERTY_KAFKA_SCHEMA_URL;
import static util.Constants.PROPERTY_KAFKA_STARTING_OFFSET;
import static util.Constants.PROPERTY_KAFKA_TOPIC;
import static util.Constants.PROPERTY_SKYFLOW_INSERT_BATCH_SIZE;
import static util.Constants.PROPERTY_SKYFLOW_TABLE_NAME;
import static util.Constants.PROPERTY_SKYFLOW_VAULT_ID;
import static util.Constants.PROPERTY_SKYFLOW_VAULT_SERVICE_ACCOUNT;
import static util.Constants.PROPERTY_SKYFLOW_VAULT_URL;
import static util.Constants.PROPERTY_SPARK_LOG_LEVEL;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import skyflow.core.ObjectMappers;
import skyflow.types.V1InsertResponse;
import util.PropertyUtil;
import util.SecretManagerReader;

public class KafkaToVault {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToVault.class);
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private String kafkaMessageFormat;
  private String kafkaSchemaUrl;
  private String kafkaStartingOffsets;
  private String kafkaOutputTopicName;
  private Long kafkaAwaitTerminationTimeout;
  private String vaultUrl;
  private String vaultId;
  private String vaultCredentials;
  private String tableName;
  private int batchSize;
  private final String sparkLogLevel;

  public KafkaToVault() {

    kafkaBootstrapServers = getProperties().getProperty(PROPERTY_KAFKA_BOOTSTRAP_SERVERS);
    kafkaTopic = getProperties().getProperty(PROPERTY_KAFKA_TOPIC);
    kafkaMessageFormat = getProperties().getProperty(PROPERTY_KAFKA_MESSAGE_FORMAT);
    kafkaSchemaUrl = getProperties().getProperty(PROPERTY_KAFKA_SCHEMA_URL);
    kafkaStartingOffsets = getProperties().getProperty(PROPERTY_KAFKA_STARTING_OFFSET);
    kafkaOutputTopicName = getProperties().getProperty(PROPERTY_KAFKA_OUTPUT_TOPIC_NAME);
    kafkaAwaitTerminationTimeout =
        Long.valueOf(getProperties().getProperty(PROPERTY_KAFKA_AWAIT_TERMINATION_TIMEOUT));
    sparkLogLevel = getProperties().getProperty(PROPERTY_SPARK_LOG_LEVEL);
    vaultUrl = getProperties().getProperty(PROPERTY_SKYFLOW_VAULT_URL);
    vaultId = getProperties().getProperty(PROPERTY_SKYFLOW_VAULT_ID);
    try {
      vaultCredentials =
          SecretManagerReader.readCredentials(
              getProperties().getProperty(PROPERTY_SKYFLOW_VAULT_SERVICE_ACCOUNT));
    } catch (Exception e) {
      LOGGER.error("Failed to read vault credentials from Secret Manager", e);
      throw new RuntimeException(
          "Could not initialize KafkaToVault due to missing credentials.", e);
    }
    tableName = getProperties().getProperty(PROPERTY_SKYFLOW_TABLE_NAME);
    batchSize =
        Integer.valueOf(getProperties().getProperty(PROPERTY_SKYFLOW_INSERT_BATCH_SIZE, "100"));
  }

  public void runTemplate() throws TimeoutException, StreamingQueryException {
    // Initialize Spark session
    SparkSession spark = SparkSession.builder().appName("KafkaToVault:Java").getOrCreate();

    // Set log level
    spark.sparkContext().setLogLevel(sparkLogLevel);

    KafkaReader reader = new KafkaReader();

    LOGGER.info("Calling Kafka Reader");

    Dataset<Row> processedData = reader.readKafkaTopic(spark, getProperties());

    final InsertHelper insertHelper =
        new InsertHelper(vaultUrl, vaultId, vaultCredentials, tableName);

    final String localKafkaOutputTopicName = kafkaOutputTopicName;
    final int localBatchSize = batchSize;
    final Properties localProperties = getProperties();

    processedData
        .writeStream()
        .foreachBatch(
            (batchDF, batchId) -> {
              LOGGER.info("Received batch with ID: {}", batchId);

              // No explode required
              Dataset<Row> recordsDF = batchDF.select("record");
              recordsDF.persist(StorageLevel.MEMORY_AND_DISK());

              LOGGER.info("Batch schema: {}", recordsDF.schema().treeString());
              LOGGER.info("Batch count: {}", recordsDF.count());

              recordsDF.foreachPartition(
                  iterator -> {
                    final int BATCH_SIZE = localBatchSize;
                    List<Row> batch = new ArrayList<>(BATCH_SIZE);
                    while (iterator.hasNext()) {
                      batch.clear();
                      // Fill the batch
                      int count = 0;
                      while (iterator.hasNext() && count < BATCH_SIZE) {
                        batch.add(iterator.next());
                        count++;
                      }
                      if (!batch.isEmpty()) {
                        LOGGER.info("Processing batch of {} rows in partition", batch.size());

                        CompletableFuture<V1InsertResponse> future =
                            insertHelper.insertAsync(new ArrayList<>(batch));

                        future
                            .thenAccept(
                                insertResponse -> {
                                  if (insertResponse != null
                                      && insertResponse.getRecords().isPresent()) {
                                    try {
                                      String respJson =
                                          ObjectMappers.JSON_MAPPER.writeValueAsString(
                                              insertResponse.getRecords());
                                      LOGGER.info(
                                          "Insert successful; response records: {}", respJson);
                                    } catch (JsonProcessingException e) {
                                      LOGGER.error(
                                          "Failed to serialize insert response records", e);
                                    }
                                    List<Map<String, Object>> kafkaEvents =
                                        TokenMapper.extractKafkaEventsFromInsertResponse(
                                            insertResponse);
                                    LOGGER.info("Kafka Events Size: {}", kafkaEvents.size());
                                    if (!kafkaEvents.isEmpty()) {
                                      LOGGER.info(
                                          "Creating publisher for {} events", kafkaEvents.size());
                                      KafkaPublisher publisher =
                                          new KafkaPublisher(
                                              localKafkaOutputTopicName, localProperties);
                                      publisher.publish(kafkaEvents);
                                    }
                                  }
                                })
                            .exceptionally(
                                ex -> {
                                  LOGGER.error(
                                      "Insert or publish failed for batch {}", ex.toString());
                                  return null;
                                });
                      }
                    }
                    if (!iterator.hasNext() && batch.isEmpty()) {
                      LOGGER.info("Empty partition encountered — skipping.");
                    }
                  });

              recordsDF.unpersist();
            })
        .start()
        .awaitTermination(kafkaAwaitTerminationTimeout);

    LOGGER.info("KafkaToVault job completed.");
    spark.stop();
  }

  Properties getProperties() {
    return PropertyUtil.getProperties();
  }

  public void validateInput() {
    if (StringUtils.isAllBlank(kafkaBootstrapServers)
        || StringUtils.isAllBlank(kafkaTopic)
        || StringUtils.isAllBlank(kafkaMessageFormat)) {
      LOGGER.error(
          "{},{},{} is required parameter. ",
          PROPERTY_KAFKA_BOOTSTRAP_SERVERS,
          PROPERTY_KAFKA_TOPIC,
          PROPERTY_KAFKA_MESSAGE_FORMAT);
      throw new IllegalArgumentException(
          "Required parameters for KafkaToVault not passed. "
              + "Set mandatory parameter for KafkaToVault "
              + "in resources/conf/template.properties file.");
    }

    if (kafkaMessageFormat.equals("json") & StringUtils.isAllBlank(kafkaSchemaUrl)) {
      LOGGER.error(
          "{} is a required parameter for JSON format messages", PROPERTY_KAFKA_SCHEMA_URL);
      throw new IllegalArgumentException("Required parameters for KafkaToVault not passed.");
    }

    SparkSession spark = null;
    LOGGER.info(
        "Starting Kafka to Vault spark job with following parameters:"
            + "1. {}:{}"
            + "2. {}:{}"
            + "3. {}:{}"
            + "4. {},{}",
        PROPERTY_KAFKA_BOOTSTRAP_SERVERS,
        kafkaBootstrapServers,
        PROPERTY_KAFKA_TOPIC,
        kafkaTopic,
        PROPERTY_KAFKA_STARTING_OFFSET,
        kafkaStartingOffsets,
        PROPERTY_KAFKA_AWAIT_TERMINATION_TIMEOUT,
        kafkaAwaitTerminationTimeout);
  }
}

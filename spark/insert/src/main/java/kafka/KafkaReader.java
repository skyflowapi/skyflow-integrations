// Copyright (c) 2025 Skyflow, Inc.

package kafka;

import static util.Constants.PROPERTY_KAFKA_BOOTSTRAP_SERVERS;
import static util.Constants.PROPERTY_KAFKA_MESSAGE_FORMAT;
import static util.Constants.PROPERTY_KAFKA_SCHEMA_URL;
import static util.Constants.PROPERTY_KAFKA_STARTING_OFFSET;
import static util.Constants.PROPERTY_KAFKA_TOPIC;

import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ReadSchemaUtil;

public class KafkaReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReader.class);

  private String kakfaMessageFormat;
  private String kafkaBootstrapServers;
  private String kafkaTopic;
  private String kafkaStartingOffsets;
  private String kafkaSchemaUrl;

  /**
   * Reads data from a Kafka topic using Spark Structured Streaming. This method configures and
   * establishes a secure connection to Kafka using OAuth bearer token authentication, and reads
   * messages from the specified topic.
   *
   * @param spark The SparkSession instance used for creating the streaming connection
   * @param prop Properties containing Kafka configuration parameters including: - bootstrap servers
   *     - topic name - message format - starting offsets - schema URL
   * @return Dataset<Row> A streaming Dataset containing the messages read from Kafka, processed
   *     according to the specified message format
   */
  public Dataset<Row> readKafkaTopic(SparkSession spark, Properties prop) {

    kafkaBootstrapServers = prop.getProperty(PROPERTY_KAFKA_BOOTSTRAP_SERVERS);
    kafkaTopic = prop.getProperty(PROPERTY_KAFKA_TOPIC);
    kakfaMessageFormat = prop.getProperty(PROPERTY_KAFKA_MESSAGE_FORMAT);
    kafkaStartingOffsets = prop.getProperty(PROPERTY_KAFKA_STARTING_OFFSET);
    kafkaSchemaUrl = prop.getProperty(PROPERTY_KAFKA_SCHEMA_URL);

    Dataset<Row> inputData =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)
            .option("subscribe", kafkaTopic)
            .option("startingOffsets", kafkaStartingOffsets)
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism", "OAUTHBEARER")
            .option(
                "kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;")
            .option(
                "kafka.sasl.login.callback.handler.class",
                "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
            .option("failOnDataLoss", "false")
            .load();

    // check on memory constraints

    return getDatasetByMessageFormat(inputData, prop);
  }

  /**
   * Determines how to process the Kafka messages based on the message format specified in
   * properties. Supports processing of both bytes and JSON formatted messages.
   *
   * @param inputData The raw Dataset<Row> containing Kafka messages
   * @param prop Properties containing message format and schema configuration
   * @return Dataset<Row> Processed dataset according to the specified message format
   */
  public Dataset<Row> getDatasetByMessageFormat(Dataset<Row> inputData, Properties prop) {
    Dataset<Row> processedData = null;
    String kakfaMessageFormat = prop.getProperty(PROPERTY_KAFKA_MESSAGE_FORMAT);
    String kafkaSchemaUrl = prop.getProperty(PROPERTY_KAFKA_SCHEMA_URL);

    switch (kakfaMessageFormat) {
      case "bytes":
        processedData = processBytesMessage(inputData);
        break;

      case "json":
        StructType schema = ReadSchemaUtil.readSchema(kafkaSchemaUrl);
        processedData = processJsonMessage(inputData, schema);
        break;
    }

    return processedData;
  }

  /**
   * Processes JSON formatted Kafka messages by parsing them according to the provided schema.
   * Converts the JSON string value into structured columns based on the schema.
   *
   * @param df The input Dataset<Row> containing JSON formatted messages
   * @param schema The StructType schema defining the JSON structure
   * @return Dataset<Row> Processed dataset with JSON fields extracted into columns
   */
  public Dataset<Row> processJsonMessage(Dataset<Row> df, StructType schema) {

    Dataset<Row> processedDF =
        df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .select(
                functions.col("key"),
                functions.from_json(functions.col("value"), schema).alias("record"));

    return processedDF;
  }

  /**
   * Processes byte array Kafka messages by converting them to string format. Converts both key and
   * value columns from bytes to strings.
   *
   * @param df The input Dataset<Row> containing byte array messages
   * @return Dataset<Row> Processed dataset with key and value as strings
   */
  public Dataset<Row> processBytesMessage(Dataset<Row> df) {

    Dataset<Row> processedDF =
        df.withColumn("key", functions.col("key").cast(DataTypes.StringType))
            .withColumn("value", functions.col("value").cast(DataTypes.StringType));

    return processedDF;
  }
}

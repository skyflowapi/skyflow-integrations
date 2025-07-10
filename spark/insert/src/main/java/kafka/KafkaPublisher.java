// Copyright (c) 2025 Skyflow, Inc.

package kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaPublisher {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPublisher.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private final String topic;
  private final Properties kafkaProps;

  public KafkaPublisher(String topic, Properties kafkaProps) {
    this.topic = topic;
    this.kafkaProps = kafkaProps;
  }

  public void publish(List<Map<String, Object>> events) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", kafkaProps.getProperty("kafka.bootstrap.servers"));
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("security.protocol", "SASL_SSL");
    properties.put("sasl.mechanism", "OAUTHBEARER");
    properties.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;");
    properties.put(
        "sasl.login.callback.handler.class",
        "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler");

    Producer<String, String> producer = new KafkaProducer<>(properties);
    try {
      LOGGER.info("Publishing {} event(s) to topic {}", events.size(), topic);
      for (Map<String, Object> event : events) {
        String message = OBJECT_MAPPER.writeValueAsString(event);
        ProducerRecord<String, String> record =
            new ProducerRecord<>(topic, event.get("skyflowID").toString(), message);
        RecordMetadata metadata = producer.send(record).get();
        LOGGER.info(
            "Published Kafka event: topic={}, partition={}, offset={}",
            metadata.topic(),
            metadata.partition(),
            metadata.offset());
      }
    } catch (Exception e) {
      LOGGER.error("Failed to publish events to Kafka", e);
    } finally {
      producer.close();
      LOGGER.info("Kafka producer closed.");
    }
  }
}

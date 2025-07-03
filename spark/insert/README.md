# Skyflow Vault Insert Integration for Spark

This integration enables the consumption of events from Kafka and insertion into a Skyflow Vault. Tokenized events are published to a separate Kafka topic, which can be consumed by other applications or services. The integration is designed to work with Apache Spark and Google Cloud Dataproc, allowing for scalable processing of large datasets.
On its current form, the README outlines the steps to set up and run the integration, including its planned configurations and properties.


---

## ⚙️ Spark Job Submission

start.sh encapsulates job submission to Google Cloud Dataproc. The script takes various parameters, including project ID, Kafka configuration, and Skyflow Vault details.

#### ✅ General Execution

```bash
GCP_PROJECT=<gcp-project-id>
REGION=<region>
SUBNET=<subnet>
JOB_TYPE=<cluster|serverless>
CLUSTER=<your-data-proc-cluster>
GCS_STAGING_LOCATION=<gcs-directory-to-store-build-jar>
SPARK_SCHEMA_FILE=<gcs-spark-schema-link>
SKYFLOW_CREDENTIALS=<gcs-credentials-json-link>

scripts/start.sh \
-- --templateProperty project.id=$GCP_PROJECT \
--templateProperty kafka.bootstrap.servers=<kafka broker list> \
--templateProperty kafka.topic=<kafka topic name> \
--templateProperty kafka.output.topic.name=<kafka output topic name> \
--templateProperty kafka.starting.offset=<kafka-offset> \
--templateProperty kafka.message.format=<kafka message format> \
--templateProperty kafka.schema.url=<kafka-topic-schema-file> \
--templateProperty kafka.await.termination.timeout.ms=<dataproc-job-timeout> \
--templateProperty skyflow.vault.id=<skyflow vault id> \
--templateProperty skyflow.vault.table.name=<skyflow table name> \
--templateProperty skyflow.vault.url=<skyflow vault url> \
--templateProperty skyflow.vault.sa=$SKYFLOW_CREDENTIALS \
--templateProperty skyflow.insert.batch.size=<kafka output topic name>
```

---

#### 💡 Example Submission

```bash
export GCP_PROJECT=gcp-playground1-uw1
export REGION=us-central1
export SUBNET=default
export JOB_TYPE=CLUSTER
export CLUSTER=vault-spark-cluster
export GCS_STAGING_LOCATION=gs://gcp-playground1-uw1-dataproc-scripts/stage
export SPARK_SCHEMA_FILE=gs://gcp-playground1-uw1-dataproc-scripts/schema/table1.json
export SKYFLOW_CREDENTIALS=projects/999999999999/secrets/vault-creds

scripts/start.sh \
-- --templateProperty project.id=$GCP_PROJECT \
--templateProperty kafka.bootstrap.servers=bootstrap.vault-spark-kafka-cluster.us-central1.managedkafka.gcp-playground1-uw1.cloud.goog:9092 \
--templateProperty kafka.topic=insert-records \
--templateProperty kafka.output.topic.name=vault-responses \
--templateProperty kafka.starting.offset=latest \
--templateProperty kafka.message.format=json \
--templateProperty kafka.schema.url=$SPARK_SCHEMA_FILE \
--templateProperty kafka.await.termination.timeout.ms=600000 \
--templateProperty skyflow.vault.id=rf4b2c42de794e388901dcd9618d100a \
--templateProperty skyflow.vault.table.name=table1 \
--templateProperty skyflow.vault.url=https://re496ktech4x.skyvault.skyflowapis.dev \
--templateProperty skyflow.vault.sa=$SKYFLOW_CREDENTIALS
--templateProperty skyflow.insert.batch.size=100
```

---

## ⚙️ Configuration Properties

### 🔧 Common Properties

| Property                  | Description                          |
| ------------------------- | ------------------------------------ |
| `project.id`              | GCP project ID                       |
| `gcs.staging.bucket.path` | GCS staging bucket path              |
| `log.level`               | Spark context log level (e.g., INFO) |

### 🔧 Kafka Properties

| Property                  | Description                                                                         |
| ------------------------- | ----------------------------------------------------------------------------------- |
| `kafka.bootstrap.servers` | Kafka bootstrap server address                                                      |
| `kafka.topic`             | Kafka topic to subscribe to                                                         |
| `kafka.output.topic.name`             | Kafka topic name to publish tokens                                                          |
| `kafka.message.format`    | Format of the Kafka message (`json` or `bytes`)                                     |
| `kafka.starting.offset`   | Offset to consume from (`earliest`, `latest`, or JSON string specifying partitions) |
| `kafka.schema.url`        | GCS URL pointing to the schema for parsing JSON messages                            |

### 🔧 Kafka to Vault Specific Properties

| Property                             | Description                                                |
| ------------------------------------ | ---------------------------------------------------------- |
| `kafka.await.termination.timeout.ms` | Time in milliseconds to wait before terminating the stream |

### 🔧 Skyflow Properties

| Property                    | Description                                                       |
| --------------------------- |-------------------------------------------------------------------|
| `skyflow.vault.id`          | The Skyflow Vault ID                                              |
| `skyflow.vault.table.name`  | The table name within the Skyflow Vault                           |
| `skyflow.vault.url`         | Vault URL of the Skyflow Vault                                    |
| `skyflow.vault.credentials` | SecretManager Resource Name storing Skyflow credentials JSON file |
| `skyflow.insert.batch.size` | Batch Size to insert into the Skyflow Vault                       |

---

## 📝 Notes

* You need to have the `gcloud` CLI installed on your machine.
* Ensure you are logged into your Google Cloud account (`gcloud auth login`).
* You must have the required IAM permissions to submit jobs to Dataproc and access GCS resources.
* Make sure to upload the schema file to your GCS bucket beforehand using the `gsutil cp` command.
* Ensure your Kafka bootstrap server is accessible from the Dataproc job and has the necessary permissions for communication.
* Store Skyflow Vault Credentials JSON in SecretManager and provide resource name via `skyflow.vault.credentials`.

---

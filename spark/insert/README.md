# Skyflow Vault Insert Integration for Spark

Easily move and tokenize data from Kafka into a Skyflow Vault using scalable Spark jobs on Google Cloud Dataproc. This integration helps you automate secure data ingestion, tokenization, and downstream publishing for privacy-first analytics and processing.

## Features

- **Kafka to Skyflow Vault**: Ingests events from Kafka and inserts them into a Skyflow Vault.
- **Tokenization**: Publishes tokenization events to a separate Kafka topic for downstream use.
- **Scalable Spark Processing**: Runs on Apache Spark and Google Cloud Dataproc for high-volume, distributed workloads.
- **Configurable**: Supports flexible configuration for Kafka, Skyflow, and Spark properties.
- **Batch Insert**: Customizable batch size for efficient vault operations.

## Get started

### Before you start

You need the following items:

- _gcloud_ installed ([Google Cloud SDK documentation](https://cloud.google.com/sdk/docs/install)) and logged in to your Google Cloud account
- Google Cloud account with IAM permissions for Dataproc and Google Cloud Storage (GCS)
- Access to a Kafka cluster
- A Skyflow vault
- Skyflow credentials stored in Secret Manager

### 1. Clone the repository

Clone the repository and make sure all dependencies are available in your Spark/Dataproc environment:

```bash
git clone https://github.com/skyflowapi/skyflow-integrations.git
cd spark/insert
```

### 2. Submit the job

Submit a job to Dataproc using the provided script and your configuration:

```bash
# 1. Set required environment variables
export GCP_PROJECT=<gcp-project-id>
export REGION=<region>
export SUBNET=<subnet>
export JOB_TYPE=<cluster|serverless>
export CLUSTER=<your-data-proc-cluster>
export GCS_STAGING_LOCATION=<gcs-directory-to-store-build-jar>
export SPARK_SCHEMA_FILE=<gcs-spark-schema-link>
export SKYFLOW_CREDENTIALS=<secret-manager-resource-name>

# 2. Submit the Spark job
scripts/start.sh \
  -- --templateProperty project.id=$GCP_PROJECT \
  --templateProperty kafka.bootstrap.servers=<kafka-broker-list> \
  --templateProperty kafka.topic=<kafka-topic-name> \
  --templateProperty kafka.output.topic.name=<kafka-output-topic-name> \
  --templateProperty kafka.starting.offset=<kafka-offset> \
  --templateProperty kafka.message.format=<kafka-message-format> \
  --templateProperty kafka.schema.url=$SPARK_SCHEMA_FILE \
  --templateProperty kafka.await.termination.timeout.ms=<dataproc-job-timeout> \
  --templateProperty skyflow.vault.id=<skyflow-vault-id> \
  --templateProperty skyflow.vault.table.name=<skyflow-table-name> \
  --templateProperty skyflow.vault.url=<skyflow-vault-url> \
  --templateProperty skyflow.vault.sa=$SKYFLOW_CREDENTIALS \
  --templateProperty skyflow.insert.batch.size=<batch-size-to-insert-into-vault>
```

### Example

```bash
export GCP_PROJECT=gcp-playground1-uw1
export REGION=us-central1
export SUBNET=default
export JOB_TYPE=CLUSTER
export CLUSTER=vault-spark-cluster
export GCS_STAGING_LOCATION=gs://gcp-playground1-uw1-dataproc-scripts/stage
export SPARK_SCHEMA_FILE=gs://gcp-playground1-uw1-dataproc-scripts/schema/table1.json
export SKYFLOW_CREDENTIALS=projects/999999999999/secrets/vault-creds/versions/latest

scripts/start.sh \
  -- --templateProperty project.id=$GCP_PROJECT \
  --templateProperty kafka.bootstrap.servers=bootstrap.vault-spark-kafka-cluster.us-central1.managedkafka.gcp-playground1-uw1.cloud.goog:9092 \
  --templateProperty kafka.topic=insert-records \
  --templateProperty kafka.output.topic.name=vault-responses \
  --templateProperty kafka.starting.offset=latest \
  --templateProperty kafka.message.format=json \
  --templateProperty kafka.schema.url=$SPARK_SCHEMA_FILE \
  --templateProperty kafka.await.termination.timeout.ms=600000 \
  --templateProperty skyflow.vault.id=ab12ab12ab12ab12ab12aba \
  --templateProperty skyflow.vault.table.name=table1 \
  --templateProperty skyflow.vault.url=https://xyz.vault.skyflowapis.com \
  --templateProperty skyflow.vault.sa=$SKYFLOW_CREDENTIALS \
  --templateProperty skyflow.insert.batch.size=100
```

## Configuration

### Environment variables

| Variable | Description |
| --- | --- |
| `CLUSTER` | Dataproc cluster name. Required if `JOB_TYPE` is `CLUSTER`. |
| `GCP_PROJECT` | GCP project ID. |
| `GCS_STAGING_LOCATION` | GCS location for staging files. |
| `JOB_TYPE` | Type of job to submit (`CLUSTER` or `SERVERLESS`). |
| `REGION` | GCP region. |

### Common properties

| Property                  | Description                                    |
| ------------------------- | ---------------------------------------------- |
| `project.id`              | GCP project ID.                                |
| `gcs.staging.bucket.path` | GCS staging bucket path.                       |
| `log.level`               | Spark context log level (for example, `INFO`). |

### Kafka properties

| Property | Description |
| --- | --- |
| `kafka.bootstrap.servers` | Kafka bootstrap server address. |
| `kafka.topic` | Kafka topic to subscribe to. |
| `kafka.output.topic.name` | Kafka topic name to publish tokens. |
| `kafka.message.format` | Format of the Kafka message (`json` or `bytes`). |
| `kafka.starting.offset` | Offset to consume from (`earliest`, `latest`, or JSON string specifying partitions). |
| `kafka.schema.url` | GCS URL pointing to the schema for parsing JSON messages. |
| `kafka.await.termination.timeout.ms` | Time in milliseconds to wait before terminating the stream. |

### Skyflow properties

| Property | Description |
| --- | --- |
| `skyflow.vault.id` | The Skyflow Vault ID. |
| `skyflow.vault.table.name` | The table name within the Skyflow Vault. |
| `skyflow.vault.url` | Vault URL of the Skyflow Vault. |
| `skyflow.vault.sa` | Path to the Skyflow credentials secret version in Secret Manager. |
| `skyflow.insert.batch.size` | Batch size to insert into the Skyflow Vault. Recommended value of `100`. |

## Troubleshooting

| Problem | Solution |
| --- | --- |
| Permission denied on Dataproc or GCS | Ensure your IAM role includes Dataproc and Google Cloud Storage (GCS) permissions. |
| Kafka connection refused | Verify network access and Kafka server address. |
| Skyflow credentials error | Confirm Secret Manager resource name and permissions. |
| Job times out | Increase `kafka.await.termination.timeout.ms` as needed. |
| Kafka server inaccessible | Ensure the Kafka bootstrap server is accessible from Dataproc and has required permissions. |
| Skyflow credentials not found | Store Skyflow Vault Credentials JSON in Secret Manager and provide the resource name via `skyflow.vault.sa`. |

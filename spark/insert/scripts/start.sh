#!/usr/bin/env bash
set -e

#Initialize functions and Constants
echo "Script Started Execution"


BIN_DIR="$(dirname "$BASH_SOURCE")"
source ${BIN_DIR}/dataproc_template_functions.sh

PROJECT_ROOT_DIR=${BIN_DIR}/..
JAR_FILE=vault-insert-integration-0.0.1-SNAPSHOT.jar
if [ -z "${JOB_TYPE}" ]; then
  JOB_TYPE=SERVERLESS
fi

. ${BIN_DIR}/dataproc_template_functions.sh

check_required_envvar GCP_PROJECT
check_required_envvar REGION
check_required_envvar GCS_STAGING_LOCATION

# Remove trailing forward slash
GCS_STAGING_LOCATION=`echo $GCS_STAGING_LOCATION | sed 's/\/*$//'`

# Do not rebuild when SKIP_BUILD is specified
# Usage: export SKIP_BUILD=true
if [ -z "$SKIP_BUILD" ]; then
  java --version
  java_status=$?
  check_status $java_status "\n Java is installed, thus we are good to go \n" "\n Java is not installed on this machine, thus we need to install that first \n"

  mvn --version
  mvn_status=$?

  check_status $mvn_status "\n Maven is installed, thus we are good to go \n" "\n Maven is not installed on this machine, thus we need to install that first \n"

  #Change PWD to root folder for Maven Build
  cd ${PROJECT_ROOT_DIR}
  mvn clean spotless:apply install -DskipTests
  build_status=$?

  check_status $build_status "\n Code build went successful, thus we are good to go \n" "\n We ran into some issues while building the jar file, seems like mvn clean install is not running fine \n"

  #Copy jar file to GCS bucket Staging folder
  echo_formatted "Copying ${PROJECT_ROOT_DIR}/target/${JAR_FILE} to staging bucket: ${GCS_STAGING_LOCATION}/${JAR_FILE}"
  gsutil cp ${PROJECT_ROOT_DIR}/target/${JAR_FILE} ${GCS_STAGING_LOCATION}/${JAR_FILE}
  check_status $? "\n Commands to copy the project jar file to GCS Staging location went fine, thus we are good to go \n" "\n It seems like there is some issue in copying the project jar file to GCS Staging location \n"

  # Copy log4j.properties to GCS bucket
  echo_formatted "Copying src/main/resources/log4j.properties to gs://${GCP_PROJECT}-dataproc-scripts/config/"
  gsutil cp src/main/resources/log4j.properties gs://gcp-playground1-uw1-dataproc-scripts/config/
  check_status $? "\n log4j.properties copied to GCS, thus we are good to go \n" "\n Failed to copy log4j.properties to GCS \n"
fi

OPT_SPARK_VERSION="--version=1.2"
OPT_PROJECT="--project=${GCP_PROJECT}"
OPT_REGION="--region=${REGION}"

OPT_JARS="--jars=file:///usr/lib/spark/connector/spark-avro.jar,${GCS_STAGING_LOCATION}/${JAR_FILE}"

if [[ $OPT_SPARK_VERSION == *"=1.1"* ]]; then
  echo "Dataproc Serverless Runtime 1.1 or CLUSTER Job Type Detected"
	OPT_JARS="--jars=file:///usr/lib/spark/external/spark-avro.jar,${GCS_STAGING_LOCATION}/${JAR_FILE}"
fi
if [[ $JOB_TYPE == "CLUSTER" ]]; then
  if [[ -n "${CLUSTER}" ]]; then
    CLUSTER_IMAGE_VERSION=$(gcloud dataproc clusters describe "${CLUSTER}" --project="${GCP_PROJECT}" --region="${REGION}" --format="value(config.softwareConfig.imageVersion)")
    if [[ $CLUSTER_IMAGE_VERSION == *"2.0"* || $CLUSTER_IMAGE_VERSION == *"2.1"* ]]; then
      echo "Dataproc Cluster Image ${CLUSTER_IMAGE_VERSION} Detected"
      OPT_JARS="--jars=file:///usr/lib/spark/external/spark-avro.jar,${GCS_STAGING_LOCATION}/${JAR_FILE}"
    fi
  fi
fi
OPT_LABELS="--labels=job_type=kafka_to_vault"
OPT_DEPS_BUCKET="--deps-bucket=${GCS_STAGING_LOCATION}"
OPT_CLASS="--class=Main"

# Optional arguments
if [ -n "${SUBNET}" ]; then
  OPT_SUBNET="--subnet=${SUBNET}"
fi
if [ -n "${CLUSTER}" ]; then
  OPT_CLUSTER="--cluster=${CLUSTER}"
fi
if [ -n "${HISTORY_SERVER_CLUSTER}" ]; then
  OPT_HISTORY_SERVER_CLUSTER="--history-server-cluster=${HISTORY_SERVER_CLUSTER}"
fi
if [ -n "${METASTORE_SERVICE}" ]; then
  OPT_METASTORE_SERVICE="--metastore-service=${METASTORE_SERVICE}"
fi
if [ -n "${JARS}" ]; then
  OPT_JARS="${OPT_JARS},${JARS}"
fi
if [ -n "${SPARK_PROPERTIES}" ]; then
  OPT_PROPERTIES="--properties=${SPARK_PROPERTIES}"
fi
if [ -n "${SERVICE_ACCOUNT_NAME}" ]; then
  OPT_SERVICE_ACCOUNT_NAME="--service-account=${SERVICE_ACCOUNT_NAME}"
fi

# external log4j config
OPT_FILES="--files=gs://gcp-playground1-uw1-dataproc-scripts/config/log4j.properties"
OPT_LOG4J_PROPS="--properties=spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties,spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties"

# Running on an existing dataproc cluster or run on serverless spark
if [ "${JOB_TYPE}" == "CLUSTER" ]; then
  echo "JOB_TYPE is CLUSTER, so will submit on existing dataproc cluster"
  check_required_envvar CLUSTER
  command=$(cat << EOF
  gcloud dataproc jobs submit spark \
      ${OPT_PROJECT} \
      ${OPT_REGION} \
      ${OPT_CLUSTER} \
      ${OPT_JARS} \
      ${OPT_LABELS} \
      ${OPT_FILES} \
      ${OPT_LOG4J_PROPS} \
      ${OPT_PROPERTIES} \
      ${OPT_CLASS}
EOF
)
elif [ "${JOB_TYPE}" == "SERVERLESS" ]; then
  echo "JOB_TYPE is SERVERLESS, so will submit on serverless spark"
  command=$(cat << EOF
  gcloud dataproc batches submit spark \
      ${OPT_SPARK_VERSION} \
      ${OPT_PROJECT} \
      ${OPT_REGION} \
      ${OPT_JARS} \
      ${OPT_LABELS} \
      ${OPT_DEPS_BUCKET} \
      ${OPT_FILES} 
      ${OPT_LOG4J_PROPS} \
      ${OPT_PROPERTIES} \
      ${OPT_SUBNET} \
      ${OPT_HISTORY_SERVER_CLUSTER} \
      ${OPT_METASTORE_SERVICE} \
      ${OPT_SERVICE_ACCOUNT_NAME}
EOF
)
else
  echo "Unknown JOB_TYPE \"${JOB_TYPE}\""
  exit 1
fi

echo "Triggering Spark Submit job"
echo ${command} "$@"
${command} "$@"
spark_status=$?

check_status $spark_status "\n Spark Command ran successful \n" "\n It seems like there are some issues in running spark command. Requesting you to please go through the error to identify issues in your code \n"


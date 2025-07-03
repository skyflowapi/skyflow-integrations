#!/usr/bin/env bash

# Check if an environment variable is set,
# takes the name of the environment variable as an argument
check_required_envvar() {
  name=$1        # We pass in the variable to check as a string
  value=${!name} # Indirect expansion to get the value
  if [ -z "${value}" ]
  then
    echo "Required environment variable ${name} is missing"
    Help && exit 1
  else
    echo "${name}=${value}"
  fi
}

# Auxiliary Function to check the exit status passed as an argument
# This function is also responsible for printing the error message or success message based on the exit status
check_status()
{
  if [ "$1" -eq 0 ];
  then
    printf "$2"
  else
    printf "$3"
    exit 1
  fi
}


#Mandatory vs optional  specify
Help() {
  # Display Help
  help_text=$(cat << EndOfMessage
    Usage:

    # Environment variables
    export GCP_PROJECT=projectId
    export REGION=us-west1
    export GCS_STAGING_LOCATION=gs://bucket/path

    export JOB_TYPE=SERVERLESS|CLUSTER # Defaults to serverless

    # Required environment variables for CLUSTER mode
    export CLUSTER={clusterId}

    # Optional environment variables for SERVERLESS mode
    export SUBNET=projects/{projectId}/regions/{regionId}/subnetworks/{subnetId}
    export HISTORY_SERVER_CLUSTER=projects/{projectId}/regions/{regionId}/clusters/{clusterId}
    export METASTORE_SERVICE=projects/{projectId}/locations/{regionId}/services/{serviceId}

    Usage syntax:

    start.sh [sparkSubmitArgs] -- --template templateName [--templateProperty key=value] [extraArgs]

    eg:
    start.sh -- --template GCSTOBIGQUERY --templateProperty gcs.bigquery.input.location=gs://bucket/path/ (etc...)
EndOfMessage
)
  echo "${help_text}"
}


#Formatted print
echo_formatted() {
  echo "==============================================================="
  echo
  echo $*
  echo
  echo "==============================================================="
}

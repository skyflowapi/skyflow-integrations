# BigQuery Detokenization UDF

This user-defined function (UDF) enables bulk detokenization from within BigQuery. The UDF runs through [BigQuery remote functions](https://cloud.google.com/bigquery/docs/remote-functions) and uses [Cloud Run](https://cloud.google.com/functions) to make detokenization requests and convert formats between BigQuery and the Skyflow Data API.

Using the UDF in BigQuery looks like this:

```sql
SELECT
  dataset.as_string(dataset.detokenize(email_token)) email_cleartext,
FROM dataset.tokenized_user;
```

## Interface

The BigQuery detokenization UDF is intended to match the `POST /v2/tokens/detokenize` endpoint's
interface as closely as possible—though there are some documented exceptions.

The remote function accepts a Skyflow vault ID and token, and the function returns a JSON object
matching the `POST /v2/tokens/detokenize` response object.

### Retries

Transient errors involving requests to the Skyflow Data API are resolved automatically by the Cloud Run service.

### Error Handling

If an error can't be resolved through retries, the error is communicated to BigQuery in one of two ways,
depending on whether the error is (1) query- or (2) token-level:

- **Query-level errors:** Fatal errors that affect the entire query—such as when authentication with the Skyflow Data API fails or when arguments are malformed—result in BigQuery receiving a custom HTTP 599 status code, which fails the entire query.

- **Token-level errors:** For errors that affect individual tokens and their corresponding response objects, the response object includes the HTTP code and error message fields to indicate the failure, and the value field is omitted. BigQuery receives an HTTP 200 status code so that the query as a whole will continue unaffected. The consumer of the remote function in BigQuery is responsible for checking for errors, either by inspecting the Cloud Run logs or by checking the returned response objects for missing values, non-200 HTTP status codes, and error messages.

> Note: The value field is also omitted in the response object when the input token is null.
> When checking for errors, make sure to consider missing values only for response objects
> where the input token was non-null.

### Null Tokens

If you specify a null token, the UDF doesn't send the token to the Skyflow Data API.
Instead, the UDF returns an empty response object (with all fields omitted) to BigQuery.
This differs from the usual behavior of the Skyflow Data API, where a null token is
considered an invalid input and leads to an HTTP 400 status code.

## Configuration

Step [2. Deploy](#2-deploy) documents the configuration options and shows how to customize them. Non-sensitive values, such as the logging level and Skyflow vault URL, are
set as environment variables directly in the Cloud Run service. Sensitive values, such as the Skyflow service account credentials, are stored in Secret Manager and exposed to the Cloud Run service as environment variables.

## Logging

The Cloud Run service sends structured logs to stderr, which are captured by GCP and are available
in Log Explorer. By default, the logging level is set to `WARN`, which includes query-level
and token-level error messages. During performance tuning, it can be convenient to receive logs
about successful requests—in which case, a logging level of `INFO` is useful.
Customize the logging level by setting the `LOGGING_LEVEL` variable during step [2. Deploy](#2-deploy).

## Metrics

Metrics on Cloud Run resource usage and requests between BigQuery and Cloud Run are available
in the Cloud Run console in GCP.

## Project Structure

```
 .
├── cmd       <- entrypoint
└── terraform <- deployment
```

## Getting Started

See the [Makefile](./Makefile) for useful commands.

### 1. Build

1. Authenticate with GCP through the `gcloud` CLI:

    ```bash
    gcloud auth login
    gcloud config set project "<REPLACE_WITH_GCP_PROJECT_ID>"
    ```

1. Build the detokenization image to be deployed to Cloud Run:

    > Note: If you omit `IMAGE_NAME` or `IMAGE_TAG`,
    > they will default to `detokenize` and the current date/time, respectively.

    ```bash
    make gcloud-build \
        GCP_PROJECT_ID="<REPLACE_WITH_GCP_PROJECT_ID>" \
        IMAGE_NAME="<REPLACE_WITH_IMAGE_NAME>" \
        IMAGE_TAG="<REPLACE_WITH_IMAGE_TAG>"
    ```

### 2. Deploy

You can use the included Terraform module to conveniently deploy the BigQuery detokenization UDF stack.

To deploy the stack, follow these instructions:

1. Navigate to the [terraform](./terraform/) directory:

    ```bash
    cd terraform
    ```

1. Initialize the Terraform working directory:

    ```bash
    terraform init
    ```

1. Create a copy of [`detokenization.tfvars.example`](./terraform/detokenization.tfvars.example) named
`detokenization.tfvars` and update the necessary variables. The image name and image tag must match
the values from [1. Build](#1-build), and the Skyflow service account must have the required permissions
to detokenize tokens in your Skyflow vault.

    ```bash
    cp detokenization.tfvars.example detokenization.tfvars
    vim detokenization.tfvars
    ```

    Here is an example `detokenization.tfvars` file:

    ```
    # gcp_project_id: The ID of the GCP project where the stack will be deployed.
    gcp_project_id = "skyflow"

    # gcp_region: The GCP region where the stack will be deployed.
    gcp_region = "us-west1"

    # cloud_run_service_name: The name of the Cloud Run service, as it will appear in GCP.
    cloud_run_service_name = "detokenize-53ea587"

    # cloud_run_service_sa_id: The ID of the Cloud Run service account, as it will appear in GCP.
    cloud_run_service_sa_id = "detokenize-53ea587"

    # cloud_run_service_env_vars: The environment variables to be set on the Cloud Run service.
    cloud_run_service_env_vars = {
      # SKYFLOW_VAULT_URL: The Skyflow vault URL that detokenization API requests will be sent to.
      SKYFLOW_VAULT_URL = "https://skyflow.skyvault.skyflowapis.dev"

      # SKYFLOW_MAX_BATCH_SIZE: The maximum number of tokens to be detokenized in a single API request to Skyflow.
      SKYFLOW_MAX_BATCH_SIZE = 1000

      # LOGGING_LEVEL: The logging level to be used by the Cloud Run service.
      LOGGING_LEVEL = "WARN" # or "INFO", "ERROR", "FATAL"
    }

    # image_name: The name of the pre-built Docker image for detokenization that will be deployed to Cloud Run.
    image_name = "detokenize"

    # image_tag: The tag of the pre-built Docker image for detokenization that will be deployed to Cloud Run.
    image_tag = "53ea587"

    # bigquery_connection_id: The ID of the BigQuery connection to be created between BigQuery and the Cloud Run service, as it will appear in GCP.
    bigquery_connection_id = "detokenize-53ea587"

    # skyflow_sa_credentials_secret_id: The ID of the secret containing the Skyflow service account credentials, as it will appear in GCP.
    skyflow_sa_credentials_secret_id = "detokenize-53ea587"

    # skyflow_sa_credentials_secret_value: The Skyflow service account credentials to be stored in `skyflow_sa_credentials_secret_id`.
    skyflow_sa_credentials_secret_value = <<EOT
    {"clientID":"","clientName":"","tokenURI":"","keyID":"","privateKey":"","keyValidAfterTime":"","keyValidBeforeTime":"","keyAlgorithm":""}
    EOT
    ```

1. Deploy the stack with Terraform:

    ```bash
    terraform apply -var-file="detokenization.tfvars"
    ```

1. Note the Terraform outputs. You'll need them for step [3. Query](#3-query).

    If you want to view the Terraform outputs again, run the following command:

    ```bash
    terraform output
    ```

If you're finished using the deployment and want to destroy it, run the following command:

```bash
terraform destroy -var-file="detokenization.tfvars"
```

## 3. Query

How you configure the BigQuery remote function depends heavily on your organization's approach to naming and arranging
datasets and UDFs in BigQuery.

BigQuery
[supports manipulating the JSON-encoded](https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions)
response objects received from the remote function. Below is an example that you can use as a starting point to define the
detokenization remote function in BigQuery, plus some helpful utility UDFs.

```sql
----------------------------------------------------------------------------------
-- START: ONE-TIME CONFIGURATION
----------------------------------------------------------------------------------


-- Create a dataset to contain UDFs and tables
CREATE SCHEMA `<REPLACE_WITH_PROJECT_ID>.<REPLACE_WITH_DATASET>`;


-- `skyflow_detokenize` is a remote function that invokes the `detokenize` Cloud Run
-- service to detokenize a token from a given Skyflow vault.
-- The Skyflow service account used by the Cloud Run service must have
-- the required permissions to detokenize tokens in the given Skyflow vault.
-- The return type is `JSON` as the Skyflow Data API response is returned as-is, and
-- the detokenized value within the result can have any supported type,
-- such as string, date, boolean, number, etc. The consumer is responsible
-- for casting the detokenized value to the appropriate type.
--
--  Args:
--    - vault_id:   ID of the Skyflow vault containing the token
--    - token:      Token to be detokenized from the vault
--
-- Returns: The Skyflow Data API response as-is
CREATE OR REPLACE FUNCTION `<REPLACE_WITH_PROJECT_ID>.<REPLACE_WITH_DATASET>.skyflow_detokenize`(
    vault_id STRING,
    token STRING
)
RETURNS JSON
REMOTE WITH CONNECTION `<REPLACE_WITH_BIGQUERY_CONNECTION_ID>` -- from the Terraform outputs in step 2. Deploy
OPTIONS (
    endpoint = '<REPLACE_WITH_CLOUD_RUN_SERVICE_URL>', -- from the Terraform outputs in step 2. Deploy
    max_batching_rows = 1000 -- maximum number of rows sent to Cloud Run from BigQuery in a single request
);


-- `detokenize` detokenizes a given token in the Skyflow vault.
-- Improves usability for consumers by filling in the vault ID automatically.
-- This wrapper is not suitable if tokens could belong to one of multiple vaults.
--
-- Args:
--    - token: Token in the Skyflow vault
--
-- Returns: The Skyflow Data API response as-is
CREATE OR REPLACE FUNCTION `<REPLACE_WITH_PROJECT_ID>.<REPLACE_WITH_DATASET>.detokenize` (
  token STRING
)
RETURNS JSON
AS (
  `<REPLACE_WITH_PROJECT_ID>.<REPLACE_WITH_DATASET>.skyflow_detokenize` (
    "<REPLACE_WITH_SKYFLOW_VAULT_ID>",
    token
  )
);


-- `as_string` extracts the value from a detokenization result and casts it to a string.
--
-- Args:
--    - detoken: Detokenization result to be processed
--
-- Returns: The detokenized value as a string
CREATE OR REPLACE FUNCTION `<REPLACE_WITH_PROJECT_ID>.<REPLACE_WITH_DATASET>.as_string`(
  detoken JSON
) RETURNS STRING AS (
  LAX_STRING(detoken.value)
);


-- `as_date` extracts the value from a detokenization result and casts it to a date.
--
-- Args:
--    - detoken: Detokenization result to be processed
--
-- Returns: The detokenized value as a date
CREATE OR REPLACE FUNCTION `<REPLACE_WITH_PROJECT_ID>.<REPLACE_WITH_DATASET>.as_date`(
  detoken JSON
) RETURNS DATE AS (
  DATE(LAX_STRING(detoken.value))
);


-- `as_bool` extracts the value from a detokenization result and casts it to a bool.
--
-- Args:
--    - detoken: Detokenization result to be processed
--
-- Returns: The detokenized value as a bool
CREATE OR REPLACE FUNCTION `<REPLACE_WITH_PROJECT_ID>.<REPLACE_WITH_DATASET>.as_bool`(
  detoken JSON
) RETURNS BOOL AS (
  LAX_BOOL(detoken.value)
);


----------------------------------------------------------------------------------
-- END: ONE-TIME CONFIGURATION
----------------------------------------------------------------------------------




----------------------------------------------------------------------------------
-- START: EXAMPLE USAGE
----------------------------------------------------------------------------------


-- Detokenize emails
SELECT
  `<REPLACE_WITH_PROJECT_ID>.<REPLACE_WITH_DATASET>.as_string`(`<REPLACE_WITH_PROJECT_ID>.<REPLACE_WITH_DATASET>.detokenize`(email_token)) email_cleartext,
FROM <REPLACE_WITH_PROJECT_ID>.<REPLACE_WITH_DATASET>.<REPLACE_WITH_TOKENS_TABLE>;


----------------------------------------------------------------------------------
-- END: EXAMPLE USAGE
----------------------------------------------------------------------------------
```

## Performance Tuning

You can tune the UDF's performance with the following parameters:

- `max_instance_count` (T): The maximum number of allowed Cloud Run instances.
- `max_instance_request_concurrency` (T): The maximum number of concurrent requests that each Cloud Run instance can receive from BigQuery.
- `cpu` and `memory` (T): The compute resources for each Cloud Run instance.
- `max_batching_rows` (B): The maximum number of remote function calls in each HTTP request to Cloud Run from BigQuery.
- `SKYFLOW_MAX_BATCH_SIZE` (E): The maximum number of tokens sent in each `POST /v2/tokens/detokenize` request to the Skyflow Data API.

`T` indicates the option is controllable from the Terraform module, `B` indicates its controllable from BigQuery, and `E` indicates its
controllable through an environment variable on the detokenization service.

Under maximum load, assuming that BigQuery is fully saturating `max_batching_rows`, the number of concurrent detokenizations
processed on the Skyflow Data API is equal to

```
var max_concurrent_requests         = max_instance_count * max_instance_request_concurrency
var max_concurrent_detokenizations  = max_concurrent_requests * min(max_batching_rows, SKYFLOW_MAX_BATCH_SIZE)
```

`min(max_batching_rows, SKYFLOW_MAX_BATCH_SIZE)` reflects the fact that, within the context of a single request from BigQuery,
Cloud Run sends batches serially and synchronously to the Skyflow Data API. The current detokenization service implementation
doesn't increase or multiply the concurrency generated by BigQuery. You may decide to set `max_batching_rows > SKYFLOW_MAX_BATCH_SIZE`
to reduce pressure on the Skyflow Data API while also reducing the number of round trips between BigQuery and Cloud Run.

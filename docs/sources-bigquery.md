# BigQuery

Run GoogleSQL jobs in BigQuery and read their JSON result pages as typed Arrow batches. For selected-table joins, configure the optional [federation bridge](federation-adapters.md#configure-exact-remote-names).

1. Provision a short-lived OAuth token with the required job and dataset read permissions. Token acquisition and renewal are operator responsibilities.
2. Register the source; `project` and `location` are required:

```yaml
sources:
  - id: warehouse
    type: bigquery
    url_env: KELVO_SOURCE_BIGQUERY_URL
    token_env: KELVO_SOURCE_BIGQUERY_TOKEN
    options:
      project: example-project
      location: US
      dataset: analytics
      maximum_bytes_billed: "10000000000"
```

3. Set the URL to `https://bigquery.googleapis.com` and submit native SQL with `connection_id: warehouse`. Parameters, scripts and writes are rejected.

`dataset` and `maximum_bytes_billed` are optional. Database grants cover all accessible data, remote functions and external connections; a SQL LIMIT does not bound billed reads.

Each job has a client-generated ID and is never resubmitted after a network failure. Pages contain at most 1,000 rows and must fit the shared HTTP budget. Timeout is sent to BigQuery; failure or cancellation triggers a separate bounded, best-effort cancel request. Partial results, changed job/schema, inconsistent counts and output-limit overruns fail. Worker termination can interrupt cleanup; see the [cancellation grace](usage.md#native-cancellation-and-remote-cleanup).

Supported types: nullable integers, finite floats, Boolean, strings, bytes, dates, DATETIME, microsecond TIMESTAMP, NUMERIC and BIGNUMERIC within Decimal256's 76-digit bound. Extreme 77-digit values, RECORD/STRUCT, repeated fields, TIME, GEOGRAPHY, JSON, RANGE and picosecond timestamps need an explicit supported conversion.

The connector buffers JSON pages and does not use the Storage Read API. Tests cover protocol behavior; live BigQuery acceptance and export throughput remain unmeasured.

References: [jobs.insert](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert), [results](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults), [cancel](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel), [schema](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema).

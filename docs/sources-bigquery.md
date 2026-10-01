# BigQuery

The native BigQuery connector submits GoogleSQL jobs and reads bounded pages from
the BigQuery REST API. BigQuery executes the query; Kelvo converts returned scalar
values into Arrow batches. Configure a short-lived OAuth access token with only
the required job and dataset read permissions:

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

The URL is `https://bigquery.googleapis.com`. Project and location are required;
dataset and maximum billed bytes are optional. Token acquisition and renewal are
operator responsibilities. Submit native SQL using `connection_id: warehouse`.
Parameters, scripts and writes are currently rejected. Database grants remain
the authorization boundary, including access through remote functions or
external connections.

Each job has a client-generated ID. Kelvo does not replay submission after a
network failure. Results use at most 1,000 rows per page; a page must fit the
shared HTTP response budget. Query timeout is sent to BigQuery, and failure or
client cancellation triggers a separate bounded cancellation request. BigQuery
cancellation is best effort. Partial results, changing job identity/schema,
inconsistent counts, and row/byte overflow fail explicitly.

Supported values include nullable integers, finite floats, Boolean, strings,
bytes, dates, DATETIME, microsecond TIMESTAMP, NUMERIC and BIGNUMERIC within Arrow
Decimal256's 76-digit precision. BigQuery's extreme 77-digit BIGNUMERIC values
fail explicitly. Integer timestamp output avoids floating-point epoch conversion.
RECORD/STRUCT, repeated fields, TIME, GEOGRAPHY, JSON, RANGE, picosecond timestamps,
and other unsupported types require an explicit SQL conversion or another adapter.

This API buffers each JSON page before Arrow conversion. It does not use the
BigQuery Storage Read API and has no measured export-throughput claim. Protocol
tests cover exact int64/decimal/timestamps, NULL, paging, cancellation, and invalid
responses. A live BigQuery account has not been used for acceptance testing.

References: [jobs.insert](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert),
[jobs.getQueryResults](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/getQueryResults),
[jobs.cancel](https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/cancel),
[table field schema](https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableFieldSchema).

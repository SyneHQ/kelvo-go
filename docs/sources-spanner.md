# Google Cloud Spanner

## Official API references

The connector design follows the official REST contracts reviewed before
implementation:

- [Create a session](https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases.sessions/create).
- [Execute SQL](https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases.sessions/executeSql): a single materialized reply, with a provider result limit of 10 MiB.
- [ResultSet](https://cloud.google.com/spanner/docs/reference/rest/v1/ResultSet).
- [Type encodings](https://cloud.google.com/spanner/docs/reference/rest/v1/Type): INT64 and NUMERIC use strings, timestamps use UTC RFC3339, and bytes use base64.
- [Transaction selection](https://cloud.google.com/spanner/docs/reference/rest/v1/TransactionSelector) and [read-only options](https://cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions).
- [Delete a session](https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases.sessions/delete): releases resources and asynchronously cancels its operations.
- [Streaming partial results](https://cloud.google.com/spanner/docs/reference/rest/v1/PartialResultSet): values can span messages and require recursive chunk reconstruction. This connector deliberately uses the bounded `executeSql` API; it does not implement that streaming protocol.

## Configuration contract

Use source type `spanner`, `url_env`, `token_env`, and the required `project`,
`instance`, and `database` options. The endpoint is an explicit HTTPS origin,
normally `https://spanner.googleapis.com`. The token is an externally refreshed,
short-lived OAuth bearer token. Kelvo does not discover Application Default
Credentials or call a metadata server.

```yaml
sources:
  - id: ledger
    type: spanner
    url_env: KELVO_SOURCE_SPANNER_URL
    token_env: KELVO_SOURCE_SPANNER_TOKEN
    options:
      project: example-project
      instance: analytics
      database: ledger
```

The three resource components accept 1–128 ASCII letters, digits, underscores,
or hyphens; path separators and unknown options are rejected. Grant a dedicated
read-only principal access to the intended database, including session creation,
session deletion, and SQL reads. The token needs the provider's `spanner.data`
or `cloud-platform` OAuth scope. HTTPS certificate verification is required;
redirects and environment HTTP proxies are disabled.

```sh
bin/kelvo query --mode native --connection ledger --config kelvo.yml \
  --sql 'SELECT AccountId, Balance FROM Accounts' --out accounts.arrow
```

Native requests use one `connection_id` and conservative single-statement
SELECT/WITH SQL. DML, administrative/session syntax, multiple statements, GQL,
MongoDB pipelines, and query parameters are unsupported. No string substitution
or parameter interpolation is performed.

Each execution creates a session, submits one explicitly single-use, strongly
consistent read-only SQL transaction, and deletes the known session under an
independent cleanup deadline. Requests are never automatically replayed.
Result delivery does not stream Spanner execution: the bounded REST response is
materialized before Arrow batches are emitted.

The JSON reply is limited to the smaller of 10 MiB and one quarter of the
configured memory budget, in addition to Spanner's own 10 MiB result limit.
Rows are then converted one at a time into synchronous Arrow batches. Row and
encoded-output limits fail the query; they do not truncate a successful result.
This is not a large-export or Spanner Data Boost/partitioned-query connector.

## Result types

| Spanner type | Arrow representation |
| --- | --- |
| BOOL, INT64 | Boolean, Int64; INT64 never passes through floating point |
| FLOAT32, FLOAT64 | Float32, Float64; nonfinite values fail explicitly |
| NUMERIC | Exact Decimal128(38,9), including supported scientific notation |
| DATE | Date32 |
| TIMESTAMP | UTC nanosecond timestamp; precision/range loss fails |
| BYTES | Binary, decoded from base64 |
| STRING, JSON, UUID | UTF-8 string with native type metadata |

All fields retain NULLs and `source_type`/`native_type` metadata. JSON is the
JSON-formatted string returned by Spanner, whose own normalization may already
have changed whitespace, key ordering, or duplicate keys. `PG_JSONB` and
`PG_OID` annotations are retained. PostgreSQL `PG_NUMERIC`, arrays, structs,
protobufs, enums, and unknown types fail rather than being guessed. Timestamps
outside Arrow's nanosecond range also fail; cast to a string explicitly when a
wider source timestamp range is needed.

Cleanup gets a separate two-second deadline, including after caller cancellation.
An already-deleted session is accepted; another cleanup failure prevents a
successful outcome. If session creation succeeds remotely but its response is
lost, Kelvo cannot identify that session for deletion; Spanner's idle-session
expiry remains the fallback. Execution and session creation are not retried.

## Validation

TLS protocol fixtures cover source/session binding, explicit read-only
transactions, exact integers/decimals/nanosecond timestamps/NULLs, empty results,
malformed and incomplete replies, unsupported types, output/response budgets,
cancellation cleanup, cleanup failure, and no execution replay. These are
connector-contract tests, not live Google Cloud account acceptance or throughput
measurements.

Worker boundary: the cleanup described here requires the connector process to
remain alive. CLI/HTTP/cluster cancellation or an outer deadline can kill that
process before remote cleanup runs. See [native cancellation and remote cleanup](usage.md#native-cancellation-and-remote-cleanup).

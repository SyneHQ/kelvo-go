# Google Cloud Spanner

Run one strongly consistent read-only Spanner query and convert its bounded REST result to Arrow. This connector targets small results, with a 10 MiB provider ceiling.

## Official API references

[Create session](https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases.sessions/create), [executeSql](https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases.sessions/executeSql), [ResultSet](https://cloud.google.com/spanner/docs/reference/rest/v1/ResultSet), [types](https://cloud.google.com/spanner/docs/reference/rest/v1/Type), [transaction selector](https://cloud.google.com/spanner/docs/reference/rest/v1/TransactionSelector), [read-only options](https://cloud.google.com/spanner/docs/reference/rest/v1/TransactionOptions), and [delete session](https://cloud.google.com/spanner/docs/reference/rest/v1/projects.instances.databases.sessions/delete).

The separate [streaming partial-result protocol](https://cloud.google.com/spanner/docs/reference/rest/v1/PartialResultSet) requires recursive chunk reconstruction and is not implemented here.

## Configuration contract

1. Provision an externally refreshed OAuth token with database reads, session creation/deletion, and `spanner.data` or `cloud-platform` scope.
2. Register the source; Kelvo does not load Application Default Credentials or metadata credentials:

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

3. Set the HTTPS origin, normally `https://spanner.googleapis.com`, and query:

```sh
bin/kelvo query --mode native --connection ledger --config kelvo.yml \
  --sql 'SELECT AccountId, Balance FROM Accounts' --out accounts.arrow
```

Resource components allow 1–128 ASCII letters, digits, underscores or hyphens. Path separators and unknown options fail. TLS certificates are verified; redirects and environment proxies are disabled.

Requests accept one `connection_id` and SELECT/WITH. DML, admin/session commands, multiple statements, GQL, MongoDB pipelines and parameters are unsupported.

Each execution creates a session, submits a single-use strongly consistent read-only transaction, then deletes the known session. Calls are not replayed. The complete JSON response is buffered before synchronous Arrow conversion, capped at the smaller of 10 MiB and one quarter of query memory. Row/byte overflow fails instead of truncating. Partitioned queries and Data Boost are unsupported.

## Result types

| Spanner type | Arrow representation |
| --- | --- |
| BOOL, INT64 | Boolean, exact Int64 |
| FLOAT32, FLOAT64 | Finite Float32, Float64 |
| NUMERIC | Exact Decimal128(38,9), including supported scientific notation |
| DATE | Date32 |
| TIMESTAMP | UTC nanoseconds; precision/range loss fails |
| BYTES | Binary decoded from base64 |
| STRING, JSON, UUID | UTF-8 with native metadata |

NULLs and `source_type`/`native_type` are retained, including PG_JSONB/PG_OID annotations. Spanner may already have normalized JSON. PG_NUMERIC, arrays, structs, protobufs, enums and unknown types fail; explicitly cast unsupported values when a textual representation is acceptable.

Session cleanup has an independent two-second deadline. Already-deleted sessions succeed; other cleanup failures fail the query. A lost create-session reply can leave an unknown session until provider expiry. See [worker cleanup limits](usage.md#native-cancellation-and-remote-cleanup).

## Validation

TLS fixtures cover binding, read-only transactions, exact types, errors, limits, cleanup and no replay. Live Google Cloud acceptance and throughput remain unverified.

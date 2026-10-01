# Cloud SQL sources

Databricks, Snowflake and Cloudflare D1 execute single-source queries in native
mode. Use `connection_id` with the source ID from the YAML catalog; examples are
in [sources.yml](../deploy/examples/sources.yml). These result streams do not yet
participate in DuckDB federation.

Each source uses `url_env` for its HTTPS **origin**, without a path, query or
embedded credentials, and `token_env` for its bearer token. Examples of origins
are `https://<workspace-host>`, `https://<account>.snowflakecomputing.com`, and
`https://api.cloudflare.com`. Tokens are provisioned by the operator; Kelvo
does not refresh OAuth or generate Snowflake JWTs. Transport requires TLS 1.2 or newer
with certificate verification and disables environment proxies and redirects.

Only a conservative single SELECT/WITH statement is accepted. Typed parameters
are explicitly unsupported on these three adapters for now. Use provider
identities with minimum read permissions: SQL syntax restrictions cannot
replace source grants or prevent side effects inside privileged functions.

## Databricks

The adapter uses the [Statement Execution API](https://docs.databricks.com/api/workspace/statementexecution/executestatement)
with `options.warehouse_id`; `catalog` and `schema` are optional. It submits an
asynchronous statement, polls its handle, and validates contiguous result
chunks with `JSON_ARRAY` / `INLINE` disposition. Declared row counts, chunk
offsets and truncation flags must agree. It only fetches internal chunk paths
for the same statement.

Databricks limits inline results to **25 MiB**. Queries exceeding that limit,
Kelvo's output limit, or the per-response memory budget fail explicitly. This
initial connector is suitable for bounded query results; large raw exports
need a future external Arrow-result path. Kelvo does not follow external
storage links. See Databricks' [inline result documentation](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial).

## Snowflake

The adapter uses [SQL API v2](https://docs.snowflake.com/en/developer-guide/sql-api/reference).
Optional settings are `database`, `schema`, `warehouse`, `role` and
`token_type`. Token type is `OAUTH` (default), `KEYPAIR_JWT`, or
`PROGRAMMATIC_ACCESS_TOKEN`. It requests exactly one statement, polls the
returned handle, then reads partitions in order. Compressed partitions have a
bounded decompressed size; partition and total row counts must agree.

The mapper follows Snowflake's [documented JSON representations](https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses):
exact decimal strings, binary hex, epoch-day dates, and nanosecond epoch values
for NTZ/LTZ timestamps. TIME, TIMESTAMP_TZ, VARIANT, ARRAY and OBJECT are
currently unsupported. Explicitly cast/project these in SQL if needed; Kelvo
will not silently discard a timezone offset or stringify a nested value.

## Cloudflare D1

D1 uses its [raw query endpoint](https://developers.cloudflare.com/api/resources/d1/subresources/database/methods/raw/),
which provides ordered column names and array rows. Configure `account_id` and
`database_id`. The connector checks both top-level and statement-level success,
bounds the response, and validates inferred result types before Arrow delivery.
SQLite's dynamic types and JSON transport limit the available type metadata;
mixed incompatible values must fail rather than become NULL or silently round.

## Limits and validation

These APIs return JSON which Kelvo converts to Arrow. This is not Arrow-native
source transport. Decimal values must be exactly representable; integer widths
from typed provider schemas and NULL values are preserved. Unsupported types,
overflow, malformed pagination and incomplete results are errors. Timestamp
values outside Arrow's selected nanosecond range are unsupported.

Each HTTP response is capped at the smaller of 32 MiB and one quarter of the
query memory setting; D1 also applies the output-byte ceiling to its response.
This bounds response buffers, not total process RSS or remote warehouse memory.
Use container memory limits and provider query policies as well. Databricks and
Snowflake cancellation is best effort after a statement handle has been
received; a lost submission response can leave an unknown remote statement.
Provider-side query timeouts remain necessary.

HTTP contract tests use official response shapes and exercise exact values,
polling, pagination, failure handling, cancellation and resource limits. Live
Databricks, Snowflake and D1 acceptance requires operator-provided credentials
and has not been performed. No cloud-source throughput claim is made.

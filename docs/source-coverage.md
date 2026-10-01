# Source coverage

Kelvo uses the 44 engine identifiers in the reference database gateway as its
compatibility checklist. Engine coverage is separate from metadata browsing,
writes/migrations, CDC, and federation. Those APIs are not implied by this table.
Parquet is an additional Kelvo file source.

A **native** route executes through a built-in connector. A **protocol-family**
route shares a compatible database protocol but still needs acceptance against
that specific product. An **external adapter** route requires a separately
operated service that actually supports the configured connection. It is not a
bundled driver or evidence that the backend has been tested.

| Engine ID | Reference query path | Kelvo query path |
| --- | --- | --- |
| `postgresql` | SQL driver | Native PostgreSQL; DuckDB federation |
| `mysql` | SQL driver | Native MySQL; DuckDB federation |
| `mariadb` | MySQL driver | Native protocol-family |
| `sqlite` | SQL driver/file | DuckDB SQLite extension, read-only file |
| `duckdb` | Embedded SQL/file | DuckDB federation |
| `cockroachdb` | PostgreSQL driver | Native protocol-family |
| `sqlserver` | SQL driver | Native |
| `clickhouse` | Dedicated client | Native ArrowStream |
| `cosmosdb` | SQL driver | Native NoSQL query REST API; bounded JSON documents |
| `oracle` | SQL driver | Native |
| `dynamodb` | SQL driver | Native read-only PartiQL; lossless AttributeValue documents |
| `trino` | SQL driver | Native HTTPS statement protocol |
| `clickhouse_lambda` | Lambda SQL driver | External adapter |
| `alloydb` | PostgreSQL driver | Native protocol-family |
| `presto` | SQL driver | Native HTTPS statement protocol |
| `athena` | SQL driver | Native signed HTTPS query API |
| `hive` | External JDBC bridge | External adapter |
| `h2` | External JDBC bridge | External adapter |
| `ignite` | SQL driver | Native Ignite 2 REST SQL fields; same-node cursors |
| `spanner` | SQL driver | Native read-only REST SQL; bounded materialized results |
| `db2` | External JDBC bridge | External adapter |
| `exasol` | SQL driver | Native verified-TLS WebSocket SQL |
| `sap_hana` | Declared; connection builder missing | Custom external adapter required |
| `sap_ase` | Declared; connection builder missing | Custom external adapter required |
| `salesforce` | SQL/API driver | External adapter |
| `google_ads` | API adapter | External adapter |
| `facebook_ads` | API adapter | External adapter |
| `spark` | External JDBC bridge | External adapter |
| `d1` | HTTP/SQL driver | Native HTTPS API |
| `snowflake` | Warehouse adapter | Native SQL API |
| `bigquery` | Warehouse adapter | Native jobs API |
| `databricks` | Warehouse adapter | Native Statement Execution API |
| `redshift` | Warehouse adapter | Native PostgreSQL protocol-family |
| `mongodb` | Document adapter | Native aggregation and restricted SQL |
| `cassandra` | Wide-column adapter | External adapter |
| `scylla` | Wide-column adapter | External adapter |
| `elasticsearch` | Search adapter | Native SQL API |
| `csv` | DuckDB/file | DuckDB federation |
| `posthog` | API adapter | External adapter |
| `ga4` | API adapter | External adapter |
| `stripe` | API adapter | External adapter |
| `redis` | External JDBC bridge | External adapter |
| `arrow_flight` | External gateway | Native Flight SQL only |
| `google_sheets` | API snapshot into DuckDB | External adapter |

The native expansion is configured in [sources-native.yml](../deploy/examples/sources-native.yml).
These routes execute at one configured source; they do not add DuckDB federation,
metadata browsing, write APIs, or CDC. Athena retains query results in the configured
S3 location. DynamoDB and Cosmos DB preserve document payloads in Arrow Binary;
they do not infer a tabular schema. Consult the [source guides](usage.md#source-guides)
for explicit query, type, authentication and pagination limits.

## External adapters

Set `adapter: flightsql` to connect to a trusted Flight SQL service, or
`adapter: dbapi` for the reference gateway's connection-ID query API. Configure
each source explicitly; Kelvo never falls back to another service silently.
See [adapter configuration and limitations](sources-adapters.md).

The compatibility route carries received JSON objects in Arrow Binary values.
It cannot restore precision or types already lost in the upstream gateway, and
it buffers a bounded response. Prefer native typed Arrow paths for large exports.
Flight SQL preserves Arrow types but requires a real Flight SQL implementation;
an arbitrary Flight server or JDBC driver is not sufficient.

The two declared SAP entries are not working reference gateway connections.
Registering their names in Kelvo does not fix that: they require an independently
implemented external adapter. No default adapter binary or proprietary driver is
included in Kelvo.

## Validation boundaries

The [validation record](validation.md) separates live engine tests from protocol
fixtures, family compatibility, and unvalidated cloud accounts. The external
adapter contract has local TLS server tests; no claim is made that all 44 engines
were deployed and tested end to end. Every connector has its own supported SQL,
result types, cancellation behavior and limits. Consult its source documentation.

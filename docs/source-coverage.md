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
| `cosmosdb` | SQL driver | External adapter |
| `oracle` | SQL driver | Native |
| `dynamodb` | SQL driver | External adapter |
| `trino` | SQL driver | Native HTTPS statement protocol |
| `clickhouse_lambda` | Lambda SQL driver | External adapter |
| `alloydb` | PostgreSQL driver | Native protocol-family |
| `presto` | SQL driver | Native HTTPS statement protocol |
| `athena` | SQL driver | External adapter |
| `hive` | External JDBC bridge | External adapter |
| `h2` | External JDBC bridge | External adapter |
| `ignite` | SQL driver | External adapter |
| `spanner` | SQL driver | External adapter |
| `db2` | External JDBC bridge | External adapter |
| `exasol` | SQL driver | External adapter |
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

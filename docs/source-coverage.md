# Source coverage

Use this matrix to choose a query route. Query support does not imply metadata browsing, writes, CDC or live federation; each connector has its own limits.

**Native** means a built-in connector. **Protocol-family** shares a wire protocol but needs product-specific acceptance. **External adapter** needs a separately operated service with backend support. The checklist contains 44 engine IDs; Parquet is an additional file source.

| Engine ID | Kelvo query path |
| --- | --- |
| `postgresql` | Native PostgreSQL; DuckDB federation |
| `mysql` | Native MySQL; DuckDB federation |
| `mariadb` | Native protocol-family |
| `sqlite` | DuckDB SQLite extension, read-only file |
| `duckdb` | DuckDB federation |
| `cockroachdb` | Native protocol-family |
| `sqlserver` | Native; opt-in DuckDB bridge federation |
| `clickhouse` | Native ArrowStream; opt-in DuckDB bridge federation |
| `cosmosdb` | Native NoSQL query REST API; bounded JSON documents |
| `oracle` | Native; opt-in DuckDB bridge federation |
| `dynamodb` | Native read-only PartiQL; lossless AttributeValue documents |
| `trino` | Native HTTPS statement protocol |
| `clickhouse_lambda` | External adapter |
| `alloydb` | Native protocol-family |
| `presto` | Native HTTPS statement protocol |
| `athena` | Native signed HTTPS query API |
| `hive` | External adapter |
| `h2` | External adapter |
| `ignite` | Native Ignite 2 REST SQL fields; same-node cursors |
| `spanner` | Native read-only REST SQL; bounded materialized results |
| `db2` | External adapter |
| `exasol` | Native verified-TLS WebSocket SQL |
| `sap_hana` | Custom external adapter required |
| `sap_ase` | Custom external adapter required |
| `salesforce` | External adapter |
| `google_ads` | External adapter |
| `facebook_ads` | External adapter |
| `spark` | External adapter |
| `d1` | Native HTTPS API |
| `snowflake` | Native SQL API; opt-in DuckDB bridge federation |
| `bigquery` | Native jobs API; opt-in DuckDB bridge federation |
| `databricks` | Native Statement Execution API; opt-in DuckDB bridge federation |
| `redshift` | Native PostgreSQL protocol-family |
| `mongodb` | Native aggregation and restricted SQL |
| `cassandra` | External adapter |
| `scylla` | External adapter |
| `elasticsearch` | Native SQL API |
| `csv` | DuckDB federation |
| `posthog` | External adapter |
| `ga4` | External adapter |
| `stripe` | External adapter |
| `redis` | External adapter |
| `arrow_flight` | Native queries; opt-in [Flight SQL federation](federation-flight-sql.md) |
| `google_sheets` | External adapter |

Start with [native configuration](../deploy/examples/sources-native.yml) and the [source guides](usage.md#source-guides). The [native bridge](federation.md) supports selected tables from eight database engines and compatible Flight SQL services; [custom adapters](federation-adapters.md) use the public Go contract.

## Acceleration and federation capabilities

All 24 built-in native routes can feed full-refresh Arrow-to-Parquet snapshots when their query and [result types](acceleration.md#type-and-memory-boundaries) are supported.

**Eligible** describes the implemented refresh path, not live acceptance. **Verified** links a live fixture. **Conditional** adds adapter/result-shape requirements. **Live federation** queries the original source; joining stored snapshot aliases is separate.

| Sources | Full-refresh acceleration | Live federation | Acceleration validation |
| --- | --- | --- | --- |
| PostgreSQL, MySQL | Eligible | Yes; signed extensions or [custom Go adapters](federation.md) | Live acceleration acceptance pending; native/federation tests are separate |
| MariaDB, CockroachDB, AlloyDB, Redshift | Eligible; protocol-family compatibility | Not through these native routes | Product-specific acceleration acceptance pending |
| ClickHouse | Eligible | [Opt-in native bridge](federation.md); supported column/filter pushdown | Verified: [10-million-row snapshot and exact aggregates](evidence/acceleration-clickhouse.json) |
| SQL Server, Oracle | Eligible | [Opt-in native bridge](federation-adapters.md); selected tables and supported scalar types | Live acceleration acceptance pending; federation validation is separate |
| Snowflake, Databricks, BigQuery | Eligible | [Opt-in native bridge](federation-adapters.md); native API transfer/type limits apply | Live acceleration acceptance pending; federation validation is separate |
| Exasol, Ignite 2, Spanner, Athena, Elasticsearch | Eligible | No | Live acceleration acceptance pending |
| Trino, Presto | Eligible | No; the remote engine may itself federate | Live acceleration acceptance pending |
| Flight SQL | Eligible | [Opt-in native bridge](federation-flight-sql.md); explicit ANSI profile, projection only | Live acceleration acceptance pending |
| Cloudflare D1 | Conditional: value-inferred schema | No | Live acceleration acceptance pending |
| MongoDB | Eligible through restricted SQL or read-only aggregation pipeline; BSON documents stay binary | No | [Live MongoDB 8.0.32 refresh, exact values and rejected writes](evidence/mongodb-acceleration.json) |
| DynamoDB, Cosmos DB for NoSQL | Eligible; document payloads stay binary | No | Live acceleration acceptance pending |
| CSV, Parquet, DuckDB, SQLite | Eligible through a federated refresh query | Yes | [CSV-derived snapshots and typed Parquet round trips verified](evidence/acceleration-acceptance.json); other file sources as refresh inputs await acceptance |
| Engines reached through external adapters | Conditional on the configured service and returned types | No | All-provider acceleration acceptance pending |

Snapshots can be queried or joined as aliases backed by local Parquet or opt-in [object storage](object-storage.md). BSON/JSON/AttributeValue columns remain binary documents; snapshots do not infer relational fields.

MongoDB refresh accepts restricted SQL or read-only pipelines in YAML, rejecting `$out`/`$merge`. D1's inferred schema can yield unsupported Null columns for empty/all-NULL results, even with a source cast. Other unsupported snapshot types include zoned nanosecond timestamps, nested values and dictionaries. Choose an explicit supported representation or query the source directly.

**Incremental refresh and CDC are not implemented for any connector.** Benchmark snapshots against the workload before making latency or cost claims.

## External adapters

Configure `adapter: flightsql` for a real Flight SQL service or `adapter: dbapi` for the connection-ID gateway API. Routes are explicit; there is no automatic fallback. See [setup and limits](sources-adapters.md).

Flight SQL preserves typed Arrow results. The compatibility route buffers bounded JSON objects in Arrow Binary and cannot restore precision lost upstream. Neither service nor its proprietary drivers are bundled. The SAP entries require custom services because the reference gateway's connection builders are incomplete.

## Validation boundaries

The [validation record](validation.md) separates live acceptance, protocol fixtures, family compatibility and unverified cloud accounts. The external-adapter contract has TLS fixture coverage; all 44 engines have not been deployed and tested end to end.

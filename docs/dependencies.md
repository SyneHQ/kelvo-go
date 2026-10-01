# Dependency rationale

| Dependency | Purpose | License |
| --- | --- | --- |
| duckdb-go v2.10506.0 / DuckDB 1.5.6 | Embedded SQL execution, file analysis and supported federation | MIT |
| Arrow Go v18.5.1 | Typed record batches and IPC encoding/decoding | Apache-2.0 |
| [NATS Go v1.54.0](https://github.com/nats-io/nats.go/tree/v1.54.0) | Durable cluster jobs, KV state and authenticated broker connections | Apache-2.0 |
| [Go YAML v3.0.5](https://github.com/yaml/go-yaml/tree/v3.0.5) | Strict YAML source configuration | MIT and Apache-2.0, by file |
| [Microsoft SQL Server driver v1.9.8](https://github.com/microsoft/go-mssqldb/tree/v1.9.8) | Native SQL Server connector | BSD-3-Clause |
| [go-ora v2.9.0](https://github.com/sijms/go-ora/tree/v2.9.0) | Native Oracle connector | MIT |
| [MongoDB Go Driver v2.9.1](https://github.com/mongodb/mongo-go-driver/tree/v2.9.1) | Native MongoDB connector | Apache-2.0 |
| [zero-sql b01a7e8](https://github.com/SyneHQ/zero-sql/commit/b01a7e87002271a661ebd68060824be013347845) | Restricted MongoDB SQL compiler | Apache-2.0 |
| [pgx v5.11.0](https://github.com/jackc/pgx/tree/v5.11.0) | Native PostgreSQL protocol connections | MIT |
| [Go MySQL driver v1.10.1](https://github.com/go-sql-driver/mysql/tree/v1.10.1) | Native MySQL and MariaDB connections | MPL-2.0 |
| [gRPC Go v1.78.0](https://github.com/grpc/grpc-go/tree/v1.78.0) | Apache Arrow Flight SQL transport | Apache-2.0 |
| Go standard library | HTTP, subprocess lifecycle, configuration and CLI | Go BSD-style license |

Transitive dependencies are pinned by `go.sum`; review their licenses when distributing binaries. DuckDB's driver includes platform-specific native libraries. Matching signed extensions are external provisioned artifacts, with their own dependency notices.

The ClickHouse adapter uses HTTP and source-produced ArrowStream. Databricks,
Snowflake and Cloudflare D1 use their documented HTTPS APIs and convert bounded
JSON responses into Arrow batches. No provider SDK is required for those APIs.
BigQuery, Elasticsearch, Trino and Presto also use documented HTTPS APIs.
Flight SQL uses the existing Arrow dependency and gRPC for typed record-batch
transport. Optional compatibility adapters connect to separately operated
services; their database drivers and licenses are not bundled with Kelvo.
CDC remains a separate capability requiring its own acceptance evidence.

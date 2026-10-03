# Dependency rationale

Use this list when reviewing upgrades or distributing Kelvo. Versions are pinned in `go.mod`/`go.sum`; review transitive licenses and provisioned extension notices too.

| Dependency | Purpose | License |
| --- | --- | --- |
| duckdb-go v2.10506.0 / DuckDB 1.5.6 | Embedded SQL execution, file analysis and supported federation | MIT |
| Arrow Go v18.5.1 | Typed record batches and IPC encoding/decoding | Apache-2.0 |
| Arrow Go Parquet/pqarrow v18.5.1 | Bounded Parquet snapshot encoding with Snappy and stored Arrow schema | Apache-2.0 |
| [NATS Go v1.54.0](https://github.com/nats-io/nats.go/tree/v1.54.0) | Durable cluster jobs, KV state and authenticated broker connections | Apache-2.0 |
| [Go YAML v3.0.5](https://github.com/yaml/go-yaml/tree/v3.0.5) | Strict YAML source configuration | MIT and Apache-2.0, by file |
| [Microsoft SQL Server driver v1.9.8](https://github.com/microsoft/go-mssqldb/tree/v1.9.8) | Native SQL Server connector | BSD-3-Clause |
| [go-ora v2.9.0](https://github.com/sijms/go-ora/tree/v2.9.0) | Native Oracle connector | MIT |
| [MongoDB Go Driver v2.9.1](https://github.com/mongodb/mongo-go-driver/tree/v2.9.1) | Native MongoDB connector | Apache-2.0 |
| [zero-sql b01a7e8](https://github.com/SyneHQ/zero-sql/commit/b01a7e87002271a661ebd68060824be013347845) | Restricted MongoDB SQL compiler | Apache-2.0 |
| [pgx v5.11.0](https://github.com/jackc/pgx/tree/v5.11.0) | Native PostgreSQL protocol connections | MIT |
| [Go MySQL driver v1.10.1](https://github.com/go-sql-driver/mysql/tree/v1.10.1) | Native MySQL and MariaDB connections | MPL-2.0 |
| [gRPC Go v1.83.2](https://github.com/grpc/grpc-go/tree/v1.83.2) | Apache Arrow Flight SQL transport | Apache-2.0 |
| [AWS SDK for Go v2 core v1.47.1](https://github.com/aws/aws-sdk-go-v2/tree/v1.47.1) | Official SigV4 signer for source-bound Athena/DynamoDB and S3-compatible snapshot requests; no ambient credential provider chain | Apache-2.0 |
| [Exasol Go driver v1.1.1](https://github.com/exasol/exasol-driver-go/tree/v1.1.1) | DSN parsing for the exact native WebSocket connector | MIT |
| [Gorilla WebSocket v1.5.3](https://github.com/gorilla/websocket/tree/v1.5.3) | Bounded Exasol WebSocket transport | BSD-2-Clause |
| Go standard library | HTTP, subprocess lifecycle, configuration and CLI | Go BSD-style license |

ClickHouse supplies ArrowStream. The cloud, search, Trino/Presto, Spanner, Cosmos DB and Ignite connectors use documented APIs with bounded JSON conversion. Exasol uses its driver's DSN parser with Kelvo's exact-number WebSocket path, avoiding the stock driver's float64 decoding and background-context operations. Optional adapter services and their drivers remain external.

Object storage uses the AWS core signer for S3/R2/GCS XML and standard HTTPS with explicit Azure SAS tokens. No ambient credential chain is loaded. The Go parent owns cloud TLS and conditional reads; DuckDB's matching signed `httpfs` extension reads anonymous loopback ranges.

Native federation optionally adds a C++17 shim and scoped accessor patch against pinned DuckDB 1.5.6. Provisioning verifies its inputs and leaves baseline `go.mod` unchanged. It adds neither Arrow C++ nor a runtime service. See [build/upgrade requirements](federation.md#build-and-update) and [MIT notices](../licenses/duckdb.txt).

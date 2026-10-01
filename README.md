<p align="center">
  <img src="brand/kelvo-banner.png" alt="Kelvo — open-source analytics by SYNEHQ." width="100%">
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> · <a href="docs/usage.md">Documentation</a> · <a href="#execution-and-sources">Sources</a> · <a href="docs/validation.md">Benchmarks</a> · <a href="CONTRIBUTING.md">Contributing</a> · <a href="brand/README.md">Brand</a>
</p>

# Kelvo

**Query your databases. Get Arrow results.**

Kelvo is an open-source analytics gateway built in Go. Run native queries at the source, or use DuckDB to join supported sources. Deliver the results to your data tools as Apache Arrow IPC.

> **Developer preview.** Read the [validation record](docs/validation.md) for tested workloads, connector coverage, and current limits.

## Quick start

Kelvo requires Go 1.26 or newer, a C/C++ toolchain for cgo linking, and a supported DuckDB binary platform. Linux amd64 is the initial validation target.

```sh
git clone https://github.com/SYNEHQ/kelvo-go.git
cd kelvo-go
go build -tags duckdb_arrow -o bin/kelvo ./cmd/kelvo

bin/kelvo query --config examples/kelvo.yml --sources sales \
  --sql 'SELECT region, SUM(amount::DECIMAL(18,2)) AS revenue FROM sales GROUP BY region ORDER BY region' \
  --out revenue.arrow
```

The result is Arrow IPC, not JSON. Read the full [usage guide](docs/usage.md) for HTTP lifecycle, configuration, parameters, limits, and cluster commands.

## Execution and sources

| Path | Sources | Scope |
| --- | --- | --- |
| DuckDB federation | CSV, Parquet, DuckDB, SQLite, PostgreSQL, MySQL | Join selected sources with DuckDB SQL. Execution materializes before Arrow delivery. |
| Native connectors | ClickHouse, PostgreSQL/MySQL families, SQL Server, Oracle, MongoDB, Snowflake, Databricks, BigQuery, D1, Trino/Presto, Elasticsearch, Flight SQL | Query one configured connection using its supported SQL or protocol. |
| External adapters | Additional engines through an explicitly configured service | Connect a separately operated `dbapi` or Flight SQL adapter. Drivers are not bundled. |

The [44-engine routing matrix](docs/source-coverage.md) distinguishes native connectors, protocol families, and external adapters. It is a coverage checklist, not 44 live-validated databases. Find provider setup, supported types, and query examples in the [source guides](docs/usage.md#source-guides).

## Security and cluster operation

Configuration references environment-variable names, never credentials. Sources need dedicated read-only identities; requests name only registered sources. Query workers have deadlines, output limits, cancellation, and single-use result delivery. Deployment still needs its own filesystem, network, process, and resource isolation.

`serve` is a single-trust-domain mode. Cluster mode uses tenant-bound worker pools, NATS JetStream admission and mTLS result delivery; it does not create automatic per-user data policies or split a SQL plan across machines. See [cluster lifecycle](docs/cluster.md) and the [tenant deployment example](deploy/README.md).

## Evidence and limits

The [validation record](docs/validation.md) separates executed checks from protocol fixtures and unvalidated live-provider paths. It includes the exact scope of the local 10-million-row ClickHouse export measurement: **1.18–1.48 million rows/s**, including file persistence, on one native VM workload. It is not a general throughput or memory guarantee.

Native engine limits and Arrow output limits are not a hard process-RSS boundary; use deployment-level limits as well. CDC, durable exports, a Flight SQL server, and production HA certification remain outside the current release.

## Build with us

Connector improvements, type-fidelity fixes, reproducible performance work, and documentation are welcome. Start with [contributing](CONTRIBUTING.md) and keep changes focused.

```sh
go test -tags duckdb_arrow ./...
go vet -tags duckdb_arrow ./...
go build -tags duckdb_arrow -o bin/kelvo ./cmd/kelvo
```

Explore the [architecture](docs/architecture.md), [security model](SECURITY.md), [dependency rationale](docs/dependencies.md), and [DuckDB upgrade process](docs/upgrading-duckdb.md).

Kelvo is [Apache-2.0 licensed](LICENSE). DuckDB and its Go client are MIT licensed; Arrow is Apache-2.0 licensed. See [NOTICE](NOTICE).

Built by [SYNEHQ](https://synehq.com).

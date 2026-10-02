<p align="center">
  <img src="brand/kelvo-banner.png" alt="Kelvo — Your databases. One query gateway. Native execution. DuckDB federation. Arrow results." width="100%">
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
| Optional native federation | ClickHouse, PostgreSQL and MySQL tables through the pinned Go/C++ Arrow bridge | Source column/filter pushdown; joins and aggregates in DuckDB. [Build and limits](docs/federation.md). |
| Native connectors | ClickHouse, PostgreSQL/MySQL families, SQL Server, Oracle, MongoDB, Snowflake, Databricks, BigQuery, D1, Trino/Presto, Elasticsearch, Exasol, Spanner, Ignite 2, Athena, DynamoDB, Cosmos DB, Flight SQL | Query one configured connection using its supported SQL or protocol. |
| External adapters | Additional engines through an explicitly configured service | Connect a separately operated `dbapi` or Flight SQL adapter. Drivers are not bundled. |

The [44-engine routing matrix](docs/source-coverage.md) distinguishes native connectors, protocol families, and external adapters. It is a coverage checklist, not 44 live-validated databases. Find provider setup, supported types, and query examples in the [source guides](docs/usage.md#source-guides).

## Dataset acceleration

Enable acceleration for selected datasets to serve repeated analytics from persistent Parquet snapshots. Native connectors feed the same snapshot path when their result types are supported; the source does not need a lakehouse. Query or join snapshot aliases with DuckDB. Original connections remain available for live queries.

Every full refresh reruns the configured source query and consumes source resources. Queries using only accelerated aliases avoid source reads between refreshes. Configure freshness and resource limits in YAML; use local scheduling or tenant-scoped NATS jobs in cluster mode. Refreshes publish complete generations while existing readers keep their pinned files. See [acceleration setup and limits](docs/acceleration.md) and [capability indicators](docs/source-coverage.md#acceleration-and-federation-capabilities).

```sh
bin/kelvo accelerate refresh --config examples/acceleration.yml --dataset sales_fast
bin/kelvo query --config examples/acceleration.yml --sources sales_fast \
  --sql 'SELECT region, SUM(amount) FROM sales_fast GROUP BY region' --out revenue.arrow
```

Snapshots default to local storage. Opt into [S3, R2, GCS or Azure Blob](docs/object-storage.md) for shared object storage with separate reader/publisher credentials and bounded range reads. MongoDB refreshes also accept read-only aggregation pipelines in YAML.

This is full-refresh dataset acceleration. Incremental loading, CDC and query-result caching remain separate work.

## Security and cluster operation

Configuration references environment-variable names, never credentials. Sources need dedicated read-only identities; requests name only registered sources. Query workers have deadlines, output limits, cancellation, and single-use result delivery. Deployment still needs its own filesystem, network, process, and resource isolation.

`serve` is a single-trust-domain mode. Cluster mode uses tenant-bound worker pools, NATS JetStream admission and mTLS result delivery; it does not create automatic per-user data policies or split a SQL plan across machines. See [cluster lifecycle](docs/cluster.md) and the [tenant deployment example](deploy/README.md).

## Real analytics, measured

Four ride-hailing-style workflows analyze **22,612,607 real NYC Taxi trips** using CTEs, joins, rolling windows and deterministic rankings. **All 84 measured runs passed exact-result validation**, following 28 separately validated preflights.

Times below are **median seconds across three sequential trials**, from fresh process startup through Arrow output persistence and exit. Python imports are included. The [full report](docs/analytics-workflow-benchmarks.md) includes every trial, timing ranges, source bytes, memory, SQL and reproduction commands.

**Identical local Parquet on Azure:** two execution threads, a 1536 MiB service cap, LZ4 Arrow output, DuckDB 1.5.6 and Polars 1.44.2.

| Workflow | Kelvo / DuckDB | DuckDB / Python | Polars / Python |
| --- | ---: | ---: | ---: |
| Daily KPIs + 7-day windows | 0.484 | 0.634 | 0.636 |
| Hourly borough hotspots | 1.035 | 1.052 | 1.464 |
| Route joins + distance mix | 1.497 | 1.816 | 1.064 |
| Monthly zone momentum | 0.606 | 0.691 | 0.749 |

Kelvo and direct DuckDB use the same engine version; process startup and binding costs contribute to their differences. Polars uses equivalent native LazyFrame expressions and is fastest on the route workflow. These results support comparison of the tested complete commands, including their overhead.

**Live ClickHouse source on Azure:** both native paths execute SQL in ClickHouse. Federation fetches source rows and computes in DuckDB on the named Kelvo host. Oracle is an Always Free `VM.Standard.E2.1.Micro` with 951 MiB RAM and a burstable 1/8 OCPU entitlement; its source connection uses SSH forwarding.

| Workflow | Azure native | Oracle native | Azure federation | Oracle federation |
| --- | ---: | ---: | ---: | ---: |
| Daily KPIs + 7-day windows | 0.627 | 1.480 | 1.508 | 33.490 |
| Hourly borough hotspots | 2.171 | 3.727 | 4.096 | 42.922 |
| Route joins + distance mix | 1.822 | 2.358 | 3.180 | 62.275 |
| Monthly zone momentum | 0.911 | 1.630 | 1.861 | 40.274 |

All **24 Oracle runs** completed under a **640 MiB service cap**. Across these runs, peak sampled combined process RSS was **379.3 MiB** and peak charged cgroup memory was **347.4 MiB**. Source database and SSH memory are outside that cap. Azure's federated hotspot query reached its 640 MiB charged-memory cap and spilled to disk, so that profile has no demonstrated memory headroom. The report explains shared-page accounting, warm caches, returned versus fetched rows, and the limits of short trials on a burstable VM.

## Evidence and limits

The [validation record](docs/validation.md) separates executed checks from protocol fixtures and unvalidated live-provider paths. It includes the exact scope of the local 10-million-row ClickHouse export measurement: **1.18–1.48 million rows/s**, including file persistence, on one native VM workload. It is not a general throughput or memory guarantee.

On an **Oracle Cloud Always Free micro VM** (`VM.Standard.E2.1.Micro`, 951 MiB visible RAM), ten simultaneous native exports with opt-in LZ4 completed **30 of 30 four-million-row exports: 120 million verified rows at 443,000 aggregate returned rows/s**. The service stayed within its 640 MiB memory cap. ClickHouse ran on a separate Azure VM; client delivery used SSH tunnels. Ten simultaneous federated sorts exhausted that same cap. See the [micro VM measurements, memory accounting and limits](docs/oracle-micro-capacity.md) before sizing a deployment.

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

<p align="center">
  <img src="brand/kelvo-banner.png" alt="Kelvo — Your databases. One query gateway. Native execution. DuckDB federation. Arrow results." width="100%">
</p>

<p align="center">
  <a href="#try-it-in-a-notebook">Try in Colab</a> · <a href="#quick-start">Quick start</a> · <a href="#documentation">Documentation</a> · <a href="#how-kelvo-fits">Architecture</a> · <a href="#execution-and-sources">Sources</a> · <a href="#real-analytics-measured">Benchmarks</a> · <a href="#build-with-us">Contributing</a>
</p>

# Kelvo

**Query your databases. Get Arrow results.**

Kelvo is an open-source analytics gateway built in Go by [SYNEHQ](https://synehq.com). Run queries in the source database, join supported sources with DuckDB, and reuse persistent Parquet datasets for repeated analysis. Deliver typed Apache Arrow results to your notebooks, agents, dashboards, APIs and workflows.

> **Developer preview.** The [validation record](docs/validation.md) documents tested workloads and connector coverage. The [delivery status](docs/production-roadmap.md#delivery-status) tracks implemented capabilities and remaining production work.

## Quick start

You need Go 1.26 or newer, a C/C++ toolchain for cgo linking, and a supported DuckDB binary platform. Linux amd64 is the initial validation target. This example queries the included CSV:

```sh
git clone https://github.com/SYNEHQ/kelvo-go.git
cd kelvo-go
go build -tags duckdb_arrow -o bin/kelvo ./cmd/kelvo

bin/kelvo query --config examples/kelvo.yml --sources sales \
  --sql 'SELECT region, SUM(amount::DECIMAL(18,2)) AS revenue FROM sales GROUP BY region ORDER BY region' \
  --out revenue.arrow
```

Open `revenue.arrow` with [PyArrow or another Arrow IPC reader](docs/usage.md#quick-start). The CLI writes statistics to standard error and publishes a complete result file on success.

[Serve the HTTP API](docs/usage.md#http-api) · [Configure your sources](docs/usage.md#configuration) · [Set limits](docs/usage.md#limits) · [Enable LZ4 results](docs/usage.md#opt-in-result-compression)

## Try it in a notebook

[**15 hands-on Colab and Jupyter notebooks**](notebooks/README.md) take you from a first query to joins, CTEs, windows, charts, Arrow batches, compression, accelerated snapshots and the authenticated API. Every lesson includes setup, real public data, answer checks and cleanup. Default paths need no database account or GPU.

| Start here | Dataset | Open |
| --- | --- | --- |
| First query and typed Arrow results | 344 Palmer Penguins observations | [Google Colab](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/01-first-query.ipynb) |
| Analytical SQL over millions of trips | January 2024 NYC Taxi Parquet, about 50 MB | [Google Colab](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/03-taxi-parquet-exploration.ipynb) |
| Use Kelvo from your application | Authenticated loopback HTTP gateway | [Google Colab](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/15-authenticated-http-gateway.ipynb) |

[Browse all lessons](notebooks/README.md) · [Clone for another Jupyter environment](notebooks/README.md#run-in-colab-or-another-jupyter-environment) · [Dataset sources and terms](notebooks/README.md#public-datasets-and-attribution)

## Documentation

Choose a starting point below, or browse the [complete documentation index](docs/README.md) for individual source guides, recovery procedures and validation tools.

| I want to… | Start here |
| --- | --- |
| Try real analytical workflows | [15 runnable notebooks](notebooks/README.md) · [Colab quick start](https://colab.research.google.com/github/SYNEHQ/kelvo-go/blob/cargo/notebooks/01-first-query.ipynb) |
| Query a database or integrate the API | [CLI and HTTP API](docs/usage.md) · [YAML examples](examples/) · [Source setup guides](docs/usage.md#source-guides) |
| Join data across sources | [Source capabilities](docs/source-coverage.md#acceleration-and-federation-capabilities) · [Native federation](docs/federation.md) · [Query examples](examples/federation-relational.yml) |
| Reuse datasets between source refreshes | [Acceleration](docs/acceleration.md) · [Multipart snapshots](docs/multipart-acceleration.md) · [Schema evolution](docs/schema-evolution.md) · [Backup and recovery](docs/snapshot-backup.md) |
| Deploy and operate workers | [Deployment](deploy/README.md) · [Cluster lifecycle](docs/cluster.md) · [Resource controls and operations](docs/operations.md) · [Tracing](docs/tracing.md) |
| Evaluate performance and reliability | [Validation record](docs/validation.md) · [Analytics benchmarks](docs/analytics-workflow-benchmarks.md) · [Micro VM capacity](docs/oracle-micro-capacity.md) · [Storage release gates](docs/storage-conformance.md) · [Operational acceptance](docs/operational-acceptance.md) |
| Build an adapter or contribute | [Adapter guide](docs/federation-adapters.md) · [Contributing](CONTRIBUTING.md) · [DuckDB upgrades](docs/upgrading-duckdb.md) · [Roadmap](docs/production-roadmap.md) · [Production checklist](docs/production-status.md) |

## How Kelvo fits

Put Kelvo between your data sources and the software you build. Keep execution at the source where it fits; use local DuckDB for cross-source joins and analytical SQL. Optional snapshots reduce repeated source reads, and tenant-bound worker pools run independent queries across nodes.

[![Kelvo architecture: applications submit queries to a Go coordinator; disposable workers use native connectors or DuckDB and deliver Arrow results. Optional immutable snapshots support schema policy, refresh and local recovery. NATS handles cluster dispatch; mTLS carries results. Worker controls cover admission, readiness, metrics and drain.](brand/kelvo-architecture.png)](brand/kelvo-architecture.svg)

[Architecture walkthrough](docs/architecture.md) · [Full-size diagram](brand/kelvo-architecture.svg) · [Security boundaries](SECURITY.md) · [Dependency choices](docs/dependencies.md)

## Execution and sources

| Path | Available inputs | Guide |
| --- | --- | --- |
| DuckDB federation | CSV, Parquet, DuckDB, SQLite, PostgreSQL and MySQL | [Configuration and attachments](docs/usage.md#configuration) |
| Optional native federation | ClickHouse, PostgreSQL, MySQL, SQL Server, Oracle, Snowflake, BigQuery and Databricks through the pinned Go/C++ Arrow bridge | [Build, setup and pushdown](docs/federation.md) |
| Native queries | ClickHouse, PostgreSQL/MySQL families, SQL Server, Oracle, MongoDB, Snowflake, Databricks, BigQuery, D1, Trino/Presto, Elasticsearch, Exasol, Spanner, Ignite 2, Athena, DynamoDB, Cosmos DB and Flight SQL | [Provider setup and supported types](docs/usage.md#source-guides) |
| External adapters | Additional engines through an explicitly configured `dbapi` or Flight SQL service | [Adapter services](docs/sources-adapters.md) |

The [44-engine routing matrix](docs/source-coverage.md) distinguishes built-in connectors, compatible protocol families and external adapters, with their validation status. Connector availability does not imply live federation support or tested compatibility with every related database.

Native mode executes one source's supported SQL or request. DuckDB handles federated joins and aggregates; the optional bridge pushes down selected columns and [supported typed predicates](docs/federation.md#what-runs-where). The pinned DuckDB path completes execution before Arrow delivery, and independently queried sources have no shared transaction snapshot.

To add a federation source, implement the public Go [`federation.Driver`](federation/federation.go) interface. The [adapter guide](docs/federation-adapters.md) covers typed scan plans, Arrow ownership, a working example and validation requirements.

## Dataset acceleration

Serve repeated analytics from persistent Parquet snapshots of selected datasets. Native connectors feed the same snapshot path when their result types are supported. Query or join accelerated aliases with DuckDB while keeping original connections available for live queries.

```sh
bin/kelvo accelerate refresh --config examples/acceleration.yml --dataset sales_fast
bin/kelvo query --config examples/acceleration.yml --sources sales_fast \
  --sql 'SELECT region, SUM(amount) FROM sales_fast GROUP BY region' --out revenue.arrow
```

Each full refresh reruns the configured source query. Queries using only accelerated aliases avoid source reads between refreshes. Set freshness and resource limits in YAML, then choose manual refresh, local scheduling or tenant-scoped NATS dispatch. Refreshes publish complete immutable generations while existing readers keep their pinned data.

- **Store and read datasets:** use local snapshots or opt into [S3, R2, GCS or Azure Blob](docs/object-storage.md). [Multipart snapshots](docs/multipart-acceleration.md) split larger generations into bounded Parquet parts.
- **Control schema and access:** schemas are strict by default, with optional [nullable additions and conservative type widening](docs/schema-evolution.md). [Authorization versions](docs/acceleration.md#tenant-and-cluster-operation) invalidate snapshots after source policy changes.
- **Recover deliberately:** inspect and restore retained [local](docs/acceleration.md#schema-contracts-and-generation-recovery) or [remote](docs/operations.md#remote-generation-inventory-and-restore) generations. Linux [verified backups](docs/snapshot-backup.md) copy a current local dataset into a new private root, preserving its identity and original age.

[Acceleration setup](docs/acceleration.md) · [MongoDB pipeline refresh](docs/acceleration.md#mongodb-pipeline-refresh) · [Refresh failures and recovery](docs/operations.md#source-refresh-failures-and-recovery)

This is full-refresh dataset acceleration. Incremental loading, CDC and query-result caching remain separate work.

## Security and cluster operation

Source catalogs name credential references. Workers resolve selected source credentials from the environment or optional [private credential files](docs/operations.md#file-based-source-credential-rotation). Use database-enforced read-only accounts and deployment-level filesystem, network, process and resource isolation.

`serve` operates within one trust domain. Cluster mode uses tenant-bound worker pools, NATS JetStream admission and mTLS result delivery. It distributes independent queries; each SQL plan stays on one node. Tenant catalog authorization does not create per-user row or column policies. Start with the [security model](SECURITY.md), [deployment example](deploy/README.md) and [cluster lifecycle](docs/cluster.md).

- **Control load:** configure [node resource reservations](docs/operations.md), [shared source quotas](docs/operations.md#shared-source-quotas), query deadlines and output limits. [CSV buffer options](docs/csv-memory.md) help tune supported inputs on constrained workers.
- **Observe work:** collect authenticated [metrics and resource diagnostics](docs/operations.md#diagnostics), inspect [passive native-source observations](docs/source-health.md), opt into bounded [execution history](docs/operations.md#optional-execution-history), or export sampled [OpenTelemetry lifecycle traces](docs/tracing.md).
- **Maintain workers:** use [graceful drain](docs/operations.md#maintenance) and [required-dataset readiness](docs/operations.md#dataset-diagnostics-and-required-readiness). Diagnose [worker failures](docs/worker-failures.md) and [classified source errors](docs/native-error-classification.md) before resetting failed refreshes.

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

Connector improvements, type-fidelity fixes, reproducible performance work and documentation are welcome. Start with [contributing](CONTRIBUTING.md), choose a focused change, and use [the roadmap's delivery status](docs/production-roadmap.md#delivery-status) to distinguish existing capabilities from upcoming work.

```sh
go test -tags duckdb_arrow ./...
go vet -tags duckdb_arrow ./...
go build -tags duckdb_arrow -o bin/kelvo ./cmd/kelvo
```

[Contributor guide](CONTRIBUTING.md) · [Adapter SDK](docs/federation-adapters.md) · [DuckDB upgrade process](docs/upgrading-duckdb.md) · [Brand assets](brand/README.md)

Kelvo is [Apache-2.0 licensed](LICENSE). DuckDB and its Go client are MIT licensed; Arrow is Apache-2.0 licensed. See [NOTICE](NOTICE) for attribution.

Built by [SYNEHQ](https://synehq.com).

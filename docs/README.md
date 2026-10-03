# Kelvo documentation

[Project overview](../README.md) · [Quick start](usage.md#quick-start) · [Architecture](architecture.md) · [Source coverage](source-coverage.md) · [Delivery status](production-roadmap.md#delivery-status)

Use this index to find a setup guide, operating procedure or measured result. Kelvo is a developer preview; each connector and feature guide records its supported behavior and validation limits.

## Start and integrate

| Guide | What you will find |
| --- | --- |
| [CLI and HTTP API](usage.md) | Query submission, result retrieval, parameters, cancellation and configuration |
| [Configuration examples](../examples/) | YAML catalogs for local files, federation, acceleration and object storage |
| [Result limits and memory](usage.md#limits) | Row, byte, timeout and engine limits, with process-memory boundaries |
| [LZ4 result compression](usage.md#opt-in-result-compression) | Opt-in Arrow compression and compatible client requirements |
| [Architecture](architecture.md) | Query and result paths, worker isolation, snapshots and cluster boundaries |
| [Security](../SECURITY.md) | Tenant boundaries, source grants, sandbox requirements and vulnerability reporting |

## Connect and federate data

Start with the [source coverage matrix](source-coverage.md) and [acceleration/federation capabilities](source-coverage.md#acceleration-and-federation-capabilities). A native connector, a compatible protocol family and an external adapter have different setup and acceptance requirements.

[Native federation](federation.md) covers the optional Go/C++ Arrow bridge, registered tables, supported pushdown and scan limits. [Building a federation adapter](federation-adapters.md) documents the public Go interface and contribution contract.

| Source | Guide |
| --- | --- |
| CSV, Parquet and DuckDB | [File sources and configuration](usage.md#configuration) · [CSV memory options](csv-memory.md) |
| PostgreSQL and MySQL families | [Relational sources](sources-relational.md) |
| ClickHouse | [Native and federated configuration](../examples/federation.yml) · [Native query usage](usage.md#native-and-federated-queries) |
| SQL Server and Oracle | [SQL sources](sources-sql.md) |
| Databricks, Snowflake and Cloudflare D1 | [Cloud sources](sources-cloud.md) |
| BigQuery | [Jobs API and authentication](sources-bigquery.md) |
| MongoDB | [Restricted SQL and aggregation](sources-mongodb.md) |
| Trino and Presto | [Statement protocol](sources-trino.md) |
| Elasticsearch | [SQL API](sources-elasticsearch.md) |
| Exasol | [WebSocket SQL](sources-exasol.md) |
| Google Spanner | [Read-only REST SQL](sources-spanner.md) |
| Apache Ignite 2 | [REST SQL fields](sources-ignite.md) |
| Athena and DynamoDB | [AWS query APIs](sources-aws.md) |
| Cosmos DB for NoSQL | [Documents and query budgets](sources-cosmosdb.md) |
| Flight SQL | [Client connections](sources-flight.md) |
| SQLite | [Read-only attachments](sources-sqlite.md) |
| Additional engines | [External adapter services](sources-adapters.md) |

## Manage accelerated datasets

| Guide | What you will find |
| --- | --- |
| [Dataset acceleration](acceleration.md) | Source queries, schedules, freshness, authorization and immutable publication |
| [MongoDB refresh pipelines](acceleration.md#mongodb-pipeline-refresh) | Read-only aggregation pipelines as refresh input |
| [Multipart snapshots](multipart-acceleration.md) | Bounded Parquet parts, local/remote storage and reader compatibility |
| [Schema evolution](schema-evolution.md) | Strict defaults, optional nullable additions and conservative widening |
| [Object storage](object-storage.md) | S3, R2, GCS and Azure Blob credentials, permissions and range reads |
| [Local backup and recovery](snapshot-backup.md) | Verified copies into fresh roots and recovery after source/store loss |
| [Retained generation restore](acceleration.md#schema-contracts-and-generation-recovery) | Local inventory, generation preconditions and freshness preservation |
| [Remote generation restore](operations.md#remote-generation-inventory-and-restore) | Bounded remote inventory, validation and manifest upgrades |
| [Refresh failure recovery](operations.md#source-refresh-failures-and-recovery) | Durable failure state, retry classification and operator reset |

## Deploy, observe and troubleshoot

| Guide | What you will find |
| --- | --- |
| [Tenant deployment example](../deploy/README.md) | Host prerequisites, containers, identities, mounts and network boundaries |
| [Cluster lifecycle](cluster.md) | NATS JetStream dispatch, ownership, delivery, cancellation and worker loss |
| [Resource admission](operations.md) | Shared query/refresh budgets and protected interactive capacity |
| [Source quotas](operations.md#shared-source-quotas) | Distributed limits on concurrent source work |
| [Metrics and diagnostics](operations.md#diagnostics) | Authenticated worker metrics and aggregate resource state |
| [Readiness](operations.md#dataset-diagnostics-and-required-readiness) · [Drain](operations.md#maintenance) | Required datasets, probes and bounded shutdown |
| [Credential-file rotation](operations.md#file-based-source-credential-rotation) | Selected source credentials, private files and cache TTL |
| [Execution history](operations.md#optional-execution-history) · [Lifecycle tracing](tracing.md) | Optional bounded history and OTLP/HTTP spans |
| [Worker failure recovery](worker-failures.md) | Memory failures, quota loss and snapshot preservation |
| [Native error classification](native-error-classification.md) | Recognized PostgreSQL/MySQL driver failures and public errors |
| [Federation scan diagnostics](federation.md#actual-scan-diagnostics) | Actual source rows/bytes and bounded redacted scan information |

## Evaluate and reproduce results

| Guide | What you will find |
| --- | --- |
| [Validation record](validation.md) | Executed checks, live-provider coverage and remaining acceptance work |
| [Analytics workflow benchmarks](analytics-workflow-benchmarks.md) | NYC Taxi CTEs, joins and windows; Kelvo, DuckDB and Polars comparisons |
| [Federation capacity](federation-capacity.md) | Multiple adapters, joins, tenant concurrency and remote transfer |
| [Oracle micro VM measurements](oracle-micro-capacity.md) | Native/federated workloads, result compression and memory accounting |
| [ClickHouse benchmark helper](benchmarking.md) | Reproducible native export fixture and measured timing boundaries |
| [CSV buffer experiment](csv-memory.md#validation-and-measurement) | Managed-memory settings, RSS observations and exact-result checks |
| [Backup recovery gate](snapshot-backup.md#reproduce-the-recovery-gate) | One-million-row local recovery and explicit measurement scope |
| [Storage release gates](storage-conformance.md) | Local/remote correctness, TLS fixtures and datasets above 4 GiB |

Reports link their [raw evidence](evidence/), versions, failures and reproduction commands. The [CTE workflow plan](cte-workflow-plan.md) records the benchmark design; the measured report records its outcomes.

## Extend and contribute

[Contributing](../CONTRIBUTING.md) · [Public federation interface](../federation/federation.go) · [Adapter guide](federation-adapters.md) · [Dependency rationale](dependencies.md) · [DuckDB upgrades](upgrading-duckdb.md) · [Production roadmap](production-roadmap.md) · [Brand assets](../brand/README.md)

The roadmap separates [delivered capabilities](production-roadmap.md#delivery-status) from remaining milestones. See [LICENSE](../LICENSE) and [NOTICE](../NOTICE) for licensing and attribution.

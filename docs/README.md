# Kelvo documentation

Start with the [quick start](usage.md#quick-start) or [15 notebooks](../notebooks/README.md). Kelvo is a developer preview; check each guide's supported behavior and evidence.

## Start and integrate

| Task | Guide |
| --- | --- |
| Run queries, read Arrow, use HTTP | [Usage](usage.md) · [YAML examples](../examples/) |
| Choose a database connector | [Coverage matrix](source-coverage.md) · [Source setup guides](usage.md#source-guides) |
| Understand execution and isolation | [Architecture](architecture.md) · [Security](../SECURITY.md) |
| Join live sources | [Federation](federation.md) · [Adapter SDK](federation-adapters.md) |
| Tune output and CSV memory | [LZ4 and limits](usage.md#opt-in-result-compression) · [CSV buffers](csv-memory.md) |

## Accelerate and recover datasets

| Task | Guide |
| --- | --- |
| Configure full refresh | [Acceleration](acceleration.md) |
| Store larger generations | [Multipart snapshots](multipart-acceleration.md) |
| Allow schema changes | [Schema evolution](schema-evolution.md) |
| Use S3, R2, GCS or Azure Blob | [Object storage](object-storage.md) · [Protected readers](protected-object-readers.md) · [Writer shutdown](object-writer-shutdown.md) |
| Verify protected snapshots | [Budgets and inventory](protected-verification.md) |
| Back up or migrate snapshots | [Backup and recovery](snapshot-backup.md) |
| Restore or reset failed refreshes | [Generation restore](acceleration.md#schema-contracts-and-generation-recovery) · [Refresh recovery](operations.md#source-refresh-failures-and-recovery) |

## Deploy, observe and troubleshoot

| Task | Guide |
| --- | --- |
| Deploy tenant workers | [Deployment](../deploy/README.md) · [Cluster lifecycle](cluster.md) |
| Enforce native process-tree limits | [Linux containment](process-containment.md) |
| Configure budgets, probes, quotas and drain | [Operations](operations.md) |
| Restrict users and services | [Principal keys](principal-access.md) · [Row/column policies](row-column-access.md) · [Snapshot policies](guarded-snapshots.md) |
| Verify protected query isolation | [Authenticated query acceptance](protected-query-acceptance.md) |
| Rotate credentials | [API keys](gateway-key-rotation.md) · [Source files](operations.md#file-based-source-credential-rotation) · [Cloud source secrets](cloud-secrets.md) · [TLS identity](tls-identity-rotation.md) · [TLS trust](tls-trust-rotation.md) |
| Retain security and execution receipts | [Durable local audit](durable-audit.md) |
| Inspect query/refresh outcomes | [Tracing](tracing.md) · [Passive source health](source-health.md) · [Native errors](native-error-classification.md) |
| Recover failed workers and scratch | [Worker failures](worker-failures.md) · [Managed scratch](worker-scratch.md) |
| Run exports and repeat downloads | [Durable exports](exports.md) · [Storage API](export-storage.md) · [Admission](workload-admission.md) |

## Evaluate and reproduce results

| Evidence | Guide |
| --- | --- |
| Current implementation and test coverage | [Production checklist](production-status.md) · [Validation record](validation.md) |
| CTEs, windows and analytical-library comparisons | [Analytics results](analytics-workflow-benchmarks.md) · [Workflow design](cte-workflow-plan.md) |
| Multi-adapter joins and small VMs | [Federation capacity](federation-capacity.md) · [Oracle worker profile](node-capacity.md) · [Earlier standalone trials](oracle-micro-capacity.md) |
| Telemetry cost and tracing correctness | [Paired measurements](telemetry-overhead.md) · [OTLP acceptance](tracing-acceptance.md) |
| Native export benchmark | [ClickHouse runner](benchmarking.md) |
| Mixed load and fault recovery | [Operational acceptance](operational-acceptance.md) · [Process loss](process-loss-acceptance.md) |
| Storage and version compatibility | [Storage gates](storage-conformance.md) · [Snapshot upgrades](release-upgrades.md) · [Rolling applications](rolling-upgrades.md) |

Reports link [raw evidence](evidence/), exact revisions, failures and reproduction commands.

## Extend and contribute

[Contributing](../CONTRIBUTING.md) · [Dependencies](dependencies.md) · [DuckDB upgrades](upgrading-duckdb.md) · [Native bridge](../internal/duckbridge/README.md) · [ClickHouse internals](../internal/sources/clickhouse/README.md) · [Brand assets](../brand/README.md)

## Delivery and release engineering

[Roadmap](production-roadmap.md) · [Delivery process](delivery-process.md) · [Project board](https://github.com/orgs/SyneHQ/projects/3) · [PR template](../.github/pull_request_template.md)

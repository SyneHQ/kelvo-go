# Production delivery checklist

Status reviewed 4 October 2026. This is a capability checklist, not production certification. Owners and acceptance criteria live on the [board](https://github.com/orgs/SyneHQ/projects/3) and [tracker #32](https://github.com/SyneHQ/kelvo-go/issues/32).

## Operational foundation

| Available | Still needed |
| --- | --- |
| Shared query/export/refresh budgets, interactive reserves and source quotas | Sustained mixed-workload capacity gates |
| [Pinned two-hour campaign (#7)](sustained-acceptance.md#recorded-two-hour-run) on `0544d5f`: all ten gates, exact source/binary checks and independent cleanup passed | Sustained acceptance for later releases; joins/exports, WAN, live providers and deployment capacity need separate evidence |
| [Pinned worker-capacity gate (#8)](node-capacity.md): five metrics on/off pairs and 130 exact workload queries on `b1a0ea5` | Revalidate after runtime changes; sustained and deployment-wide capacity remain separate |
| Opt-in Linux process-tree memory, CPU and PID limits | Deployment-specific headroom, restart recovery and sustained fault/load validation |
| Independent probes, dataset readiness, phased drain and [application](rolling-upgrades.md) and [broker/client](nats-compatibility.md) upgrade matrices | Deployment-specific rollout and multi-hour fault/load campaigns |
| Metrics opt-out, bounded history, [authorized trace continuity](tracing.md), [real OTLP acceptance](tracing-acceptance.md#recorded-result), [child stage metrics](child-timings.md), source observations and [local audit](durable-audit.md) | Full distributed attribution, audit archival and broader workload-cost measurements |
| [Paired telemetry cost](telemetry-overhead.md#recorded-full-comparison) on `93339e7`: 48 epochs, 96 measured queries and exact results | Deployment-specific cost; broker queue and separate source/compute attribution remain unknown |
| Selected-secret forwarding, files, opt-in cloud secrets, API-key and TLS rotation | Live-provider IAM/rotation coverage and coordinated enrollment/revocation |
| Managed Linux scratch with inherited leases | Deployment-specific disk capacity and recovery validation |
| [Merged `cargo` acceptance](evidence/cargo-b1a0ea5-acceptance.json) on `b1a0ea5`: both CI jobs, 15 notebooks, process-loss and snapshot-upgrade gates passed | Recurring provider, sustained-load and deployment tests on each release candidate |

See [operations](operations.md), [process containment](process-containment.md), [process-loss acceptance](process-loss-acceptance.md), [snapshot upgrades](release-upgrades.md), [rolling applications](rolling-upgrades.md), [broker/client compatibility](nats-compatibility.md), [worker scratch](worker-scratch.md) and [runtime recovery](runtime-recovery.md).

Admission reserves budgets; optional containment enforces native process-tree limits. Parent allocations still need host/container limits and measured headroom. DuckDB materializes before Arrow delivery.

## Dataset safety and scale

| Available | Still needed |
| --- | --- |
| Immutable local/remote full refresh, fenced publication and local reader pins | Durable remote reader integration and live acceleration acceptance for each intended provider |
| Strict schemas, optional nullable additions and conservative widening | Separate proof for any broader evolution policy |
| Multipart snapshots and over-4-GiB development gates | Selective replacement, part reuse and incremental checkpoints |
| Verified inventory/restore, local backup and remote-to-local migration | Remote-destination recovery, cross-host cutover and measured RTO/RPO |
| Local pruning with reader protection | Durable remote reader protection, orphan accounting, GC and compaction |

[Object writer shutdown](object-writer-shutdown.md) joins pending factories, transactions and owned client cleanup; 45 focused race tests and vet passed on `997157e`. Reader activation and remote deletion remain separate.

The [durable reader registry](durable-reader-registry.md), [object-store adapter and immutable binding](reader-objectstore.md) passed isolated race tests and vet; runtime integration and live-provider acceptance remain open. Remote pruning intentionally deletes nothing. Protect current, retained, pinned, staging and orphan data before adding GC. Remote v4 writes require coordinated reader/writer upgrades. [Storage guide](storage-conformance.md) · [Recovery](snapshot-backup.md)

## Results and interoperability

| Capability | Status |
| --- | --- |
| Arrow delivery and optional LZ4 | Available; decoded limits remain unchanged |
| Durable exports | Delivered: [opt-in federated jobs and repeat downloads](exports.md). [Merged-cargo gates](export-ci-diagnostics.md) passed on NATS 2.14.7 and 2.15.0; the earlier failure's cause remains unknown. Sustained capacity, live providers and deployment acceptance remain separate |
| Result cache | Pending authorization/generation keys, bounded fills and revocation fencing |
| Eight federation adapters | Available; live Oracle/warehouse gates remain open |
| Automatic remote joins/aggregates | Pending bound-plan integration and parity checks |
| Flight SQL | Client available; read-only server pending |
| PostgreSQL/MySQL CDC | Pending checkpoint/publication and replay contracts |
| Principal access | [Keys and handle ownership](principal-access.md), [callback](row-column-access.md), [local/object snapshot policies](guarded-snapshots.md) and [federated exports](exports.md); native row policies, cache and coordinated revocation remain open |
| Single-query distribution | Outside this design; workers distribute independent queries |

## Gates that can run on the dedicated test hosts

1. Run ordinary/native-bridge tests, race checks, vet and the public adapter example.
2. Repeat storage, schema, worker-failure, backup and upgrade gates on the release revision.
3. Mix joins, exports and refreshes with slow readers, cancellation and forced gateway/worker/broker loss.
4. Record exact answers, queue tails, RSS, charged memory, OOMs and scratch; retain failures.
5. Compare telemetry on/off and rerun the micro-VM profile after runtime changes.

Use the designated Linux build hosts per [AGENTS.md](../AGENTS.md). Entry points: [CI](../.github/workflows/ci.yml), [operational acceptance](operational-acceptance.md), [worker failures](worker-failures.md), [benchmarks](analytics-workflow-benchmarks.md).

## Gates requiring real provider access or deployment decisions

No dedicated live-provider fixtures are available. Tickets [#9](https://github.com/SyneHQ/kelvo-go/issues/9), [#10](https://github.com/SyneHQ/kelvo-go/issues/10) and [#11](https://github.com/SyneHQ/kelvo-go/issues/11) remain blocked.

- Oracle TCPS, Snowflake, BigQuery and Databricks: real grants, TLS, types, cancellation and rotation.
- S3, R2, Azure Blob and GCS: dedicated namespaces and scoped fixture identities.
- Every claimed engine: acceleration, revoked-grant, type and sustained-load checks. A protocol-family alias is not product validation.
- Shared enterprise rollout: user policy, retention, topology, capacity, key rotation and recovery objectives.

## Completion rule

Publish the implementation, pass review and satisfy every required gate. Record passed, failed, skipped and unrun checks against exact revisions and hashes. [Validation](validation.md) separates fixtures, live providers and capacity evidence; [delivery process](delivery-process.md) defines board status.

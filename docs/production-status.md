# Production delivery checklist

Status reviewed 3 October 2026. This is a capability checklist, not production certification. Owners and acceptance criteria live on the [board](https://github.com/orgs/SyneHQ/projects/3) and [tracker #32](https://github.com/SyneHQ/kelvo-go/issues/32).

## Operational foundation

| Available | Still needed |
| --- | --- |
| Shared query/refresh budgets, interactive reserves and source quotas | Export dispatch integration and sustained capacity gates |
| Opt-in Linux process-tree memory, CPU and PID limits | Deployment-specific headroom, restart recovery and sustained fault/load validation |
| Independent probes, dataset readiness and phased drain | Rolling cluster upgrades and multi-hour fault/load campaigns |
| Metrics, bounded history, local tracing and passive source observations | Distributed attribution, durable audit and instrumentation-cost measurements |
| Selected-secret forwarding, files, opt-in cloud secrets, API-key and TLS rotation | Live-provider IAM/rotation coverage and coordinated enrollment/revocation |
| Managed Linux scratch with inherited leases | Deployment-specific disk capacity and recovery validation |
| CI, 15 executed notebooks, process-loss and snapshot-upgrade gates | Recurring real-provider tests on each release candidate |

See [operations](operations.md), [process containment](process-containment.md), [process-loss acceptance](process-loss-acceptance.md), [release upgrades](release-upgrades.md) and [worker scratch](worker-scratch.md).

Admission reserves budgets; optional containment enforces native process-tree limits. Parent allocations still need host/container limits and measured headroom. DuckDB materializes before Arrow delivery.

## Dataset safety and scale

| Available | Still needed |
| --- | --- |
| Immutable local/remote full refresh, fenced publication and pinned readers | Live acceleration acceptance for each intended provider |
| Strict schemas, optional nullable additions and conservative widening | Separate proof for any broader evolution policy |
| Multipart snapshots and over-4-GiB development gates | Selective replacement, part reuse and incremental checkpoints |
| Verified inventory/restore, local backup and remote-to-local migration | Remote-destination recovery, cross-host cutover and measured RTO/RPO |
| Local pruning with reader protection | Durable remote reader protection, orphan accounting, GC and compaction |

Remote pruning intentionally deletes nothing. Protect current, retained, pinned, staging and orphan data before adding GC. Remote v4 writes require coordinated reader/writer upgrades. [Storage guide](storage-conformance.md) · [Recovery](snapshot-backup.md)

## Results and interoperability

| Capability | Status |
| --- | --- |
| Arrow delivery and optional LZ4 | Available; decoded limits remain unchanged |
| Durable exports | [Internal store](export-storage.md) tested; jobs, API and downloads pending |
| Result cache | Pending authorization/generation keys, bounded fills and revocation fencing |
| Eight federation adapters | Available; live Oracle/warehouse gates remain open |
| Automatic remote joins/aggregates | Pending bound-plan integration and parity checks |
| Flight SQL | Client available; read-only server pending |
| PostgreSQL/MySQL CDC | Pending checkpoint/publication and replay contracts |
| Principal source grants | [Opt-in user/service keys and handle ownership](principal-access.md); row/column policy still pending |
| Single-query distribution | Outside this design; workers distribute independent queries |

## Gates that can run on the dedicated test hosts

1. Run ordinary/native-bridge tests, race checks, vet and the public adapter example.
2. Repeat storage, schema, worker-failure, backup and upgrade gates on the release revision.
3. Mix joins, exports and refreshes with slow readers, cancellation and forced gateway/worker/broker loss.
4. Record exact answers, queue tails, RSS, charged memory, OOMs and scratch; retain failures.
5. Compare telemetry on/off and rerun the micro-VM profile after runtime changes.

Use the designated Linux build hosts per [AGENTS.md](../AGENTS.md). Entry points: [CI](../.github/workflows/ci.yml), [operational acceptance](operational-acceptance.md), [worker failures](worker-failures.md), [benchmarks](analytics-workflow-benchmarks.md).

## Gates requiring real provider access or deployment decisions

- Oracle TCPS, Snowflake, BigQuery and Databricks: real grants, TLS, types, cancellation and rotation.
- S3, R2, Azure Blob and GCS: dedicated namespaces and scoped fixture identities.
- Every claimed engine: acceleration, revoked-grant, type and sustained-load checks. A protocol-family alias is not product validation.
- Shared enterprise rollout: user policy, retention, topology, capacity, key rotation and recovery objectives.

## Completion rule

Publish the implementation, pass review and satisfy every required gate. Record passed, failed, skipped and unrun checks against exact revisions and hashes. [Validation](validation.md) separates fixtures, live providers and capacity evidence; [delivery process](delivery-process.md) defines board status.

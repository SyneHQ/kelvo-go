# Production roadmap

[Delivery board](https://github.com/orgs/SyneHQ/projects/3) · [Tracker #32](https://github.com/SyneHQ/kelvo-go/issues/32) · [Current checklist](production-status.md)

## Delivery status

Kelvo remains a developer preview. The board tracks owners, dependencies and acceptance; a feature is Done only after publication, review and its required gates.

| Available | Guide / evidence |
| --- | --- |
| Query/refresh admission, source quotas, diagnostics, readiness and drain | [Operations](operations.md) |
| Key, TLS identity and TLS trust rotation | [API keys](gateway-key-rotation.md) · [TLS identity](tls-identity-rotation.md) · [Trust/revocation](tls-trust-rotation.md) |
| Principal keys and callback row/column policies | [Authority](principal-access.md) · [Table restrictions](row-column-access.md); native/snapshot/export/cache paths remain open |
| Parent-only cloud secrets and bounded local audit | [Cloud secrets](cloud-secrets.md) · [Audit contract and measured cost](durable-audit.md) |
| Managed Linux worker scratch | [Ownership and recovery](worker-scratch.md) |
| Opt-in Linux process-tree memory, CPU and PID limits | [Containment](process-containment.md) · [18-gate evidence](evidence/process-containment-publication.json) |
| Strict schemas, optional widening, multipart snapshots and verified restore | [Acceleration](acceleration.md) · [Schema policy](schema-evolution.md) · [Multipart](multipart-acceleration.md) |
| Local backup and remote-to-local migration | [Recovery](snapshot-backup.md) |
| Passive source observations and local lifecycle telemetry | [Source health](source-health.md) · [Tracing](tracing.md) |
| Durable Arrow export storage primitive | [Contributor API](export-storage.md); no export jobs or download API yet |
| Repeatable release gates | [Process loss](process-loss-acceptance.md) · [Snapshot upgrades](release-upgrades.md) · [Rolling matrix](rolling-upgrades.md) · [Broker/client matrix](nats-compatibility.md) · [Storage](storage-conformance.md) |

Implemented controls are not deployment certification. Live-provider gates and sustained fault/load campaigns remain open. The [rolling matrix](rolling-upgrades.md) covers one application pair; the [broker/client matrix](nats-compatibility.md) adds the declared NATS versions and security-configuration refusal checks. Deployment-specific combinations still need validation. Existing measurements belong to their recorded binaries.

## Review scope

This roadmap began with a source review of [`90ade2e`](https://github.com/SyneHQ/kelvo-go/tree/90ade2e2092a90a05a65e20628cd350079f36373) on 2 October 2026. The status above includes later implementation; [validation](validation.md) records its evidence.

## What Kelvo already provides

Keep the Go coordinator, DuckDB execution, Arrow results, Parquet snapshots and NATS dispatch. Preserve tenant isolation, exact values, required filters, selected-secret forwarding, writer fencing and pinned readers.

One SQL plan runs on one worker. DuckDB completes execution before Arrow delivery, so transport backpressure does not bound native query memory.

## Priority matrix

| Priority | Remaining work | Completion needs |
| --- | --- | --- |
| P0 | Deployment containment acceptance, provider/rotation coverage, rolling upgrades and sustained load | Correct cleanup, exact results and failure-inclusive runtime evidence |
| P1 | Remote reader protection, GC, compaction and selective refresh | Crash-safe retention, bounded storage and unchanged results |
| P1 | Asynchronous exports and result cache | Authorization, storage/admission bounds and publication fencing |
| P1/P2 | Explain, richer pushdown and columnar warehouse reads | Real bound plans, dialect parity and measured savings |
| P2 | Read-only Flight SQL server and CDC | Client conformance and recoverable data/checkpoint publication |

Priorities express order, not delivery dates.

## First delivery: visibility, bounded execution and maintenance

Admission, optional Linux containment, probes, drain and local telemetry are implemented. Validate delegation, parent headroom and recovery on each deployment before claiming safe production capacity.

- Account for native memory, Arrow buffers, scratch and refresh publication. Export dispatch must reserve its own workload capacity.
- Keep metrics and trace queues bounded; exclude SQL, parameters, secrets and unbounded labels.
- Report queue, source, compute and delivery time only where measured. Unknown or overlapping intervals must stay explicit.

**Gate:** mixed query/refresh/export load, slow readers, saturation, cancellation and forced process loss under real host limits. Record queue tails, exact answers, RSS, charged memory, OOMs and scratch.

## Dataset safety before incremental acceleration

Strict schema contracts, classified retries, source quotas and verified restore are implemented. Preserve the previous generation on failure and its original age on restore.

**Gate:** schema drift, revoked grants, corruption, restart during publication and restore racing refresh. Only a verified authorized generation may become current; permanent errors stop retries.

## Scale acceleration with bounded parts and selective work

Multipart full refresh is available. Next: partition replacement, part reuse, incremental checkpoints and compaction.

- Define keys, tie-breakers and late-data policy. A timestamp alone misses equal-time changes and deletes.
- Account for current, retained, pinned, staging and orphan bytes.
- Protect remote readers durably before enabling deletion. Current remote pruning deliberately deletes nothing.

**Gate:** datasets above 4 GiB, bounded parts, concurrent readers/publication/GC, full disks and crashes. Prove unchanged answers and reduced source/object reads.

## Durable exports and result caching are different features

The local export store exists; jobs, dispatch and authorized repeat downloads are pending. Publish only complete, verified parts, with tenant/owner/authorization binding and expiry. Reserve storage before execution.

Start caching with immutable accelerated generations. Keys must include SQL, typed parameters, tenant, effective authorization, generations and relevant versions. Fence fills against refresh/revocation; bound entries, bytes and concurrent fills.

**Gate:** disconnects and crashes at publication boundaries, repeat downloads without SQL replay, revocation, expiry and compressible oversized results. Any stale-data policy must never bypass revoked access or schema incompatibility.

## Federation and client interoperability

1. Expose source/local operators and residual reasons from the real bound plan.
2. Prove additional predicates per dialect, with pushdown-off parity and exact NULL/precision/timezone behavior.
3. Measure Arrow/ADBC warehouse reads against current retrieval paths.
4. Add a read-only Flight SQL server with bounded sessions/tickets, authorization rechecks and restricted metadata.

Automatic join/aggregate pushdown needs planner integration and compatible authorization. SQL string rewriting or early LIMIT on a joined relation is insufficient. Flight SQL and QUIC do not make DuckDB execution intrinsically faster.

## CDC and enterprise operations

Start PostgreSQL/MySQL CDC after storage maintenance and recovery. Define keys, transaction order, schema versions and snapshot-to-stream handoff. Publish data and its checkpoint durably before acknowledging the source; replay must be idempotent.

**Gate:** every data/checkpoint/ACK crash boundary, duplicates, key changes, failover and expired source history. An unavailable cursor must require explicit bootstrap.

Principal source/handle grants, callback row/column policies and bounded local audit are implemented. Enterprise deployments still need policy support for other execution paths, enrollment, coordinated revocation, audit archival and tested rollout contracts. Secret-manager credentials remain in the trusted parent.

## Release gates and delivery order

1. Complete isolation, lifecycle, credentials and recurring conformance.
2. Complete provider-backed recovery and sustained mixed-load gates.
3. Add storage maintenance, exports and generation-scoped caching.
4. Add richer federation, standard clients and CDC.

Retain failed/skipped trials, exact type checks and pinned revision/fixture hashes. Re-run the micro-VM profile after runtime changes. Single-query distribution, model/GPU runtimes and additional query engines remain outside the current design.

## Licensing

Kelvo is an independent SYNEHQ project under [Apache-2.0](../LICENSE). Preserve applicable third-party notices and review dependency licenses.

Design references: [DuckDB memory](https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads.html), [OpenTelemetry](https://opentelemetry.io/docs/specs/otel/metrics/), [Arrow](https://arrow.apache.org/docs/format/Columnar.html), [conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html), [retry backoff](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/), [reliability testing](https://sre.google/sre-book/testing-reliability/), [Iceberg maintenance](https://iceberg.apache.org/docs/latest/maintenance/), [cache semantics](https://www.rfc-editor.org/rfc/rfc9111), [Trino pushdown](https://trino.io/docs/current/optimizer/pushdown.html), [ADBC](https://arrow.apache.org/adbc/current/), [Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html), [PostgreSQL logical decoding](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html).

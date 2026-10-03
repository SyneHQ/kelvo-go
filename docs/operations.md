# Worker admission, metrics and maintenance

Opt into shared node reservations in worker YAML. These illustrative MiB values are **not** measured sizing recommendations:

```yaml
resources:
  max_concurrent: 2
  memory_mb: 4096
  baseline_mb: 512
  overhead_mb: 256
  scratch_mb: 8192
  query_reserve_slots: 1
  query_reserve_memory_mb: 1024
  query_reserve_scratch_mb: 2048
```

| Reservation | Accounting |
| --- | --- |
| Usable memory | `memory_mb - baseline_mb` |
| Query | Engine `memory_mb + overhead_mb`; `max_temp_mb` scratch |
| Refresh | Query reservation plus `max_bytes` staging, held through publication/pruning |
| Query reserves | Capacity unavailable to background refresh; interactive queries can use all idle capacity |

Queries/refreshes share one process-local pool and wait within their timeout. Oversized configurations fail startup; release follows cleanup. Inner refresh executors do not reacquire the pool. Reserves provide neither preemption nor fairness. Async export dispatch is not connected yet.

Omitting `resources` retains slot-only admission. Reservations alone do not enforce RSS/disk limits; compression does not reduce them. Divide host capacity among nodes, retain OS/container limits, and budget persistent snapshots separately. DuckDB materializes execution before Arrow delivery and its memory setting does not cap every native/Arrow allocation.

For kernel-enforced native process-tree limits, enable [Linux containment](process-containment.md). It requires explicit cgroup delegation, managed scratch and extra parent/native headroom. Uncertain cleanup retains reservations and drains the node.

## Diagnostics

Worker `GET /metrics` and `GET /resources` require gateway mTLS. Metrics have fixed cardinality; resource responses expose aggregate capacity, usage, waits and drain state. Neither includes SQL, credentials, sources, query IDs or tenant IDs. Keep collection internal.

| Measurement | Meaning |
| --- | --- |
| Query queue | Local node-resource and source-quota admission waits |
| Query duration | Setup, execution/delivery and cleanup; excludes those admission waits |
| Refresh queue | Outer node-resource admission only |
| Refresh duration | Includes nested source-quota wait, extraction and publication |
| Query phases | Validation, node admission, source admission, preparation, execution/delivery, cleanup |
| First batch | Executor entry to first decoded Arrow record; absent for empty streams |
| Sink callbacks | Subset of execution/delivery, not an additional phase or pure network time |

The startup SELECT probe counts as execution. Metrics reset on restart; there is no durable history. Optional [tracing](tracing.md) exports bounded sampled spans. Neither measures JetStream dispatch, claim delay, gateway receipt or separate overlapping source/transfer time. See [phase definitions](tracing.md#what-is-recorded).

## Passive source observations

`source_health: {observation_ttl: 5m}` enables gateway-mTLS `GET /sources` for at most 256 configured live sources. Eligible native query/refresh completions update observations; expiry becomes `unknown`. There is no probe SQL or readiness effect. Source IDs remain internal, never metric labels. See [source diagnostics](source-health.md).

## Maintenance

1. Set `--drain-timeout` on `gateway`/`node` (default `30s`).
2. Send SIGTERM or SIGINT: admission stops and readiness fails; existing results, status/cancel and leases continue during grace.
3. Allow workers to finish assigned queries/refreshes. Gateways wait their configured grace because handles are shared. Grace expiry cancels work and bounds shutdown; assigned SQL is never replayed.

Gateway `/health` and `/ready` bypass ordinary request permits. Readiness reports lifecycle/broker reconciliation, not source reachability or spare capacity. Worker probes require gateway mTLS and can additionally require datasets.

## Dataset diagnostics and required readiness

Acceleration enables gateway-mTLS `GET /datasets` for up to 64 datasets. States are `ready`, `missing`, `stale`, `configuration_changed` or `unavailable`; metadata excludes paths, object URLs, fingerprints, source configuration and raw errors.

Require selected catalog datasets in **node** YAML:

```yaml
required_datasets:
  - orders_daily
```

Missing, stale, mismatched or unavailable required data fails worker readiness. Optional datasets and gateway readiness remain independent. Without this setting, dataset state does not gate readiness.

Reads coalesce/cache for 2s with a 5s deadline and four backend slots. When required data exists, two slots are reserved for it. Cached age keeps advancing; diagnostics trigger no source SQL or refresh.

**Readiness checks metadata, not payload integrity.** Use explicit verification for checksums/schema; readiness does not prove source connectivity or execution headroom.

## Local snapshot backups

```sh
kelvo accelerate backup --config kelvo.yml --dataset orders_daily \
  --destination /absolute/new-root
```

The Linux destination must be absent under a private parent. Catalog authorization/budgets apply and refresh time is preserved. Recover into another fresh root before switching configuration. A sync failure may occur after publication: do not automatically overwrite/delete that destination. Follow [backup and recovery](snapshot-backup.md).

## Remote generation inventory and restore

Use the source/acceleration catalog, not node YAML:

```sh
kelvo accelerate inventory --config kelvo.yml --dataset orders_daily
kelvo accelerate restore --config kelvo.yml --dataset orders_daily \
  --generation RETAINED_GENERATION --expected-generation CURRENT_GENERATION
```

Restore derives authorization from the catalog, verifies current and target SHA-256/schema, takes a renewable writer lease and conditionally publishes. Changed expected generation fails. Exact immutable versions and original refresh time remain; restored data can still be stale. Custom clients without bounded range reads reject recovery.

Inventory is a retained manifest catalog: current plus at most 16 historical entries, fewer when the 64 KiB root limit requires it. Output signals truncation. It does not list storage, discover uncatalogued legacy objects or delete evicted objects. Remote GC remains unimplemented.

Corrupt current payloads cannot use this restore path because current-schema verification is required. Use independently validated backup/source recovery. Readable corrupt entries can be marked unverified; invalid root/descriptors can fail inventory entirely. This is not complete disaster recovery or a measured RTO/RPO.

### Remote manifest upgrade

Readers accept remote v2/v3/v4; every writer action emits v4, including first lease claim/renewal before extraction succeeds. Legacy v2 starts without history. Coordinate compatible readers/writers before that boundary; pre-v4 readers and binary rollback afterward are unsupported. Never edit versions to downgrade. Local formats remain v1 single-file and v2 multipart.

### Remote multipart operation

[Multipart refresh](multipart-acceleration.md) stages/uploads one part at a time, verifies immutable identity, then publishes a descriptor and conditional root. Limits: 2 MiB descriptor, 64 KiB root, 256 parts, 4 GiB/part and 1 TiB configured aggregate; other row/metadata/resource limits can lower capacity.

Queries share a parent-owned range bridge with four upstream slots and 1,024 capabilities; cloud credentials stay outside children. Verification/recovery checks full hashes, rows and schema. Orphaned/retired objects require safe operator cleanup.

At `71f3390`, [20 multipart TLS checks](evidence/object-multipart-acceptance.json), [36 legacy checks](evidence/object-multipart-legacy-acceptance.json) and the [remote over-4-GiB gate](evidence/object-multipart-large-dataset.json) passed. The latter uses synthetic Arrow, disk-backed storage and real HTTPFS; it is not live-cloud/WAN or memory-capacity evidence.

## Validation boundaries

At `6b49c17`, Azure tests, focused race checks, vet/builds, 22 fixture tests and real NATS integration passed alongside [10 cluster checks](evidence/production-foundation-cluster.json) and [14 acceleration checks](evidence/production-foundation-acceleration.json). All workers used shared budgets; no local builds ran. Fixture Python used the existing PyArrow environment.

Single loopback timings and unit tests establish no throughput/OOM guarantee. Use [production status](production-status.md) for later operational/fault evidence and remaining deployment gates.

## Shared source quotas

Configure matching tenant policies everywhere; these count **whole operations**, not connections or individual federated scans:

```yaml
policy:
  source_quotas:
    warehouse: 2
    orders: 4
```

1. Retain the other required policy fields and provision `KV_KELVO_SOURCE_QUOTAS` with `cluster-init`.
2. Allow six account streams when acceleration status is enabled, plus worker publish permissions for `$JS.API.STREAM.MSG.GET.KV_KELVO_SOURCE_QUOTAS` and `$KV.KELVO_SOURCE_QUOTAS.>`.
3. Drain/reprovision for policy changes. Limits are 64 source IDs and 64 slots each.

The parent acquires slots in stable order, releases partial acquisitions on contention and renews ownership. Wait consumes execution timeout; lease loss cancels work. Release follows cleanup, and late ownership loss prevents refresh publication. Accelerated reads consume no original-source slots. Broker TTL reclaims crashed owners independently of host clocks.

A lease cannot fence remote SQL. Keep read-only grants and source-side limits; cancellation remains best effort.

## File-based source credential rotation

Map existing environment references to private files in node YAML:

```yaml
secrets:
  ttl: 30s
  files:
    KELVO_WAREHOUSE_PASSWORD: /run/kelvo-secrets/warehouse-password
```

Only selected-source references are resolved into the selected child's environment; catalogs retain reference names. New queries/refreshes receive current values within cache TTL. Unmapped references use environment values; configured file failure never falls back.

Use service-owned regular files in trusted directories and atomic replacement. Symlinks, hardlinks, unsafe modes, NULs and values over 16 KiB fail. Preserve exact bytes without unwanted newlines. Limits: 128 files, 2 MiB retained bytes, TTL ≤5m; zero TTL disables retention. Buffer wiping is best effort.

This does not rotate existing query credentials, parent object clients, NATS, gateway tokens or TLS, or provider master credentials. For explicit AWS, Azure and GCP mappings, use [cloud source secrets](cloud-secrets.md).

## Source refresh failures and recovery

Typed PostgreSQL/MySQL errors classify authentication, permission, query/configuration and unsupported-type failures as permanent. Unknown errors retry within five failures, with jitter capped at 5m. See [error mappings](native-error-classification.md).

Typed DuckDB OOM produces permanent/resource `RESOURCE_EXHAUSTED`, including failure before Arrow delivery. It does not infer OOM from text or classify generic I/O/external kills. DuckDB memory is not a process RSS ceiling; see [worker failures](worker-failures.md).

1. Inspect durable state:

```sh
kelvo refresh-status --config worker.yml --dataset sales_snapshot
```

2. Repair credentials, grants, query/types or resource budget.
3. Reset using operator NATS authority and the current catalog fingerprint:

```sh
kelvo refresh-reset --config worker.yml --dataset sales_snapshot \
  --expected-fingerprint CURRENT_CATALOG_FINGERPRINT
```

Reset is sequence-fenced; permanent suppression survives restarts. Standalone `accelerate watch` has no durable cluster retry state.

Failed refreshes preserve the previous snapshot. Revoking source access does **not** revoke snapshot readers: bump `authorization_version` and propagate the catalog to all serving nodes. Credential rotation/reset alone does not invalidate snapshots.

## Optional execution history

```yaml
history:
  max_entries: 256
  ttl: 1h
```

Gateway-mTLS `GET /history` returns a process-local ring, at most 1,024 entries/24h. Without configuration no ring is allocated. Entries contain generated query IDs, fixed outcomes/categories and timing, without SQL, parameters, sources or result previews. Reads/appends expire entries; restart clears them.

A record follows node execution/transfer and result-ready update. Node success does not prove gateway durable success or client receipt. This is not an audit/replay catalog or queued-job history.

## Dataset safety validation

At `ad25a3c`, ordinary/bridge/race/strict-cgo checks and real NATS quota/retry/reset tests passed. [16 acceleration checks](evidence/dataset-safety-acceptance.json) cover typed data, schema drift, restore preconditions/freshness, scheduled refresh and isolation. Integration fixes increased CAS-header space and corrected a consumer timeout; corrected tests passed.

## Current development validation

Tracing/credential tests cover bounds, privacy, cancellation and selected secret forwarding. Protocol recovery fixtures establish correctness only; keep their exact runtime evidence separate from live-provider, throughput and capacity claims.

## Readiness and recovery milestone validation

At `c303b5f`, ordinary/bridge/race/strict-cgo checks, vet/builds and real NATS tests passed on Azure, followed by [10 cluster](evidence/production-readiness-cluster.json) and [19 acceleration checks](evidence/production-readiness-acceptance.json). Required/optional readiness, sanitized diagnostics and recovery were checked.

Regression fixes covered same-generation restore lease cleanup and tracing resource sanitization. Trace overhead, real-provider rollback and backup RTO/RPO were not measured. For constrained CSV workers, use [paired reader options](csv-memory.md) based on actual line sizes and retain native-memory headroom.

## Additional validation at runtime revision f3886b7

Azure ordinary/bridge/race/strict-cgo checks, vet/builds and [four sandboxed failure cases](evidence/worker-failure-acceptance.json) passed. The same binary passed [36 single-object](evidence/snapshot-verification-object-acceptance.json), [20 multipart TLS](evidence/snapshot-verification-multipart-acceptance.json) checks and the [12-case CSV experiment](evidence/csv-memory-acceptance.json). Temporary CAs/directories were removed.

These use protocol storage and injected in-memory quota/retry failures. They add no live-cloud, broker-failover, WAN, sustained-capacity or Oracle micro-VM result.

## Optional aggregate metrics

Node metrics default to enabled. Set `metrics: {enabled: false}` in node YAML to skip aggregate lifecycle collection and disable `/metrics`. Resource admission and `/resources` remain active. Tracing, history, source-health observations and audit have separate controls; disabling metrics does not disable them.

Measure off/on with the same binary, limits, data and query order before attributing a capacity change to instrumentation.

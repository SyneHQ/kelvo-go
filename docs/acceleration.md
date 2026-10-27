# Dataset acceleration

Refresh a source query into persistent Parquet, then query the copy with DuckDB. This is full-refresh acceleration; each refresh extracts the complete result.

## When to use acceleration

Use it for repeated dashboards, reports or exploration that would otherwise read the source each time. Select only needed columns, date ranges or pre-aggregations; set a refresh interval that matches acceptable staleness.

All built-in native connectors can feed snapshots when their query and Arrow types are supported. Check the [capability and validation matrix](source-coverage.md#acceleration-and-federation-capabilities). Measure source cost and query latency: copying an already efficient analytical database is not automatically faster.

Missing, expired or incompatible snapshots fail with `DATASET_UNAVAILABLE`, without live-source fallback. A dataset with `on_unavailable: refresh` instead runs its normal refresh when its snapshot is missing or expired, then serves the new snapshot; the first such read pays the full refresh latency, concurrent reads share one refresh, a failed or timed-out refresh returns `DATASET_UNAVAILABLE`, and the read is marked `refreshed_on_demand` in its acceleration stats. Incompatible or corrupt snapshots still fail. Cluster nodes only; refresh runs through the node's refresh gate and resource admission. Corruption can surface during execution; run `verify` for a full check.

## Run locally

1. Refresh the included sales example.
2. Query the alias, inspect it, then verify its bytes.
3. Run `watch` separately if you want scheduled refreshes.

```sh
bin/kelvo accelerate refresh --config examples/acceleration.yml --dataset sales_fast
bin/kelvo query --config examples/acceleration.yml --sources sales_fast \
  --sql 'SELECT region, SUM(amount) AS revenue FROM sales_fast GROUP BY region ORDER BY region' \
  --out revenue.arrow
bin/kelvo accelerate status --config examples/acceleration.yml --dataset sales_fast
bin/kelvo accelerate verify --config examples/acceleration.yml --dataset sales_fast
bin/kelvo accelerate watch --config examples/acceleration.yml
```

`query` and `serve` never start a scheduler. `status` emits YAML; successful query statistics identify pinned generations. On Linux, add `--sandbox /path/to/kelvo-landlock` to sandbox refresh children; cluster nodes require it.

## Configure a source query

```yaml
sources:
  - id: warehouse
    type: clickhouse
    url_env: KELVO_SOURCE_WAREHOUSE_URL
    username_env: KELVO_SOURCE_WAREHOUSE_USER
    password_env: KELVO_SOURCE_WAREHOUSE_PASSWORD
acceleration:
  tenant_id: tenant-a
  directory: /var/lib/kelvo/acceleration
  datasets:
    - id: recent_orders
      query:
        mode: native
        connection_id: warehouse
        sql: >-
          SELECT id, region, amount
          FROM analytics.orders
          WHERE created_at >= now() - INTERVAL 90 DAY
      authorization_version: orders-readers-v1
      refresh_interval: 5m
      max_age: 15m
      limits:
        max_rows: 10000000
        max_bytes: 1073741824
        timeout: 5m
        memory_mb: 512
        threads: 2
        max_temp_mb: 2048
```

Use source SQL with `mode: native`, or DuckDB SQL with a `sources` list. Inputs must be registered sources, not other accelerated datasets. Refresh parameters are unsupported.

Query `recent_orders` with `--sources recent_orders`. Multiple accelerated aliases can be joined even when their original connectors lack live federation.

| Setting | Rule |
| --- | --- |
| `refresh_interval` | `0s` for manual only; otherwise at least 5s and no greater than `max_age` |
| `max_age` | Positive, at most 30 days; measured from completed refresh |
| `on_unavailable` | Empty (fail) or `refresh` (refresh synchronously on a missing or expired snapshot) |
| `authorization_version` | Required; change after source grants or permitted data change |

Freshness is checked before execution; readers pin the generation for the query's lifetime. Refresh time is not a database transaction watermark.

## Hot/cold routing

A dataset with `route` serves old time ranges from its snapshot and newer ones from a live source:

```yaml
- id: orders
  route:
    time_column: created_at
    live_source: orders_live
```

| Setting | Rule |
| --- | --- |
| `route.time_column` | Identifier of the dataset's time column (integer, date or timestamp) |
| `route.live_source` | Registered, non-accelerated source that answers under the dataset's alias; both keys are required together |

Each refresh records `covered_until`, the exact `MAX(time_column)` of the rows it wrote, in the snapshot metadata (visible in `status`). A snapshot with no rows, only NULLs, no such column or an unsupported type records nothing and is always read.

At query time Kelvo parses the SQL and reads only simple top-level `WHERE` conjuncts on the column: `>`, `>=`, `<`, `<=`, `=` against a literal and `BETWEEN a AND b`, for a single-table query on the dataset alias. If an upper bound is at or before `covered_until` the snapshot answers. Otherwise the query reads the live source (no union with the snapshot) because the snapshot may be stale: `beyond_covered` (upper bound after `covered_until`), `unbounded` (no upper bound, including no `WHERE`) or `unparsed` (`OR`, functions on the column, subqueries, joins, parameters, a literal of the wrong type, or SQL the MySQL-dialect parser rejects). The read's acceleration stats show `routed_live: true` and `route_reason`, with no generation.

A routed read is a direct read of `live_source`: it must pass the same access check and source quota as one, and the live source's connection is what the query child receives. A routed dataset skips `on_unavailable: refresh`. Restricted-policy and delegated reads never route and keep reading the snapshot, as do nodes without a refresh manager (`kelvo query`, `serve`) and datasets whose snapshot has no `covered_until`.

Upgrade every node before configuring `route`: only datasets with `route` write `covered_until`, and older nodes reject a manifest that carries it. Existing snapshots route only after their next refresh.

## Incremental refresh

A keyed dataset whose rows carry an increasing watermark can read only recent rows:

```yaml
  incremental:
    column: updated_at
    key: [id]
    deletes: soft
    late_window: 10m
    full_every: 24h
```

| Key | Meaning |
|---|---|
| `incremental.column` | Watermark: integer, date or timestamp that every insert, update and soft delete advances; must equal `route.time_column` when `route` is set |
| `incremental.key` | Required. Columns that identify one row of the dataset query result; must be unique and non-null |
| `incremental.deletes` | Required. Only `soft` is accepted: a delete is an update that advances the watermark (a tombstone flag or `deleted_at` row the query still returns). Hard deletes are invisible to a delta; such datasets must not set `incremental` |
| `incremental.late_window` | Optional duration subtracted from `covered_until` before reading, so rows committed late with an older watermark are still read. Timestamp watermarks only; must be shorter than `full_every` |
| `incremental.full_every` | Positive duration after which a refresh re-reads the whole source |

An incremental refresh runs the source query wrapped as `SELECT * FROM (<query>) AS kelvo_incr WHERE "<column>" >= <literal>`, where the literal is the previous snapshot's `covered_until` minus `late_window`. The bound is inclusive, so a row committed after the previous refresh with the same watermark value as `covered_until` is read; the key is the tie-breaker that makes the re-read rows harmless. The delta is spilled to a private Arrow IPC stage file in the dataset directory (same filesystem as the Parquet stage, removed on commit or abort, and by stage cleanup after a crash), capped at the smaller of `limits.max_bytes` and half of `limits.memory_mb` and at cap/64 rows so the key map stays bounded (above either the refresh is full; a spill write failure also falls back), then the new generation is written as every previous row whose key is not in the delta, followed by the delta. An updated or soft-deleted row therefore replaces its previous version. The previous generation is never changed and stays pinned for its readers; every publication, incremental or full, gets a new generation identity. `covered_until` is recorded whenever `incremental` or `route` is set.

Guarantee: with a unique key, soft deletes and every change landing inside `late_window`, the incremental generation holds the same rows as a full refresh would. When Kelvo cannot prove that, it never publishes a partial merge: it aborts the transaction and refreshes fully, logging the reason (also in `last_refresh.fallback`). Fallback cases: no previous generation, no `covered_until` or `last_full_at`, the fingerprint changed, `full_every` elapsed, the query cannot be wrapped safely, the delta schema differs from the previous one (schema drift, including additive changes, is adopted by the full refresh under the dataset's `schema_evolution` policy), a key or watermark column is missing, a key is null, the delta repeats a key, a key's watermark moved backwards (non-monotonic watermark), the watermark type is unsupported, `late_window` on an integer watermark, or the delta exceeds that cap. Key and watermark columns match case-insensitively, like `covered_until`. When every refresh since process start fell back, `accelerate status` reports `incremental disabled: <reason>`. Only federated SQL queries are wrapped; native queries, Mongo pipelines, several statements, and timestamps finer than microseconds always refresh fully. A time-zone-aware watermark is bounded with a `TIMESTAMPTZ` literal carrying `+00:00`, so the engine session time zone cannot shift it; a naive watermark is compared by wall-clock value, as recorded. A naive watermark holding local time repeats an hour when daylight saving ends, so set `late_window` to at least `1h` or store UTC. A routed dataset with `late_window` goes live for reads reaching past `covered_until - late_window`. An integer watermark with `late_window` is detected only at refresh (column types are unknown at load) and refreshes fully. A refresh that fails or crashes before commit publishes nothing and the previous generation stays current.

Explicit limits, checked by nobody but the operator: the key must be immutable (a changed key leaves the old row behind), and the dataset query must not filter on columns that change, because a row that stops matching the `WHERE` is absent from the delta and stays in the snapshot until the next full refresh; rows that change with a watermark older than `covered_until - late_window`, hard-deleted rows, rows whose watermark is NULL, and duplicate keys outside the delta are not seen or not detected until the next full refresh, so `full_every` bounds that staleness. Key uniqueness is only checked within the delta. The new generation copies all previous rows, so each run still reads and writes the whole snapshot locally; only the source read shrinks. Object backends download each previous part to a temporary file under the system temp directory and verify its checksum. Protected object backends always refresh fully. Row, byte and part limits apply to the whole generation. This is whole-dataset refresh with a watermark, not partition-level replacement: parts are not reused across generations.

Upgrading: `key` and `deletes` are now required, so a dataset configured with only `column` and `full_every` is rejected at load and must be updated; the changed configuration changes the fingerprint, so its first refresh is full.

`accelerate status` shows `last_refresh` (incremental or full, delta rows merged), tracked in memory since process start and unknown after a restart. `last_full_at` is stored in the snapshot metadata; upgrade every node before configuring `incremental`, as older nodes reject a manifest that carries it.

## MongoDB pipeline refresh

Use a native MongoDB source and a read-only YAML pipeline:

```yaml
query:
  mode: native
  connection_id: documents
  mongo:
    collection: orders
    pipeline:
      - $match:
          status: paid
      - $group:
          _id: "$region"
          revenue:
            $sum: "$amount"
```

Empty pipelines and Extended JSON values are supported. Numeric YAML values avoid float64 conversion; duplicate keys, aliases and non-finite values are rejected. `$out` and `$merge` are forbidden. Results remain BSON documents in Arrow Binary, not inferred columns. [Live evidence](evidence/mongodb-acceleration.json)

## Object storage

Set `acceleration.object_storage` for S3, R2, GCS or Azure Blob. `directory` becomes private local staging; a parent-owned range reader keeps cloud secrets out of query children.

Object reads cannot share a query with legacy PostgreSQL/MySQL DuckDB extensions. Materialize those inputs too. [Setup and limits](object-storage.md)

## Publication, failure and retention

1. Lock the dataset and recheck whether refresh is due.
2. Convert source Arrow batches into bounded, Snappy-compressed Parquet row groups.
3. Finalize, flush, hash and mark the payload read-only; atomically publish its manifest.
4. Let new queries pin the new generation. Existing readers keep theirs; failed refreshes preserve the old generation.

Local storage normally retains current plus one previous generation. Leases, staging and replacement data need extra space; per-refresh limits are not volume quotas. A post-publication fsync failure reports uncertain durability.

Acquisition checks identity, policy, permissions and size. Unrestricted reads use `verify` for full hashing. [Guarded snapshot reads](guarded-snapshots.md) additionally hash parts during relation discovery and before each scan. Keep directories and writers trusted and private.

## Tenant and cluster operation

Catalog and worker tenant IDs must match. Callers cannot choose refresh SQL, paths or tenant identity. Dataset-only children receive no original-source secrets.

**Revoke access explicitly:** update `authorization_version`, propagate the catalog and refresh. Retired copies need an operator retention policy; database revocations do not automatically revoke snapshots. Tenant grants are not per-user row policies; configure a separate [principal snapshot policy](guarded-snapshots.md) for restricted readers.

Cluster refresh adds two streams. Allow at least **5 streams per tenant account**, or **6 with source quotas**, then rerun `cluster-init` with provisioner credentials. Add these worker publish permissions alongside existing stream-info, inbox and ACK permissions:

```text
acceleration.refresh
$JS.API.CONSUMER.INFO.KELVO_ACCEL_QUEUE.refresh
$JS.API.CONSUMER.MSG.NEXT.KELVO_ACCEL_QUEUE.refresh
$JS.API.STREAM.MSG.GET.KV_KELVO_ACCEL_STATUS
$KV.KELVO_ACCEL_STATUS.>
```

The refresh queue carries dataset IDs/fingerprints only. Delivery is at least once; writer fencing and due checks prevent conflicting publication. Each node runs one refresh at a time, with reservations held through publication.

Local workers must share the same private tenant path and UID on a filesystem supporting `flock`, atomic rename and fsync. Verify multi-host filesystems separately. The [Compose overlay](../deploy/acceleration.compose.yml) expects pre-created mode-0700 directories owned by UID 65532. Do not run conflicting catalogs against one store.

## Type and memory boundaries

Supported: nullable booleans, integer widths, float32/64, strings, binary, Date32, Decimal128 up to 38 digits with nonnegative scale, and supported timestamps.

Second/millisecond timestamps may promote to microseconds; unzoned nanoseconds stay exact and UTC aliases normalize. Named zones, zoned nanoseconds, Decimal256, negative scales, nested/dictionary/extension types and untyped NULL columns are rejected where fidelity cannot be preserved. Ambiguous names and out-of-range values fail.

Row-group and footer limits bound encoding work, not total RSS. Wide schemas or tiny batches can hit metadata limits early. DuckDB materializes before Arrow delivery; use host/container and disk quotas.

## Current scope and industry standards

Available: full refresh, explicit freshness, local/cluster scheduling, strict schema policy, multipart snapshots, keyed incremental refresh with soft deletes and verified recovery. Pending: CDC, result caching, live-source fallback, background (stale-while-revalidate) refresh and distributed single-query execution. Independent datasets have no global transactional snapshot.

### Lifecycle priorities

Next work is selective replacement, checkpoints, reader-safe remote GC and compaction. See the [roadmap](production-roadmap.md); schema contracts, shared admission, quotas and classified retries already exist.

### Lessons from Trino

Kelvo connects to customer-managed Trino. Useful references: [pushdown](https://trino.io/docs/current/optimizer/pushdown.html), [resource groups](https://trino.io/docs/current/admin/resource-groups.html), [materialized-view freshness](https://trino.io/docs/current/connector/iceberg.html#materialized-views), [retry boundaries](https://trino.io/docs/current/admin/fault-tolerant-execution.html) and [dynamic filtering](https://trino.io/docs/current/admin/dynamic-filtering.html).

## Schema contracts and generation recovery

Schemas match exactly by default. Optional [evolution flags](schema-evolution.md) allow nullable additions and conservative widening. A mismatch returns `SCHEMA_MISMATCH`; the previous generation stays intact. All batches/parts within a generation still match exactly.

Policy changes alter the fingerprint and require refresh. Coordinate catalog rollout. Inspect and restore a retained generation with a concurrency precondition:

```sh
kelvo accelerate inventory --config kelvo.yml --dataset sales_snapshot
kelvo accelerate restore --config kelvo.yml --dataset sales_snapshot \
  --generation PREVIOUS_GENERATION --expected-generation CURRENT_GENERATION
```

Restore verifies current/target checksums, exact schemas and authorization. It preserves original age; evolution flags do not relax restore. Local inventory caps at 256 generations; remote inventory keeps current plus at most 16 historical entries without listing objects.

Restore needs a verifiable current payload. Use a [separate backup root](snapshot-backup.md) for corrupt-current recovery. [Remote recovery and format upgrades](operations.md#remote-generation-inventory-and-restore)

## Verified local snapshot backups

On Linux, `kelvo accelerate backup --config kelvo.yml --dataset sales_snapshot --destination /absolute/new-root` copies a verified generation without source queries or secrets. It preserves identity, schema, checksums and age; existing roots are never overwritten.

Recover into another new root, verify it, then switch the service explicitly. See [backup and remote-to-local migration](snapshot-backup.md); remote destinations are unsupported.

## Durable refresh failures

Cluster retries share a five-failure budget per dataset/configuration fingerprint with capped exponential jitter. Schema, configuration and access errors stop immediately; success/failure state is stored before terminal ACK.

```sh
kelvo refresh-status --config worker.yml --dataset sales_snapshot
kelvo refresh-reset --config worker.yml --dataset sales_snapshot \
  --expected-fingerprint CURRENT_CATALOG_FINGERPRINT
```

Repair the cause before resetting with operator NATS authority. The broker fence rejects old deliveries; the bounded 256 KiB status store fails closed instead of evicting permanent failures. Standalone `watch` has no durable cluster retry state.

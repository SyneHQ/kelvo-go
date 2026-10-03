# Dataset acceleration

Refresh a source query into persistent Parquet, then query the copy with DuckDB. This is full-refresh acceleration; each refresh extracts the complete result.

## When to use acceleration

Use it for repeated dashboards, reports or exploration that would otherwise read the source each time. Select only needed columns, date ranges or pre-aggregations; set a refresh interval that matches acceptable staleness.

All built-in native connectors can feed snapshots when their query and Arrow types are supported. Check the [capability and validation matrix](source-coverage.md#acceleration-and-federation-capabilities). Measure source cost and query latency: copying an already efficient analytical database is not automatically faster.

Missing, expired or incompatible snapshots fail with `DATASET_UNAVAILABLE`, without live-source fallback. Corruption can surface during execution; run `verify` for a full check.

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
| `authorization_version` | Required; change after source grants or permitted data change |

Freshness is checked before execution; readers pin the generation for the query's lifetime. Refresh time is not a database transaction watermark.

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

Acquisition checks identity, policy, permissions and size. Full hashing belongs to `verify`. Keep directories and writers trusted and private.

## Tenant and cluster operation

Catalog and worker tenant IDs must match. Callers cannot choose refresh SQL, paths or tenant identity. Dataset-only children receive no original-source secrets.

**Revoke access explicitly:** update `authorization_version`, propagate the catalog and refresh. Retired copies need an operator retention policy; database revocations do not automatically revoke snapshots. Tenant grants are not per-user row policies.

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

Available: full refresh, explicit freshness, local/cluster scheduling, strict schema policy, multipart snapshots and verified recovery. Pending: incremental loading, CDC, result caching, automatic fallback and distributed single-query execution. Independent datasets have no global transactional snapshot.

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

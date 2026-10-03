# Dataset acceleration

Kelvo can materialize a configured source query into a persistent Parquet snapshot and run DuckDB SQL against that snapshot. This is **full-refresh dataset acceleration**, currently a developer preview. It is separate from source-native execution and query-result caching.

The first refresh reads the source. Later queries read the committed Parquet object or local file until another complete refresh publishes a replacement. A missing, expired, incompatible snapshot, or one failing acquisition checks, fails with `DATASET_UNAVAILABLE`. Payload corruption can instead surface during execution; use `verify` for a full digest check. Kelvo does not automatically fall back to the original source.

## When to use acceleration

The main use case is repeated analytics that would otherwise consume source-database CPU and I/O for every dashboard, report and exploratory query. PostgreSQL, MySQL and Oracle are examples, not an allowlist: all built-in native connectors can feed the common snapshot path when the configured query and returned Arrow types are supported. Teams can materialize selected datasets without first deploying a warehouse or lakehouse. Each dataset is optional; direct source queries remain available when live data or source-native execution is preferable. The [capability matrix](source-coverage.md#acceleration-and-federation-capabilities) separates eligibility from live acceleration validation.

For example, a configured orders query can refresh every 15 minutes while dashboards query the committed copy between refreshes. Select only necessary columns, date ranges or pre-aggregations. The copy supports different analytical SQL queries; it is not a cache of one final dashboard response. A read replica can further isolate extraction where the source supports one.

Full refresh still runs the extraction query and transfers its complete result every time. It can be more expensive than infrequent, well-indexed source queries, especially with short refresh intervals. Source scan cost, snapshot storage, refresh CPU, query concurrency and acceptable staleness must be budgeted together. Incremental refresh and CDC are not implemented. A failed refresh retains the previous generation, but queries reject it once `max_age` expires.

Acceleration is not automatically faster than an existing analytical database such as ClickHouse, Snowflake or Trino over a well-designed lakehouse. Measure query latency, source CPU/I/O and application latency under repeated analytical load. The current ClickHouse comparison validates snapshot correctness and source-independent reads, not PostgreSQL or Oracle production impact.

## Run locally

The checked-in [example](../examples/acceleration.yml) uses the existing sales CSV:

```sh
bin/kelvo accelerate refresh --config examples/acceleration.yml --dataset sales_fast
bin/kelvo query --config examples/acceleration.yml --sources sales_fast \
  --sql 'SELECT region, SUM(amount) AS revenue FROM sales_fast GROUP BY region ORDER BY region' \
  --out revenue.arrow
bin/kelvo accelerate status --config examples/acceleration.yml --dataset sales_fast
bin/kelvo accelerate verify --config examples/acceleration.yml --dataset sales_fast
bin/kelvo accelerate watch --config examples/acceleration.yml
```

`refresh` runs once, `watch` schedules configured datasets serially, `status` emits YAML with readiness and the current generation, and `verify` hashes a pinned generation and reports that same generation. File permissions and manifests survive restarts. Successful query statistics include `accelerations` with dataset, generation and refresh-completion time.

Local `query` and `serve` read snapshots but do not automatically start refresh scheduling. Run `watch` as a separately supervised process. `--sandbox /path/to/kelvo-landlock` applies the native Linux sandbox to source refresh subprocesses; cluster nodes require it automatically.

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

Use source-specific SQL for a native refresh; use DuckDB SQL and a `sources` list for a federated refresh. Static SQL and native MongoDB aggregation pipelines are supported; refresh parameters are not. MongoDB uses the same read-only stage validation as ordinary native requests, so `$out` and `$merge` are rejected. Pipeline results preserve BSON documents in Arrow Binary rather than inferring relational columns. See the [MongoDB refresh example](#mongodb-pipeline-refresh). Refresh inputs must be real registered sources, not other accelerated datasets. No recursive materialization graph is created.

Query the new alias explicitly with `--sources recent_orders` and DuckDB SQL. Multiple accelerated aliases can be joined, including aliases ingested through native connectors that do not support live DuckDB federation. The original connection remains available for live queries.

`refresh_interval: 0s` disables scheduling but permits manual refresh. Positive intervals must be at least 5 seconds and no greater than `max_age`. `max_age` must be positive and at most 30 days. Age is measured from completed refresh, not a database commit watermark; source extraction can take time. Each query checks freshness before execution and pins its selected generations for the query's lifetime.

## MongoDB pipeline refresh

Use native YAML for the pipeline; Extended JSON typed values are supported:

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

`documents` must be a built-in MongoDB source. An empty pipeline is valid. Numeric
YAML scalars are parsed without a float64 conversion; duplicate keys, aliases and
non-finite values fail configuration validation. `$out` and `$merge` cannot be
used to turn a refresh into a database write. [Live MongoDB evidence](evidence/mongodb-acceleration.json)
covers filtered/grouped/empty results and unchanged snapshots after rejected writes.

## Object storage

Set the optional `acceleration.object_storage` block to keep committed snapshots
in S3, R2, Google Cloud Storage or Azure Blob. `directory` then holds only private
node-local refresh staging; workers do not need a shared POSIX mount. DuckDB reads
selected byte ranges through a query-lived Go reader that retains the cloud
credentials outside the query subprocess. See [configuration, permissions and
limits](object-storage.md). Object reads currently cannot be mixed with the legacy
PostgreSQL/MySQL DuckDB extensions in the same query; materialize those sources too
when joining them to object snapshots.

## Publication, failure and retention

For the default local POSIX store:

1. The refresher obtains a process-safe dataset writer lock and rechecks whether a scheduled job is still due.
2. A subprocess executes the registered read-only source query. The parent converts borrowed Arrow batches into bounded Parquet row groups using Snappy compression.
3. After successful Arrow completion and Parquet finalization, Kelvo flushes the file, computes SHA-256, marks it read-only, and atomically publishes a YAML manifest.
4. New queries acquire that generation. Running readers retain the old file through a lease. Failed refreshes preserve the previous valid snapshot; crashed staging files are removed by the next writer.

The store normally retains the active generation plus one previous generation. Leased files remain until a later prune after their readers exit. Long-running queries, staging files and a replacement generation require additional disk space. Provision filesystem quotas and monitor capacity; the per-refresh byte limit is not a total-volume quota. An fsync failure after manifest publication is reported as uncertain crash durability rather than claimed success.

Acquisition validates manifest policy, file identity, permissions and size without hashing the whole dataset on every query. `verify` performs the full digest check. Snapshot directories and writers must therefore remain trusted and private; these checks do not protect files from the operating-system administrator.

## Tenant and cluster operation

Each catalog names one tenant. Cluster startup rejects an acceleration tenant that differs from the worker's provisioned tenant. Query callers cannot supply refresh SQL, tenant IDs, storage paths or snapshot filenames. Query subprocesses receive only selected immutable Parquet files; they receive no original-source credentials for dataset-only queries.

`authorization_version` is required. Increment it when source grants, credentials, permitted rows or other authorization policy changes. The stored fingerprint also covers the tenant, dataset query and registered source definitions. A changed fingerprint makes the old snapshot unavailable until refreshed. Copied data does not inherit later database revocations automatically. Revoke access first, update the version and refresh, and remove retired copies according to your retention policy. Tenant-wide source grants are not automatic per-user row-level authorization.

Cluster refreshes use each tenant's existing authenticated NATS account. `cluster-init` provisions `KELVO_ACCEL_QUEUE` and `KV_KELVO_ACCEL_STATUS` in addition to the three existing streams. Increase the account stream quota to at least 5 (6 with source quotas). Existing deployments must explicitly rerun `cluster-init` using provisioner credentials after updating account limits and permissions. Running workers need these additional publish permissions:

```text
acceleration.refresh
$JS.API.CONSUMER.INFO.KELVO_ACCEL_QUEUE.refresh
$JS.API.CONSUMER.MSG.NEXT.KELVO_ACCEL_QUEUE.refresh
$JS.API.STREAM.MSG.GET.KV_KELVO_ACCEL_STATUS
$KV.KELVO_ACCEL_STATUS.>
```

Existing stream-info, inbox and ACK permissions remain required. Only the provisioner may create broker resources. The queue contains dataset identifiers and configuration fingerprints, never source SQL, credentials or Arrow data. Delivery is at least once, with confirmed ACK after successful refresh, delayed retries, heartbeats and writer-lock freshness checks. Each node performs at most one refresh at a time. When node resource budgets are configured, refresh publication and queries share that reservation pool. Include native allocations, Arrow buffers and publication staging in node sizing.

For the default local backend, all workers for a tenant must use matching catalogs and mount the **same tenant-specific snapshot directory** with the same absolute path and OS UID. The filesystem must provide working POSIX `flock`, atomic rename and `fsync`. A local volume shared by containers on one host works; multi-host filesystems need their own verification. The opt-in [object backend](object-storage.md) instead uses immutable remote objects and conditional manifest writes. Automatic SSD replication and locality-aware scheduling are not implemented. Never give one tenant access to another tenant's snapshot volume.

The [Compose overlay](../deploy/acceleration.compose.yml) adds separate writable snapshot mounts while leaving the container root filesystem read-only. Pre-create each host directory with mode 0700 and ownership matching UID 65532. Add the tenant's acceleration block to its existing catalog and run Compose with both files. Use encrypted storage and operator-managed backups for sensitive copies. Do not run conflicting catalog versions against one store during a rolling update; mismatched refresh jobs retry and definition changes can otherwise cause availability gaps.

## Type and memory boundaries

Supported snapshot columns include nullable booleans, all integer widths, float32/64, strings, binary, Date32, Decimal128 up to 38 digits with nonnegative scale, and supported timestamps. Integer and decimal values are not converted through floating point.

Second/millisecond timestamps may promote to microseconds without changing their values. Unzoned nanoseconds remain nanoseconds; UTC aliases normalize. Named timezones, zoned nanoseconds, Decimal256, negative decimal scales, nested/dictionary/extension types and untyped NULL columns are rejected where the current DuckDB/Parquet path cannot preserve semantics. Use an explicit source-side cast when an intentional conversion is acceptable. Ambiguous column names and out-of-range values fail the refresh.

Row groups are bounded by row count and payload, with a separate conservative cap on cumulative footer metadata. Very small input batches or extremely wide schemas can reach that cap before the row limit. The source query, Arrow decoding, Parquet encoding, and DuckDB reading all consume memory; these budgets are not a process-RSS guarantee. DuckDB still materializes execution before Arrow delivery with the pinned driver. Apply process/container resource limits and persistent-volume quotas.

## Current scope and industry standards

This release provides persistent full-refresh snapshots, explicit freshness, isolated alias reads, manual/local scheduling and NATS cluster refresh dispatch. Incremental append, CDC, query-result caching, automatic source fallback, and splitting one query across nodes remain separate work. There is no global transactional snapshot across independent datasets.

Kelvo uses industry standards for its data boundaries: [Arrow's columnar format](https://arrow.apache.org/docs/format/Columnar.html) for typed batches and [Parquet](https://parquet.apache.org/docs/file-format/) for persistent snapshots. DuckDB executes analytical queries over those snapshots. These format choices support interoperability; they do not establish a performance advantage. Compare matching workloads, resource budgets and freshness policies.

### Lifecycle priorities

The [production roadmap](production-roadmap.md) separates implemented behavior from proposed reliability work. The next acceleration capabilities should make dataset state and recovery explicit:

- Schema contracts and verified restore, so operators can detect incompatible changes and recover a usable generation.
- Partitioned generations and incremental checkpoints, so a refresh can replace affected data without rewriting the entire dataset.
- Delete handling, retention and compaction, with reader leases and recovery rules that prevent premature removal.
- Shared resource admission for refreshes and interactive queries, with source quotas and classified retries.

These are planned capabilities, not features of the current snapshot store. Additional storage engines should follow measured workload needs rather than connector count.

### Lessons from Trino

Trino is a distributed SQL engine, not another embedded accelerator. Kelvo connects to customer-managed Trino clusters through its existing native connector. Useful design references are:

- [Pushdown](https://trino.io/docs/current/optimizer/pushdown.html): declare each connector's actual capabilities and expose what runs at the source versus locally.
- [Resource groups](https://trino.io/docs/current/admin/resource-groups.html): budget refresh and interactive work separately, with tenant admission and fair scheduling.
- [Materialized views](https://trino.io/docs/current/connector/iceberg.html#materialized-views): expose freshness and source snapshot identity where available; refresh completion alone does not prove a consistent snapshot across databases.
- [Fault-tolerant execution](https://trino.io/docs/current/admin/fault-tolerant-execution.html): retries need explicit boundaries and durable intermediate storage. NATS whole-query dispatch does not distribute one SQL plan.
- [Dynamic filtering](https://trino.io/docs/current/admin/dynamic-filtering.html): reduce data scanned and transferred before adding more execution engines.

The current direction is Go orchestration, DuckDB execution, Arrow delivery and Parquet snapshots. Partitioned generations, incremental checkpoints, delete handling and compaction should precede additional storage engines unless measured workloads justify a different order.


## Schema contracts and generation recovery

Refresh compares the original Arrow schema with the prior committed generation
under the writer lock/lease. Matching is strict by default: field order, exact
types, nullability and schema/field metadata must match. Optional dataset
[`schema_evolution`](schema-evolution.md) flags permit append-only nullable fields
and conservative widening independently. Existing nullability, metadata, renames,
removals and timestamp changes remain protected; no implicit casts are performed.
The new policy's integrated validation is pending.

A mismatch returns `SCHEMA_MISMATCH` and preserves the previous generation.
Within a generation, all batches and parts still require one exact schema.
Original Arrow schema metadata is read from legacy snapshots; new manifests also
persist its fingerprint. Object schema reads use bounded, version-pinned ranges.
Changing an effective evolution policy changes the catalog fingerprint: old data
is unavailable under the new policy until a complete refresh succeeds. Coordinate
cluster worker/scheduler configuration changes, and retain `authorization_version`
changes for permission updates. See the policy guide for rollout and restore limits.

Local and remote stores support operator-only inventory and restore:

```sh
kelvo accelerate inventory --config kelvo.yml --dataset sales_snapshot
kelvo accelerate restore --config kelvo.yml --dataset sales_snapshot \
  --generation PREVIOUS_GENERATION --expected-generation CURRENT_GENERATION
```

Restore verifies the target and current payload checksums, exact schemas and the
current catalog's authorization fingerprint, then switches the manifest under the
writer lock. It retains the original data timestamp; an old restore may correctly
remain stale. The expected generation prevents overwriting a concurrent refresh.
Local reader leases protect pinned payloads; local inventory is capped at 256
retained generations. Remote inventory uses a bounded manifest catalog of current
plus at most 16 historical entries, rather than listing storage. See
[remote recovery and protocol upgrades](operations.md#remote-generation-inventory-and-restore).
Schema evolution never relaxes restore: old-policy fingerprints and backward
schema changes remain rejected. Recovery requires a verifiable current generation;
repair of a corrupt current payload is a separate procedure. Use [verified local backups](snapshot-backup.md) for separate recovery roots.
Remote backup remains an operator-managed procedure. Generation restore alone
does not establish measured RTO/RPO.

## Verified local snapshot backups

On Linux, `kelvo accelerate backup --config kelvo.yml --dataset sales_snapshot --destination /absolute/new-root` copies one verified current generation into a
new private root. It preserves schema, checksums, authorization fingerprint and
refresh time, including stale data. The configured snapshot byte budget also
bounds the copy. Source queries and database secrets are not needed.

Recovery uses the same command with a catalog pointing at the backup and another
new destination; the operator explicitly switches the service afterward. Existing
roots are never overwritten, and object-storage backup is unsupported. See the
[backup and recovery guide](snapshot-backup.md) for private-directory requirements,
per-dataset scope, error ambiguity after publication and freshness checks.

## Durable refresh failures

Cluster retries have a five-failure budget across scheduled messages for an exact
dataset/configuration fingerprint, with capped exponential jitter. Schema,
configuration and access failures stop immediately. Unknown driver failures retry
conservatively within the budget because upstream connectors may sanitize their
original error classification. `RetryAfter()` hints are respected within the
bounded policy. Successful refresh timestamps and sanitized failure categories
are stored in a tenant-scoped KV bucket before terminal acknowledgement.

```sh
kelvo refresh-status --config worker.yml --dataset sales_snapshot
kelvo refresh-reset --config worker.yml --dataset sales_snapshot \
  --expected-fingerprint CURRENT_CATALOG_FINGERPRINT
```

Reset requires operator NATS authority and the catalog fingerprint. A broker
sequence fence rejects old deliveries after reset. Status has a bounded 256 KiB
store and does not expire permanent failures; exhaustion fails closed instead of
evicting safety state. Operators must manage obsolete configuration records as
part of a deliberate migration. Standalone `accelerate watch` retains its local
scheduler; durable retry state is a cluster feature.

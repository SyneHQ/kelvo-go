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

Cluster refreshes use each tenant's existing authenticated NATS account. `cluster-init` provisions `KELVO_ACCEL_QUEUE` in addition to the three existing streams. Increase the account stream quota to at least 4. Running workers need these additional publish permissions:

```text
acceleration.refresh
$JS.API.CONSUMER.INFO.KELVO_ACCEL_QUEUE.refresh
$JS.API.CONSUMER.MSG.NEXT.KELVO_ACCEL_QUEUE.refresh
```

Existing stream-info, inbox and ACK permissions remain required. Only the provisioner may create broker resources. The queue contains dataset identifiers and configuration fingerprints, never source SQL, credentials or Arrow data. Delivery is at least once, with confirmed ACK after successful refresh, delayed retries, heartbeats and writer-lock freshness checks. Each node performs at most one refresh at a time, separately from its query admission capacity. Include refresh CPU/memory in node sizing.

For the default local backend, all workers for a tenant must use matching catalogs and mount the **same tenant-specific snapshot directory** with the same absolute path and OS UID. The filesystem must provide working POSIX `flock`, atomic rename and `fsync`. A local volume shared by containers on one host works; multi-host filesystems need their own verification. The opt-in [object backend](object-storage.md) instead uses immutable remote objects and conditional manifest writes. Automatic SSD replication and locality-aware scheduling are not implemented. Never give one tenant access to another tenant's snapshot volume.

The [Compose overlay](../deploy/acceleration.compose.yml) adds separate writable snapshot mounts while leaving the container root filesystem read-only. Pre-create each host directory with mode 0700 and ownership matching UID 65532. Add the tenant's acceleration block to its existing catalog and run Compose with both files. Use encrypted storage and operator-managed backups for sensitive copies. Do not run conflicting catalog versions against one store during a rolling update; mismatched refresh jobs retry and definition changes can otherwise cause availability gaps.

## Type and memory boundaries

Supported snapshot columns include nullable booleans, all integer widths, float32/64, strings, binary, Date32, Decimal128 up to 38 digits with nonnegative scale, and supported timestamps. Integer and decimal values are not converted through floating point.

Second/millisecond timestamps may promote to microseconds without changing their values. Unzoned nanoseconds remain nanoseconds; UTC aliases normalize. Named timezones, zoned nanoseconds, Decimal256, negative decimal scales, nested/dictionary/extension types and untyped NULL columns are rejected where the current DuckDB/Parquet path cannot preserve semantics. Use an explicit source-side cast when an intentional conversion is acceptable. Ambiguous column names and out-of-range values fail the refresh.

Row groups are bounded by row count and payload, with a separate conservative cap on cumulative footer metadata. Very small input batches or extremely wide schemas can reach that cap before the row limit. The source query, Arrow decoding, Parquet encoding, and DuckDB reading all consume memory; these budgets are not a process-RSS guarantee. DuckDB still materializes execution before Arrow delivery with the pinned driver. Apply process/container resource limits and persistent-volume quotas.

## Scope compared with Spice

This release provides persistent full-refresh snapshots, explicit freshness, isolated alias reads, manual/local scheduling and NATS cluster refresh dispatch. Incremental append, CDC, query-result caching, automatic source fallback, and splitting one query across nodes remain separate work. There is no global transactional snapshot across independent datasets.

Spice supplies a larger dataset lifecycle around DataFusion and multiple accelerator engines, including DuckDB. Bare DataFusion is not a database gateway: its extra live database coverage comes from community table providers and application connectors. Kelvo's materialized aliases extend joins without adding a Rust runtime dependency. Neither language nor connector count establishes a performance advantage; compare matching workloads and freshness policies.

### Accelerator engines upstream

Spice's [accelerator catalog](https://spiceai.org/docs/components/data-accelerators) describes these choices. Availability depends on the edition and build features; this is an upstream inventory, not a Kelvo support matrix.

| Engine | Storage role | Documented maturity |
| --- | --- | --- |
| Arrow | In-memory columnar batches; rebuilt after restart | Stable |
| DuckDB | Embedded analytical database, in memory or on disk | Stable |
| SQLite | Embedded SQL store with memory/file modes | Release Candidate |
| PostgreSQL | Attached PostgreSQL server holding the accelerated data | Release Candidate; documented as Enterprise-only |
| Cayenne | Vortex columnar files with SQLite/Turso metadata | Stable |
| Turso | Embedded SQLite-compatible database, in memory or on disk | Beta |

PostgreSQL accelerator code is public behind a feature flag despite the documentation's edition label. Turso is included in the current upstream default feature list. Check the [runtime manifest](https://github.com/spiceai/spiceai/blob/trunk/bin/spiced/Cargo.toml) when selecting a particular build. [Cayenne](https://github.com/spiceai/spiceai/blob/trunk/crates/cayenne/README.md) uses Vortex rather than Parquet; its write-ahead log, recovery, deletion and compaction design is useful reference material, not functionality supplied by Kelvo's snapshot store.

### Lessons from Trino

Trino is a distributed SQL engine, not another embedded accelerator. Kelvo connects to customer-managed Trino clusters through its existing native connector. Useful design references are:

- [Pushdown](https://trino.io/docs/current/optimizer/pushdown.html): declare each connector's actual capabilities and expose what runs at the source versus locally.
- [Resource groups](https://trino.io/docs/current/admin/resource-groups.html): budget refresh and interactive work separately, with tenant admission and fair scheduling.
- [Materialized views](https://trino.io/docs/current/connector/iceberg.html#materialized-views): expose freshness and source snapshot identity where available; refresh completion alone does not prove a consistent snapshot across databases.
- [Fault-tolerant execution](https://trino.io/docs/current/admin/fault-tolerant-execution.html): retries need explicit boundaries and durable intermediate storage. NATS whole-query dispatch does not distribute one SQL plan.
- [Dynamic filtering](https://trino.io/docs/current/admin/dynamic-filtering.html): reduce data scanned and transferred before adding more execution engines.

The current direction is Go orchestration, DuckDB execution, Arrow delivery and Parquet snapshots. Partitioned generations, incremental checkpoints, delete handling and compaction should precede additional storage engines unless measured workloads justify a different order.

Further references: [Spice refresh modes](https://spiceai.org/docs/features/data-acceleration/data-refresh), [DataFusion sources](https://datafusion.apache.org/user-guide/features.html#data-sources), [community table providers](https://github.com/datafusion-contrib/datafusion-table-providers#table-providers).

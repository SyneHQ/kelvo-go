# Local multipart acceleration

Opt-in multipart acceleration stores one complete dataset generation as an ordered
set of immutable Parquet files. DuckDB reads the explicit file list as one table,
so existing analytical SQL and joins continue to use the dataset alias. It keeps
one Parquet encoder active at a time and bounds each part's metadata growth.

This implementation passed the development gates recorded below. Those results
do not establish a general throughput or production-capacity guarantee.

## Configure

Add `multipart` to a dataset in a local acceleration catalog:

```yaml
sources:
  - id: warehouse
    type: clickhouse
    url_env: KELVO_SOURCE_WAREHOUSE_URL
acceleration:
  tenant_id: tenant-a
  directory: /var/lib/kelvo/acceleration
  datasets:
    - id: orders_fast
      query:
        mode: native
        connection_id: warehouse
        sql: SELECT id, region, amount FROM analytics.orders
      authorization_version: orders-readers-v1
      refresh_interval: 15m
      max_age: 1h
      multipart:
        max_part_bytes: 268435456
        max_parts: 64
      limits:
        max_rows: 50000000
        max_bytes: 8589934592
        timeout: 30m
        memory_mb: 512
        threads: 2
        max_temp_mb: 2048
```

These example limits authorize work; they are not a sizing recommendation. Leave
`multipart` absent to retain single-file publication. Multipart requires local
storage: configuration rejects combining it with `acceleration.object_storage`.
Existing object-storage snapshots remain on their separate single-object path.

Use the existing commands:

```sh
bin/kelvo accelerate refresh --config kelvo.yml --dataset orders_fast
bin/kelvo accelerate verify --config kelvo.yml --dataset orders_fast
bin/kelvo query --config kelvo.yml --sources orders_fast \
  --sql 'SELECT region, SUM(amount) FROM orders_fast GROUP BY region' \
  --out revenue.arrow
```

See [dataset acceleration](acceleration.md) for refresh scheduling, source
permissions, freshness, tenant isolation and supported Arrow types.

## Limits and rotation

| Setting or bound | Behavior |
| --- | --- |
| `max_part_bytes` | Hard encoded limit per part, including its header and footer; 1 MiB–4 GiB, and no greater than dataset `limits.max_bytes`. |
| `max_parts` | Configured ceiling of 2–256; a small or empty result may still produce one part. |
| `limits.max_bytes` | Aggregate encoded snapshot limit across all parts; existing source/result transfer limits still apply independently. The general configuration ceiling is 1 TiB. |
| `limits.max_rows` | Aggregate rows across the entire refresh, up to the existing 100 million configuration ceiling. |
| Rotation target | Estimated uncompressed data reaches half `max_part_bytes`, or a part reaches 1,048,576 rows or its metadata group budget. |
| Row groups and pages | At most 8,192 rows per group, with a memory-dependent uncompressed budget capped at 8 MiB; the encoder targets 64 KiB data pages. Page size is a target, not a separate hard part limit. |
| Metadata | At most 4,096 columns, 1 MiB serialized Arrow schema, 64 MiB Parquet footer input and 64 KiB generation manifest. Row-group metadata also has a memory-dependent count budget. |

The half-byte rotation target leaves room for encoding overhead. It is not an
exact encoded-size prediction. Compression, wide schemas and footer overhead can
still exhaust a hard limit. Such a refresh fails instead of truncating data or
publishing a partial generation. A row that cannot fit the uncompressed budget
also fails explicitly. Snappy remains the snapshot compression codec.

`max_parts * max_part_bytes` is only another upper bound, not a guaranteed usable
capacity. The row target or conservative byte estimate can exhaust the part
count sooner. Raising the total byte limit does not override per-part, part-count,
row, timeout, source-scan or resource-admission limits.

## Publication and recovery

One writer transaction owns the entire refresh. Each part receives a completed
Parquet footer before sealing. The manifest records ordered part identities,
per-part rows, encoded sizes and SHA-256 digests, aggregate totals and an exact
Arrow schema fingerprint. All parts belong to the same generation and must share
the schema contract. Cross-generation schema changes continue to fail explicitly.

Only a complete successful extraction and finalized generation may replace the
current manifest atomically. Cancellation, encoding failure and pre-publication
failure preserve the previous generation. An error after the manifest swap may
report uncertain directory durability; it must not be interpreted as proof that
publication never happened.

Queries acquire leases on every selected part while metadata is protected, then
hold those leases until execution ends. A concurrent refresh cannot combine old
and new parts within one dataset read. Different dataset aliases still acquire
their generations independently; a join is not a distributed source snapshot.

Acquisition checks metadata, authorization, freshness, file permissions and sizes;
it does not hash every payload on every query. Use `verify` for full integrity
checking. Inventory and restore use the existing verified-generation recovery
flow and schema checks. Restoring a generation preserves its original freshness;
restore does not reset `max_age`.

The refresh manager normally retains the current generation, one previous
generation and any still-leased generations. Staging, replacement and leased old generations can
coexist, so `limits.max_bytes` is not a filesystem quota. Recovery and pruning
require a private trusted filesystem and exclusive writer coordination. Crash-recovery and retention checks include interrupted pruning and publication,
missing initial metadata and malformed metadata that must prevent deletion.

## Reader upgrade and compatibility

Single-file local manifests remain version 1. A multipart refresh publishes local
manifest version 2. Upgrade every process that reads, refreshes, inventories,
restores or prunes the shared local store before enabling multipart. Older binaries
reject version 2; do not mix old and new workers against a migrated store.

Existing version 1 snapshots remain readable by the new implementation. Storage
layout is excluded from the authorization fingerprint, so enabling multipart does
not invalidate the current authorized snapshot before a successful replacement.
A refresh that is not yet due may keep that existing generation. The exact schema
contract still applies when moving between layouts.

Removing `multipart` changes future writes to single-file format; it does not
rewrite retained multipart history. It is not a safe rollback procedure for old
binaries. Plan and validate a separate data/metadata migration before downgrading.
Local manifest versions are independent of remote object-manifest versions.

## Worker resources and boundaries

The trusted parent supplies `parquet_paths` internally; user YAML cannot supply
that field. Paths must be unique, absolute local filenames without glob patterns
or conflicting source settings. DuckDB binds an explicit `read_parquet` list and
its file allowlist; the Linux sandbox grants exact files, never their containing
snapshot directory.

Each multipart lease holds one file descriptor per part in the parent. DuckDB can
open additional descriptors while scanning. Across all selected sources, the Go
sandbox and native launcher accept at most 1,024 distinct source file grants. The
Go argument builder additionally caps source path arguments at 512 KiB. The worker's existing 2 MiB JSON input limit
also remains. Four 256-part datasets can consume the entire file-grant budget;
other selected files reduce that capacity. Configure process descriptor limits
and concurrency with headroom for runtime, database and coordination sockets.

Multipart bounds the snapshot encoder's work; it does not bound the source
engine's memory. The pinned DuckDB driver still materializes execution before
Arrow delivery. Input batches, concurrent readers, native engine allocations,
page caches, metadata and refresh verification also consume resources. Measure
peak RSS, descriptor use, disk space and latency on the intended workload.

Remote multipart publication, incremental refresh, partition replacement, CDC
application and remote object garbage collection are not implemented by this
feature. Splitting files also does not distribute one DuckDB query across machines.


## Development validation

The dedicated Azure Linux VM passed the complete ordinary and pinned DuckDB
bridge suites, focused race checks, `go vet`, binary and sandbox builds, and bridge
`cgocheck2` checks. The [six-check real-process acceptance](evidence/multipart-acceptance.json)
compared all 100,000 rows with the source, exercised an exact-file sandboxed join,
restored both manifest formats, exhausted the part limit and detected corruption
in a nonfirst part. Legacy single-file acceptance remains separate.

The opt-in [large dataset gate](evidence/multipart-large-dataset.json) published
4,456,448 rows in 138 parts totaling 4,600,321,235 encoded bytes, then verified the
generation and ran a real DuckDB aggregate comparing every payload's SHA-256 with
a deterministic reference. Its combined test process peaked at 220,980 KiB RSS
(about 216 MiB). The gate used a synthetic bounded Arrow producer; this does not
measure a native database driver's ingestion memory. It excludes filesystem cache,
cgroup charges, concurrent tenant load and remote transfer. Its elapsed time
includes integrity verification and is not an isolated throughput benchmark.
See the evidence for the final recovery-fix validation boundary.

Run the expensive gate only on a provisioned test machine with sufficient free
disk space; it writes over 4 GiB and cleans its test directory afterward:

```sh
KELVO_TEST_MULTIPART_LARGE=1 go test -tags duckdb_arrow \
  ./internal/acceleration -run TestMultipartDatasetLargerThanFourGiB \
  -count=1 -timeout=30m -v
```

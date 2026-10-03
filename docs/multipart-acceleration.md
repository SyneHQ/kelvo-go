# Multipart acceleration

Opt-in multipart acceleration stores one complete dataset generation as an ordered
set of immutable Parquet files. DuckDB reads the explicit file list as one table,
so existing analytical SQL and joins continue to use the dataset alias. It keeps
one Parquet encoder active at a time and bounds each part's metadata growth.

This implementation passed the development gates recorded below. Those results
do not establish a general throughput or production-capacity guarantee.

## Configure

Add `multipart` to a dataset in an acceleration catalog:

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
`multipart` absent to retain single-file publication. With no `object_storage`
block, parts use the private local store. With `acceleration.object_storage`
configured, parts and their descriptor use S3, R2, GCS or Azure Blob through the
existing provider clients; `directory` then holds private local staging. See
[object storage](object-storage.md) for provider endpoints and separate reader and
writer credential references. Remote multipart passed the provider-shaped TLS and over-4-GiB component gates
described below; actual cloud-account acceptance remains separate.

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
| Metadata | At most 4,096 columns, 1 MiB serialized Arrow schema, 64 MiB Parquet footer input, 64 KiB local/root manifest and 2 MiB remote multipart descriptor. Row-group metadata also has a memory-dependent count budget. |

The half-byte rotation target leaves room for encoding overhead. It is not an
exact encoded-size prediction. Compression, wide schemas and footer overhead can
still exhaust a hard limit. Such a refresh fails instead of truncating data or
publishing a partial generation. A row that cannot fit the uncompressed budget
also fails explicitly. Snappy remains the snapshot compression codec.

`max_parts * max_part_bytes` is only another upper bound, not a guaranteed usable
capacity. The row target or conservative byte estimate can exhaust the part
count sooner. Raising the total byte limit does not override per-part, part-count,
row, timeout, source-scan or resource-admission limits.

## Local publication and recovery

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

## Remote publication and recovery

The remote writer keeps one local staging part at a time. It finalizes that part's
footer, verifies rows and schema, hashes the encoded bytes, uploads under an
immutable absent-key condition, then confirms size, SHA-256 metadata and the exact
returned version with HEAD. Only then does it remove that part's local staging
file and start the next. Uploads are sequential; no part-per-goroutine fan-out is
introduced. Aggregate encoded bytes and rows remain bounded across all parts.

After every part succeeds, the writer uploads and confirms a bounded immutable
YAML descriptor. It records ordered per-part versions, sizes, row counts and
hashes, plus generation identity, totals and the exact schema fingerprint. Keys
are derived from the configured prefix, tenant, dataset, generation and sequential
part index; descriptors cannot substitute arbitrary object keys.

The root manifest retains a versioned descriptor reference and its SHA-256.
A single conditional root update publishes the whole generation using the
existing renewable writer lease. A failed part or descriptor upload exposes no
partial generation. If a root write has an ambiguous outcome, Kelvo reconciles
against the exact committed metadata and otherwise reports publication unknown.
It never guesses failure and deletes possibly published data.

Remote acquisition checks the descriptor's exact version, size, complete digest,
identity and totals, then checks part metadata with at most four concurrent HEAD
requests. Full `verify` and restore additionally stream payload hashes and check
footer row counts and exact schemas. This does not download every payload during
ordinary acquisition. Restored generations keep their original freshness.

The root manifest remains capped at 64 KiB, retaining current plus at most 16
historical generations, with fewer entries when necessary for the byte budget.
Each descriptor is capped at 2 MiB; every leaf is at most 4 GiB, with at most 256
parts and a configured aggregate ceiling of 1 TiB. These are validation ceilings,
not measured production capacity.

Remote pruning does not list or delete objects. Failed uploads, abandoned parts,
descriptors and retired generations may remain indefinitely. Metadata eviction
does not reclaim storage. Reader safety currently relies on immutable versions
and the absence of automatic deletion, rather than distributed reader leases.
Operator cleanup must account for every active reader and retained generation.

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
Remote readers accept versions 2, 3 and 4. **Every remote writer action now emits
version 4**, including the first lease claim or renewal, even when multipart is
absent or a refresh later fails. Version 4 can reference either a single object or
a multipart descriptor. It retains the bounded history introduced in version 3.

Coordinate all remote reader and writer upgrades before permitting any new writer
action. Older binaries reject version 4; binary rollback after the first lease
claim is unsupported without a separately validated metadata/data recovery plan.
Disabling multipart does not undo this upgrade, and changing the version number
by hand is not a downgrade procedure.

## Worker resources and boundaries

For local snapshots, the trusted parent supplies `parquet_paths` internally;
user YAML cannot supply that field. Paths must be unique, absolute local filenames without glob patterns
or conflicting source settings. DuckDB binds an explicit `read_parquet` list and
its file allowlist; the Linux sandbox grants exact files, never their containing
snapshot directory.

Each local multipart lease holds one file descriptor per part in the parent. DuckDB can
open additional descriptors while scanning. Across all selected sources, the Go
sandbox and native launcher accept at most 1,024 distinct source file grants. The
Go argument builder additionally caps source path arguments at 512 KiB. The worker's existing 2 MiB JSON input limit
also remains. Four 256-part datasets can consume the entire file-grant budget;
other selected files reduce that capacity. Configure process descriptor limits
and concurrency with headroom for runtime, database and coordination sockets.

For remote snapshots, the parent supplies process-only `object_ranges` instead of
filesystem paths. Every URL is a loopback capability bound to one query token,
dataset and sequential part index. Lists are capped at 256 per source and 1,024
capabilities per query. DuckDB receives an explicit URL list and exact allowlist;
legacy PostgreSQL/MySQL extensions that require broader native network access
cannot share that object query. Go-backed adapters preserve the exact restrictions.

One query-owned bridge serves all selected remote parts with a shared four-request
upstream range limit, 32 MiB maximum range and one 32 KiB transfer buffer per active
request. Excess requests fail boundedly rather than creating a per-part queue.
The bridge checks exact object versions, sizes, digest metadata and range response
lengths. It refuses whole-object GETs and redirects, and its lifetime ends with the
query. Cloud credentials, upstream keys and provider URLs remain in the trusted
parent; query children receive only capabilities and public sizes.

Multipart bounds the snapshot encoder's work; it does not bound the source
engine's memory. The pinned DuckDB driver still materializes execution before
Arrow delivery. Input batches, concurrent readers, native engine allocations,
page caches, metadata and refresh verification also consume resources. Measure
peak RSS, descriptor use, disk space and latency on the intended workload.

Incremental refresh, partition replacement, CDC application and remote object
garbage collection are not implemented by this
feature. Splitting files also does not distribute one DuckDB query across machines.


## Development validation

Runtime milestone `71f3390` passed the ordinary and pinned DuckDB bridge suites,
focused races, vet/build checks and bridge `cgocheck2`. The
[20-check remote multipart acceptance](evidence/object-multipart-acceptance.json)
exercised publication, reader-only queries, single/multipart restore, descriptor
corruption, missing parts and transport failures across S3, R2, GCS and Azure
TLS protocol fixtures. The separate
[36-check legacy object regression](evidence/object-multipart-legacy-acceptance.json)
also passed. Temporary test trust was removed by fixture cleanup.

The [remote large dataset gate](evidence/object-multipart-large-dataset.json)
published 4,456,448 rows, 4,600,321,235 encoded bytes and 138 parts through a
disk-backed immutable object fixture. It checked staging removal after every
part, verified all payloads, then used real DuckDB/HTTPFS and guarded loopback
ranges to compare every payload's SHA-256 against deterministic reference hashes.
The combined process peaked at 467,868 KiB RSS (about 457 MiB); this includes the
synthetic Arrow source, object fixture and DuckDB, and excludes filesystem cache.
It is not a cgroup limit, native database ingestion measurement, micro-VM sizing
result or cloud/WAN throughput benchmark.

Fixture success does not establish actual S3, R2, GCS or Azure account acceptance,
network throughput or production readiness.

The records below cover the completed **local multipart** development gates;
they do not automatically extend to remote multipart.


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


Run the corresponding object-layout gate with an installed signed HTTPFS
extension directory on a provisioned test machine:

```sh
KELVO_TEST_OBJECT_MULTIPART_LARGE=1 \
KELVO_OBJECT_EXTENSION_DIRECTORY=/path/to/approved/extensions \
go test -tags duckdb_arrow ./internal/acceleration \
  -run TestObjectMultipartDatasetLargerThanFourGiB -count=1 -timeout=30m -v
```

# Object-backed dataset snapshots

Publish full-refresh Parquet snapshots to S3, R2, GCS or Azure Blob with `acceleration.object_storage`. This opt-in developer preview shares committed snapshots across workers without a shared filesystem; local POSIX storage remains the default.

Refreshers need private staging disk for one in-progress file/part. Queries read remote ranges without a full local copy. [Source/type/freshness rules](acceleration.md) still apply; object storage adds neither CDC nor a lakehouse table format.

For durable reader leases, use a fresh namespace and the [protected-reader setup](protected-object-readers.md). It requires contained nodes and a separate registry identity. The commands below describe legacy namespaces unless stated otherwise.

## Configure one provider

1. Start with the [complete example](../examples/object-storage.yml). Keep one YAML document and one active provider block.
2. Choose a verified HTTPS origin, bucket/container and required private prefix. Tenant/dataset names are appended by configuration, not query callers.
3. Provision separate reader and writer identities and store only their environment references in YAML.

S3 requires a signing region:

```yaml
object_storage:
  provider: s3
  endpoint: https://s3.us-east-1.amazonaws.com
  bucket: example-kelvo-snapshots
  prefix: kelvo/snapshots
  region: us-east-1
  read_credentials:
    access_key_id_env: KELVO_SOURCE_OBJECT_READER_ID
    secret_access_key_env: KELVO_SOURCE_OBJECT_READER_SECRET
  write_credentials:
    access_key_id_env: KELVO_SOURCE_OBJECT_WRITER_ID
    secret_access_key_env: KELVO_SOURCE_OBJECT_WRITER_SECRET
```

For temporary S3 credentials, add `session_token_env` separately to each identity. Session tokens are supported only for S3.

R2 uses the account-specific S3 origin:

```yaml
object_storage:
  provider: r2
  endpoint: https://00000000000000000000000000000000.r2.cloudflarestorage.com
  bucket: example-kelvo-snapshots
  prefix: kelvo/snapshots
  region: auto
  read_credentials:
    access_key_id_env: KELVO_SOURCE_OBJECT_READER_ID
    secret_access_key_env: KELVO_SOURCE_OBJECT_READER_SECRET
  write_credentials:
    access_key_id_env: KELVO_SOURCE_OBJECT_WRITER_ID
    secret_access_key_env: KELVO_SOURCE_OBJECT_WRITER_SECRET
```

GCS uses HMAC interoperability keys, its XML API and object-generation preconditions:

```yaml
object_storage:
  provider: gcs
  endpoint: https://storage.googleapis.com
  bucket: example-kelvo-snapshots
  prefix: kelvo/snapshots
  region: auto
  read_credentials:
    access_key_id_env: KELVO_SOURCE_OBJECT_READER_ID
    secret_access_key_env: KELVO_SOURCE_OBJECT_READER_SECRET
  write_credentials:
    access_key_id_env: KELVO_SOURCE_OBJECT_WRITER_ID
    secret_access_key_env: KELVO_SOURCE_OBJECT_WRITER_SECRET
```

Azure uses token-only SAS references. The reader must have exactly `sp=r`; replace the account/container below:

```yaml
object_storage:
  provider: azure
  endpoint: https://examplestorage.blob.core.windows.net
  account: examplestorage
  bucket: kelvo-snapshots
  prefix: kelvo/snapshots
  read_credentials:
    sas_token_env: KELVO_SOURCE_OBJECT_READER_SAS
  write_credentials:
    sas_token_env: KELVO_SOURCE_OBJECT_WRITER_SAS
```

Origins cannot contain paths, credentials or queries. All references require dedicated `KELVO_SOURCE_*` names, with disjoint reader/writer names. Keep values in the parent environment or secret manager, never arguments, logs or source control.

Readers need exact-object reads/metadata. Publishers need reads and conditional create/replace within the prefix; Azure needs read/create/write. No list/delete permissions are required. Writer credentials load only during refresh.

## Run a refresh and query

Unrestricted reads need matching signed `httpfs` on the designated host; queries never install extensions. [Principal-guarded snapshots](guarded-snapshots.md) use the Go reader and need no `httpfs`. Neither path uses DuckDB's Azure extension.

```sh
python3 scripts/provision_extensions.py artifacts/extensions --extensions httpfs
bin/kelvo accelerate refresh --config examples/object-storage.yml --dataset sales_fast
bin/kelvo accelerate status --config examples/object-storage.yml --dataset sales_fast
bin/kelvo accelerate verify --config examples/object-storage.yml --dataset sales_fast
bin/kelvo query --config examples/object-storage.yml --sources sales_fast \
  --sql 'SELECT region, SUM(amount) AS revenue FROM sales_fast GROUP BY region ORDER BY region' \
  --out revenue.arrow
```

Supervise `bin/kelvo accelerate watch --config examples/object-storage.yml` for scheduling. Query-only processes need readers; refreshers need both identities and source credentials. Tenant catalogs/remote namespaces must match; staging directories may differ.

## Publication and freshness

Single-file keys use:

```text
<prefix>/<tenant>/<dataset>/<32-hex-generation>.parquet
<prefix>/<tenant>/<dataset>/current.yaml
```

[Multipart generations](multipart-acceleration.md) add immutable parts/descriptors. **Legacy readers accept v2/v3/v4; legacy writers emit v4. Protected namespaces require v5 throughout.** A writer lease claim can upgrade the root before any successful refresh. Coordinate upgrades first; disabling multipart does not make old-binary rollback safe.

The bounded root manifest binds committed identity, digest, size, fingerprint, age, revision, history and writer lease. Pointer and lease share one conditional object, fencing replaced writers.

| Provider | Revision check | Create-only check |
| --- | --- | --- |
| S3 / R2 / Azure | ETag with `If-Match` | `If-None-Match: *` |
| GCS | Object generation | Generation match `0` |

ETags are opaque revisions, not digests. Refresh uploads immutable data, verifies metadata, then conditionally replaces current. The 60-second lease renews every 15 seconds; renewal loss cancels extraction and prevents stale publication.

Service time establishes lease and snapshot age; monotonic elapsed time advances the observation through I/O. Acquisition selects `current.yaml` once, rejects fingerprint mismatches or stale snapshots before generation reads, and rechecks `max_age` after descriptor and metadata reads. `status` still validates stale snapshots; failed refresh retains current.

Lost publication replies trigger bounded readback. Exact matches succeed; unresolved outcomes preserve both generations and possibly published writer state. Inspect `status`/`verify` before another refresh.

## Query credential and integrity boundaries

The parent verifies selected manifests/metadata and starts one query listener on `127.0.0.1`. Children receive sizes and URLs with unpredictable 256-bit capabilities, never cloud endpoints, keys, credential references or DuckDB cloud secrets. Do not persist capability URLs in logs.

The bridge serves HEAD from acquired metadata and GET only for explicit single closed ranges. Unknown paths, host changes, bodies, other methods, multiple ranges and full GETs fail. Child headers are not forwarded. Provider requests reject redirects and bind exact revision, size and SHA-256 metadata; short/oversized bodies remain incomplete even after partial delivery.

Cancellation stops the listener/upstream requests and waits for cleanup. DuckDB's allowlist independently restricts selected capabilities.

Unrestricted range reads do not recompute every payload hash. `accelerate verify` streams all pinned payloads and checks footer rows and original Arrow schema. [Guarded reads](guarded-snapshots.md) hash each part at discovery and before every scan, adding network I/O. Both depend on immutable provider versions after verification.

Custom backends require `objectstore.RangeClient` or return `ErrRecoveryUnsupported`; built-ins implement it. Legacy schema-less snapshots remain readable only without row/column restrictions. Preserve immutability; changed revisions fail.

## Limits and retention

| Resource | Bound |
| --- | --- |
| Single file / multipart part | One upload request, at most 4 GiB |
| Optional multipart generation | At most 256 parts and 1 TiB; [other limits apply](multipart-acceleration.md#limits-and-rotation) |
| Per-query range bridge | Four concurrent requests, 32 MiB each, 32 KiB copy buffer per active request |
| Range deadline | 60 seconds plus query cancellation |

Request headers/read/idle times are bounded. Excess concurrency/ranges fail. These bounds exclude TLS/native memory and aggregate concurrency across queries. HTTPFS full-download fallback is disabled, but a full scan can still fetch all data through ranges.

Object/local snapshots can join each other. Legacy PostgreSQL/MySQL extensions require broader external access and cannot share object queries; use [custom Go adapters](federation.md) or snapshots instead.

Legacy namespaces have no distributed reader lease. [Protected namespaces](protected-object-readers.md) add durable leases, but **neither mode enables remote garbage collection**. Prune never lists/deletes payloads. Keep retired/orphaned objects; age-only lifecycle deletion can remove committed/readable data.

Changing provider, bucket, prefix or dataset definition does not migrate data. Refresh the new configuration or use an explicit [remote-to-local migration](snapshot-backup.md#migrate-a-current-object-snapshot-to-local-storage).

## Writer shutdown

[Object backend shutdown](object-writer-shutdown.md) cancels and joins admitted writers before closing its reader client. Embedders must finish borrowed files and call `Commit` or `Abort`; cancellation alone does not prove cleanup.

## Validation scope

[Remote multipart evidence](evidence/object-multipart-acceptance.json) and [legacy regression](evidence/object-multipart-legacy-acceptance.json) cover conditional publication, independent workers, failure recovery and guarded ranges against provider-shaped fixtures. They do not establish live cloud IAM, availability or throughput.

Run focused checks on the designated host:

```sh
GOMAXPROCS=2 go test -p 2 -race -count=1 ./internal/acceleration \
  -run 'TestObjectRanges|TestManagerObjectScheduling|TestObjectBackend|TestStore'
GOMAXPROCS=2 go test -p 2 ./internal/acceleration -run '^$' \
  -bench '^BenchmarkObjectRangesLoopbackStream$' -benchtime=3x -count=1
```

The loopback benchmark includes its generator, Go bridge and local client. Treat its output as a component measurement, not cloud-storage or DuckDB capacity.

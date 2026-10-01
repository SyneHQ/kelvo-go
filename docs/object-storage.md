# Object-backed dataset snapshots

Set `acceleration.object_storage` to publish full-refresh Parquet snapshots in
S3, Cloudflare R2, Google Cloud Storage, or Azure Blob. This is an opt-in developer
preview. Without that block, the existing local POSIX store remains the default.

Object storage lets independently restarted workers share committed snapshots
without a shared filesystem. Each refresher still needs a private local staging
directory and enough disk for its in-progress Parquet file. Queries use remote
byte ranges through a parent-owned Go reader; they do not require a local copy of
the whole snapshot.

The source query, Arrow/Parquet type restrictions, freshness policy, and full
refresh cost described in [dataset acceleration](acceleration.md) still apply.
Changing the backend does not add CDC, incremental ingestion, or a lakehouse
table format.

## Configure one provider

[The complete example](../examples/object-storage.yml) refreshes the existing
sales CSV into an object snapshot. Its active configuration uses S3; commented
alternatives cover the other three providers. Replace the complete
`object_storage` block to change providers. Keep one YAML document and one active
provider per catalog.

All endpoints must be HTTPS origins without paths, credentials, or query strings.
`bucket` means an S3/R2/GCS bucket or an Azure container. `prefix` is required and
names an operator-controlled namespace. The configured tenant and dataset are
appended to that prefix; query callers cannot choose them.

S3 uses access-key credentials and a required signing region:

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

For temporary S3 credentials, add a separate `session_token_env` reference to
each identity that needs one. Omit it for static keys. Session tokens are
supported only with `provider: s3`.

R2 uses its account-specific S3 API origin. Replace the example account ID:

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

GCS uses HMAC interoperability access keys and its XML API endpoint. It has its
own object-generation preconditions; it is not treated as generic S3 CAS:

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

Azure uses separate SAS token references. The reader token must have exactly
`sp=r`; the parent validates this before opening query ranges. Supply the token
itself through the named environment variable, without an endpoint or connection
string. Replace the example account and container:

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

Credential values belong in the parent service's environment or secret manager,
never in YAML, command arguments, source control, or diagnostic output. All
references must use dedicated `KELVO_SOURCE_*` names. Reader and writer reference
names must be disjoint. Provision the reader for exact-object reads and metadata;
provision the publisher for reads and conditional creates/replacements in the
chosen prefix. Kelvo needs no object-list or object-delete permission. Azure
publishers need read/create/write capability for the same namespace. The parent
loads writer credentials only when it starts a refresh.

## Run a refresh and query

Object queries require the approved signed `httpfs` extension for the pinned
DuckDB version. Provision it before starting workers; queries do not download or
install extensions. The Azure DuckDB extension is not used by this path.

```sh
python3 scripts/provision_extensions.py artifacts/extensions --extensions httpfs
bin/kelvo accelerate refresh --config examples/object-storage.yml --dataset sales_fast
bin/kelvo accelerate status --config examples/object-storage.yml --dataset sales_fast
bin/kelvo accelerate verify --config examples/object-storage.yml --dataset sales_fast
bin/kelvo query --config examples/object-storage.yml --sources sales_fast \
  --sql 'SELECT region, SUM(amount) AS revenue FROM sales_fast GROUP BY region ORDER BY region' \
  --out revenue.arrow
```

Supervise `bin/kelvo accelerate watch --config examples/object-storage.yml` to
schedule refreshes. Query-only processes require the reader identity, while
refreshers require both identities and the original source credentials. Workers
for one tenant need matching catalogs and access to the same remote namespace;
their local staging directories can be independent.

## Publication and freshness

Objects use the following exact namespace:

```text
<prefix>/<tenant>/<dataset>/<32-hex-generation>.parquet
<prefix>/<tenant>/<dataset>/current.yaml
```

The bounded version-2 manifest contains the committed generation, its digest,
size, fingerprint, refresh time and provider revision, plus an optional writer
lease. It contains no arbitrary endpoint or object path. The writer lease and
committed pointer share this one conditional object, so a replacement writer
fences the old writer's publication.

| Provider | Conditional revision | Create-only condition |
| --- | --- | --- |
| S3 / R2 | ETag with `If-Match` | `If-None-Match: *` |
| GCS | Object generation | Generation match `0` |
| Azure Blob | ETag with `If-Match` | `If-None-Match: *` |

ETags are opaque revision tokens, not SHA-256 digests. Refreshes upload a new
immutable key, verify its metadata, then conditionally replace the committed
pointer. The previous committed snapshot remains available throughout refresh.
The lease lasts 60 seconds and renews every 15 seconds. Losing renewal cancels
source extraction and prevents successful stale-writer publication.

The service's response clock establishes lease times and snapshot age. The
parent advances observed age using local monotonic elapsed time, including when
deciding whether a scheduled refresh is due. Clock observations are process-local
and never enter manifests or query results. A failed refresh retains the last
committed generation; an expired `max_age` still makes that generation unavailable
for new queries.

If a publication response is lost, the writer attempts bounded read-back
reconciliation. A confirmed matching generation is successful. Otherwise it
reports an uncertain publication outcome, preserves both generations, and does
not clear a possibly published writer state. Inspect `status` and `verify` before
deciding whether another refresh is needed.

## Query credential and integrity boundaries

The parent acquires each selected generation and verifies its manifest and
object metadata. It then starts one HTTP listener on `127.0.0.1` for that query,
with an unpredictable 256-bit capability in each selected dataset URL. The child
receives only those URLs and object sizes. It receives no cloud endpoint, bucket,
key, reader credential reference, writer identity, or DuckDB cloud secret.

The bridge serves `HEAD` from the acquired metadata. It accepts only explicit
single closed byte ranges for `GET`; unknown paths, host changes, other methods,
request bodies, multiple ranges and unbounded GETs fail. It never passes child
headers to cloud storage or redirects the child. The Go provider client rejects
upstream redirects and requests the exact acquired revision. Every range must
match the committed object size, revision and SHA-256 metadata. A short or
oversized body ends as an incomplete response, including when a prefix has
already been sent.

Query cancellation or release stops the listener, cancels upstream requests,
closes bodies, and waits for cleanup. Treat capability URLs as temporary query
access: do not publish them or persist them in application logs. DuckDB's exact
source allowlist separately restricts queries to the selected capabilities.

These range checks do not recompute the full payload SHA-256 on every query.
`accelerate verify` streams and hashes the full pinned object when full integrity
verification is needed. Preserve generation immutability and the namespace's
access controls. A query must fail if a selected revision changes.

## Limits and retention

- A snapshot upload is one request, limited to **4 GiB**. Configure `max_bytes` at
  or below that limit. Multipart uploads and partitioned generations are absent.
- A query bridge allows **4 concurrent ranges**, each at most **32 MiB**, using a
  **32 KiB copy buffer per active request**. This bounds bridge copy buffers,
  not total process RSS, TLS buffers, DuckDB memory, or aggregate concurrency
  across queries. Larger or additional simultaneous ranges fail explicitly.
- HTTP request headers, header-read time and idle time are bounded. Range
  requests have a 60-second timeout as well as the query's cancellation context.
- HTTPFS full-download fallback is disabled. A broad scan can still request all
  relevant data through ranges; object storage does not make full scans free.
- Object snapshots can join other object/local snapshots. Combining them with
  live PostgreSQL/MySQL DuckDB extensions is currently rejected because those
  extensions require a broader external-access setting. Materialize those
  sources first when they need to join object snapshots.
- There is **no automatic remote garbage collection** and no distributed reader
  lease. Prune never lists or deletes remote objects. Retired generations and
  orphaned uploads remain until an operator removes them after all possible
  readers have stopped. Do not apply an age-only lifecycle deletion rule that can
  remove a committed or still-readable generation.
- Switching provider, bucket, prefix or dataset definition does not migrate
  existing objects. Refresh the new configuration before querying it.

## Validation scope

The lifecycle tests cover independent workers, conditional writer exclusion,
renewal loss, prior-generation preservation, ambiguous publication, strict
manifests, storage-clock skew, and retained remote generations. Bridge tests cover
capability boundaries, changed revisions, malformed/short/oversized responses,
bounded concurrency, streaming buffers and cancellation. Provider HTTP protocol
tests and disposable service fixtures are development evidence; they do not
establish production availability, cloud IAM correctness, or provider throughput.

On the development VM, a synthetic 32 MiB loopback range benchmark with a generated
upstream body measured 50.54 ms/op, 663.95 MB/s and 52,938 allocated B/op across
three iterations. This measures that fixture's generator, Go bridge and loopback
client together; it is not a cloud-storage or DuckDB throughput result. Reproduce
the focused checks on the designated build/test host:

```sh
GOMAXPROCS=2 go test -p 2 -race -count=1 ./internal/acceleration \
  -run 'TestObjectRanges|TestManagerObjectScheduling|TestObjectBackend|TestStore'
GOMAXPROCS=2 go test -p 2 ./internal/acceleration -run '^$' \
  -bench '^BenchmarkObjectRangesLoopbackStream$' -benchtime=3x -count=1
```

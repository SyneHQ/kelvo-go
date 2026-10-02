# Worker admission, metrics and maintenance

Cluster nodes can opt into aggregate resource reservations in their node YAML:

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

These are illustrative budgets, not a measured sizing recommendation. Values are
MiB. The pool's usable memory is `memory_mb - baseline_mb`. Each query reserves
its configured engine `memory_mb` plus `overhead_mb`, and its `max_temp_mb` scratch
budget. Each refresh additionally reserves `max_bytes` for snapshot staging.
Refresh reservations remain held through Parquet completion, publication and
pruning. The inner refresh executor does not acquire the same pool again.

Query and refresh work share one pool in each node process. Work waits within its
timeout when capacity is occupied. Oversize configured queries or datasets fail
node startup. Release follows execution cleanup, including cancellation. Existing
cluster queue and worker-slot limits still apply. Optional `query_reserve_*`
settings protect slots, memory and scratch from background refresh work. Refreshes
must fit the remaining background budget; otherwise node startup rejects their
configuration. Interactive queries may use all idle capacity, including space not
currently needed by refreshes. These are limits within one shared pool, not
separate allocations, preemption or a fairness guarantee. Existing interactive
queries can still fill the pool. Omitting the reserve fields preserves the
previous shared-capacity behavior; exports do not have their own class yet.
Separate node processes do not share this accounting. Operators must divide
host budgets between them, retain container limits, and account for persistent
snapshots separately from temporary staging.

Omitting `resources` preserves the previous slot-only behavior. Reservations do
not enforce RSS or disk quotas. DuckDB's setting does not cap Arrow/native/runtime
allocations; measure sufficient baseline and per-job overhead. Compressed results
do not reduce reservations. A shared pool cannot establish OOM safety without
appropriate budgets and real mixed-load capacity testing.

## Diagnostics

Worker `GET /metrics` exports fixed-cardinality Prometheus counters and histograms.
It requires the existing gateway mTLS identity, as does `GET /resources`, which
returns aggregate reservation capacity, usage, active/waiting counts and drain
state when resources are configured. Neither endpoint exposes SQL, credentials,
source names, query IDs or tenant IDs. Provision authorized internal collection;
do not expose worker diagnostics publicly.

Execution metrics distinguish query and refresh success, errors, cancellations
and capacity/drain rejection. Query queue histograms include measured node-resource
and source-quota admission wait; query duration excludes those intervals. Refresh
queue histograms measure outer node-resource admission only; source-quota wait in
the nested extraction remains part of refresh duration, alongside extraction and
publication. Neither histogram measures distributed JetStream queue latency.
Durations also include setup and delivery/cleanup. These are not separate source
execution and DuckDB computation timings. The startup SELECT
probe counts as a query execution. Metrics are process-local and reset on restart;
optional [lifecycle tracing](tracing.md) exports bounded, sampled local query/refresh
spans through OTLP/HTTP. It is disabled by default and excludes SQL, parameters,
source/tenant identities and raw errors. These spans do not establish distributed
trace continuity or full queue/stage timings. There is no durable query history. Snapshotting
metrics takes a short lock; rendering occurs after releasing it.

## Maintenance

`kelvo gateway` and `kelvo node` accept `--drain-timeout` (default `30s`). SIGTERM
and SIGINT stop admission, make readiness fail and retain existing result,
status/cancel and lease operations during the grace period. Grace expiry triggers
cancellation and a bounded shutdown. Gateway drain waits the configured grace
because other gateway replicas share durable handles. Workers wait for assigned
jobs and active refresh work. Assigned SQL is never automatically replayed.

Gateway `/health` and `/ready` bypass the ordinary HTTP request permit so overload
does not turn a healthy probe into HTTP 429. Gateway readiness remains a lifecycle
and broker-reconciliation signal. Worker readiness can additionally require
operator-selected datasets as described below. Neither proves source reachability
or spare query capacity. Worker probes require gateway mTLS.


## Dataset diagnostics and required readiness

With acceleration configured, worker `GET /datasets` uses the existing gateway
mTLS identity and returns bounded metadata for at most 64 configured datasets.
States are `ready`, `missing`, `stale`, `configuration_changed` or `unavailable`.
Records expose dataset ID, generation, original refresh time, age and optional
schema hash. They exclude filesystem paths, object URLs, fingerprints, source
configuration and backend error text.

Opt into dataset readiness in the **node** YAML:

```yaml
required_datasets:
  - orders_daily
```

Every required ID must exist in the node's acceleration catalog. Worker `/ready`
returns unavailable if any required dataset is missing, stale, mismatched with
the current catalog fingerprint or unavailable. Optional dataset failures do not
gate readiness, and the gateway's own `/ready` does not inherit this check.
Without required IDs, dataset state does not gate worker readiness.

Metadata reads are coalesced per dataset and cached for two seconds. Probes have
a five-second deadline and at most four concurrent backend reads. When required
datasets exist, two slots are reserved for them and two for optional datasets;
optional diagnostic traffic cannot occupy the required slots. Age continues to
advance while metadata is cached. No background polling, source SQL or automatic
refresh is triggered by these endpoints.

**Readiness checks metadata, not payload integrity.** A `ready` record does not
prove a full checksum, decoded Parquet schema, source connectivity or sufficient
execution capacity. Use explicit verification and recovery checks for payload
integrity; a copied schema hash in diagnostics is not fresh schema validation.

## Remote generation inventory and restore

The existing operator CLI now supports both local and remote backends:

```sh
kelvo accelerate inventory --config kelvo.yml --dataset orders_daily
kelvo accelerate restore --config kelvo.yml --dataset orders_daily \
  --generation <retained-generation> \
  --expected-generation <current-generation>
```

Use the source/acceleration catalog as `--config`, not the cluster node file.
Restore derives the authorization/configuration fingerprint from that catalog;
it never accepts a caller-supplied fingerprint. Both target and current payloads
must pass full streamed SHA-256 verification and exact Arrow schema compatibility.
Object schemas use bounded range reads; datasets are not loaded entirely into
memory. Custom object clients without range reads explicitly reject recovery.

Remote inventory is a **retained manifest catalog**, not a storage listing. It
contains the current generation and at most 16 historical entries, with fewer
entries if needed to keep the manifest below 64 KiB and reserve room for writer
leases. Output identifies this scope and signals catalog truncation. Publication
retains the previous current generation atomically. Legacy historical objects
without catalog entries cannot be discovered or restored through this API.
Evicting metadata never deletes an object; storage reclamation and remote reader
GC remain separate, unimplemented operations.

Restore uses the same renewable writer lease and conditional publication as
refresh, and rejects a changed `--expected-generation`. It preserves the original
refresh timestamp, checksum and object version: restored data can remain stale.
Immutable reader versions remain usable. A corrupt current payload cannot be
repaired through this restore path because current-schema verification is
required; use a separately validated backup or source recovery procedure.
Inventory marks unreadable or corrupt entries unverified rather than treating
them as usable generations. This is not a complete backup/disaster-recovery system
or a measured RTO/RPO guarantee.

### Remote manifest upgrade

New code reads legacy remote manifest **v2** and current **v3**. Every new writer
action emits v3, including the first lease claim or renewal, even if extraction
later fails or is aborted. Local manifest versions are unchanged. A legacy
manifest begins with no historical catalog; subsequent publication can retain its
previous current generation.

Coordinate upgrades of all readers and writers before allowing a new writer to
act. Older binaries reject v3; mixed-version operation and binary rollback after
that first writer action are unsupported. Drain old workers and preserve a
verified operational recovery plan before crossing this boundary. Do not edit a
version number to downgrade a manifest containing newer metadata.

## Validation boundaries

Validated on the dedicated Azure Linux VM on 2 October 2026 against runtime
commit `6b49c17`: full `go test -tags duckdb_arrow ./...`, focused race tests for
admission/telemetry/worker/cluster/CLI, `go vet`, CLI and sandbox builds, 22 fixture
tests, real NATS store integration, [10 cluster acceptance checks](evidence/production-foundation-cluster.json)
and [14 standalone/cluster acceleration checks](evidence/production-foundation-acceleration.json)
passed. The new fixture enables shared resource budgets on every worker. The
final telemetry-only fix received another CLI race-test pass and rebuild before
acceptance. No local builds were run. Default VM Python initially lacked PyArrow;
the scripts passed using the existing analytics virtual environment.

The cluster record contains a single loopback transfer timing for reproducibility;
it is not a capacity or performance comparison. CI includes race coverage for the
new packages. A full production release still
requires mixed export/join/refresh soaks, actual rolling-process fault tests,
RSS/cgroup/scratch measurements, micro-VM remeasurement and telemetry overhead
benchmarks. No new throughput or OOM-safety claim follows from unit tests.


## Shared source quotas

Optional `policy.source_quotas` maps configured source IDs to maximum concurrent
Kelvo operations across a tenant's nodes. Every copy of the policy must match.
These limits count whole admitted operations; one federated operation may open
multiple source scans, so they are not a database connection limit.

```yaml
policy:
  # Include the other required tenant scheduling fields.
  source_quotas:
    warehouse: 2
    orders: 4
```

The parent acquires selected source slots in a stable order, rolls back partial
acquisitions on contention, and renews ownership while work runs. Admission wait
is included in the execution timeout. Lost renewal cancels execution; release
follows subprocess cleanup. Accelerated reads do not consume their original
source's slots. Broker TTL reclaims crashed owners independently of host clocks.
A distributed lease cannot fence SQL already running at a remote database:
remote cancellation is still best effort. Read-only source permissions and
source-side workload limits remain required.

Initialize `KV_KELVO_SOURCE_QUOTAS` with `cluster-init` when enabling quotas.
Account stream capacity must allow six streams when acceleration status is also
enabled. Workers additionally need publish permissions for
`$JS.API.STREAM.MSG.GET.KV_KELVO_SOURCE_QUOTAS` and
`$KV.KELVO_SOURCE_QUOTAS.>` in their own tenant account. Source quotas are limited
to 64 configured source IDs and 64 slots each, with bounded KV storage. Policies
are immutable during normal operation; coordinate drain and reprovisioning when
changing them.

## File-based source credential rotation

Node YAML may map existing source environment references to private local files:

```yaml
secrets:
  ttl: 30s
  files:
    KELVO_WAREHOUSE_PASSWORD: /run/kelvo-secrets/warehouse-password
```

The trusted parent resolves only references for selected sources and shares the
provider with refresh executors. A newly started query receives current values
within the configured cache TTL. Unmapped references use the process environment;
a configured file failure never falls back to an older environment value. Source
catalogs and child input retain reference names, not secret contents or provider
paths. Values enter only the selected child's environment.

Use private regular files owned by the service UID in trusted directories, and
rotate by atomically replacing a file. Symlinks, hardlinks, unsafe permissions,
nonregular files, embedded NULs and values over 16 KiB are rejected. Contents are
exact: use `printf`, not a command adding an unwanted newline. At most 128 files
and 2 MiB of retained bytes are supported; TTL is at most five minutes and zero
disables retention. Close wipes retained byte buffers on a best-effort basis;
Go strings and child environments cannot be guaranteed erased from memory.

This rotates source-driver credentials for new query/refresh processes. It does
not rotate credentials in existing queries, parent object-storage clients, NATS
connections, gateway API tokens or TLS certificates, and is not a cloud secret
manager integration.


## Optional execution history

```yaml
history:
  max_entries: 256
  ttl: 1h
```

Without this block no history ring is allocated. `GET /history` on the worker
requires gateway mTLS and returns at most 1,024 retained execution records with
at most 24 hours of retention. Entries contain generated query IDs, fixed
outcome/category, timestamps and duration; no query text, parameters, source
identities or result previews. Expiry is enforced on reads/appends, without a
background sweeper. Records disappear on restart.

One record is captured after a node execution/transfer and its result-ready update.
Node success does not prove the gateway committed success or the client received
the complete result. This is bounded operational history, not an audit trail,
queued-job history, trace export or durable replay catalog.


## Dataset safety validation

Runtime commit `ad25a3c` was validated on the dedicated Azure Linux VM with the
full ordinary and pinned DuckDB bridge suites, focused race checks (including
secret rotation, source quotas, history and diagnostics), and `cgocheck2` plus
race checks for the bridge/federation/engine. Real NATS tests passed durable
failure suppression, sequence-fenced reset, shared source capacity and recovery
after broker TTL. The [16-check acceleration acceptance](evidence/dataset-safety-acceptance.json)
passed typed round-trips, real schema drift rejection, restore freshness and stale
precondition rejection, scheduled cluster refresh and tenant isolation.

Two integration failures were found and fixed: the refresh status test requested
a pull timeout above the consumer limit; quota KV values initially allowed too
little space for NATS CAS headers. The corrected tests were rerun successfully.
These are correctness checks, not sustained-load, provider-wide rotation, memory
footprint or recovery-time benchmarks.

## Current development validation

Focused tracing tests and credential-rotation tests exercise bounded export,
privacy, cancellation, atomic file replacement and selected parent-to-child
credential forwarding. Protocol fixtures exercise remote recovery and failure
boundaries. These are development correctness gates; provider protocol fixtures
are not live-provider acceptance, and passing them does not establish throughput,
export overhead, sustained concurrency or a production capacity guarantee. Keep
committed acceptance evidence and its runtime revision separate from these checks.


## Readiness and recovery milestone validation

Runtime commit `c303b5f` was validated on the dedicated Azure Linux test VM with
Go 1.26.8. The full ordinary and pinned DuckDB bridge test suites passed, along
with focused race checks for acceleration, admission, cluster, worker, tracing
and CLI packages. Bridge/federation/engine checks passed with both the race
detector and `GOEXPERIMENT=cgocheck2`. `go vet`, the Kelvo binary build and the
strict C sandbox launcher build also passed. All compilation ran on the VM.

Real NATS store tests passed, followed by the
[10-check cluster acceptance](evidence/production-readiness-cluster.json) and
[19-check acceleration acceptance](evidence/production-readiness-acceptance.json).
The latter checks missing-required readiness without breaking liveness, recovery
after refresh, optional-missing independence and sanitized dataset diagnostics,
in addition to typed results, schema safety, restore, scheduled refresh and tenant
isolation. The harness restores its temporary node/catalog configuration.

Review and tests found two defects before this milestone was committed: a
same-generation remote restore could misclassify an ambiguous no-op write and
leave a writer lease behind; the tracing SDK merged environment resource
attributes despite explicit resource configuration. Regression tests now cover
no-op lease cleanup and detached resource sanitization before the export queue.
Collector redirect, header/body size and queue/shutdown bounds are also tested.

These checks do not validate a live cloud-object provider, quantify trace export
overhead, establish sustained tenant capacity or measure backup RTO/RPO. Remote
rollback remains protocol-fixture tested; it requires intact current and target
payloads and is not a repair path for corrupted current data. No new micro-VM or
throughput claims follow from this milestone. Changes and test binaries remain
separate from any production deployment.

# Worker admission, metrics and maintenance

Cluster nodes can opt into aggregate resource reservations in their node YAML:

```yaml
resources:
  max_concurrent: 2
  memory_mb: 4096
  baseline_mb: 512
  overhead_mb: 256
  scratch_mb: 8192
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
cluster queue and worker-slot limits still apply. There is no fairness guarantee
or interactive/background class reservation yet; one refresh may occupy the last
slot. Separate node processes do not share this accounting. Operators must divide
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
and capacity/drain rejection. Queue histograms measure local resource-admission
wait, not distributed JetStream queue latency. Duration excludes that admission
wait and includes setup, execution and delivery (refresh includes publication).
These are not separate source and DuckDB timing measurements. The startup SELECT
probe counts as a query execution. Metrics are process-local and reset on restart;
there is no trace export or durable query history in this release. Snapshotting
metrics takes a short lock; rendering occurs after releasing it.

## Maintenance

`kelvo gateway` and `kelvo node` accept `--drain-timeout` (default `30s`). SIGTERM
and SIGINT stop admission, make readiness fail and retain existing result,
status/cancel and lease operations during the grace period. Grace expiry triggers
cancellation and a bounded shutdown. Gateway drain waits the configured grace
because other gateway replicas share durable handles. Workers wait for assigned
jobs and active refresh work. Assigned SQL is never automatically replayed.

Gateway `/health` and `/ready` bypass the ordinary HTTP request permit so overload
does not turn a healthy probe into HTTP 429. Readiness remains a lifecycle and
broker-reconciliation signal, not proof of source reachability, fresh datasets or
spare query capacity. Worker probes require gateway mTLS.

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

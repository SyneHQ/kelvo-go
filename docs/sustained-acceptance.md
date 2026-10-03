# Sustained mixed-load acceptance

The [sustained runner](../scripts/sustained_acceptance.py) exercises a bounded,
two-tenant cluster for at least two hours. A passing smoke run cannot close the
[sustained-load gate](https://github.com/SyneHQ/kelvo-go/issues/7). Results apply
only to the recorded binary, source inventories, topology and resource budget.

## Workload and limits

The runner provisions two gateways, two workers and three NATS brokers in one
private Linux systemd service. Each tenant has one million deterministic Parquet
rows with exact integer, NULL and tenant-marker checks. Interactive aggregates
run alongside scheduled full refreshes. Periodic slow readers verify complete
Arrow framing, types and values under delivery backpressure; explicit running
query cancellations must reject result replay.

Normal clients briefly quiesce for saturation, cancellation and process-loss
assertions. Each process-loss gate creates its own two running and two queued
queries before killing an owned gateway, worker or broker. This checks recovery
under admitted work; it does not establish uninterrupted normal-client traffic
during every injected fault or exactly-once source execution.

| Boundary | Budget |
| --- | --- |
| Entire owned service | 200% CPU, 6 GiB RAM, no swap, 512 tasks |
| Worker admission | Two slots, 1 GiB accounted memory, 512 MiB scratch |
| Query engine | 128 MiB, one thread, 20-second timeout |
| Native child process tree | 192 MiB, 64 tasks |
| Parent Arrow allowance per query | 160 MiB |
| Result | 64 MiB |
| Generated artifacts | 8 GiB guard |

The private NATS fixture binds loopback addresses inside the service's isolated
network namespace, using ports 14222–14224, 14440–14445, 16222–16224 and
18222–18224. The runner owns only its unique systemd unit. Existing databases
and unrelated services are outside its scope.

DuckDB materializes execution before Arrow delivery. The slow-reader case filters
to 16,384 rows before producing its 256-byte payload column. That makes it a
delivery-backpressure workload within the fixed child cap; it is not evidence
that a LIMIT bounds arbitrary wide projections, sorts or joins.

## Run on the designated Linux VM

Build Kelvo and its sandbox on the VM, freeze the source tree, and capture a
clean build-source inventory using `containment_acceptance.source_manifest`.
Select the exact commit independently with `--expected-revision`; use matching
clean source and binaries. Place the manifest at
`artifacts/sustained-build-source.json`. Build and execution inventories must be
identical, including the acceptance scripts. Freeze the harness before compiling.

Use a non-root account with authority to start a delegated transient service.
Python needs PyArrow and PyYAML; the VM also needs systemd, cgroup v2, the sandbox
kernel prerequisites. Stage the pinned official NATS archive before execution
and supply `--nats-archive`: the measured service has no external network access.
The fixture verifies the archive's pinned checksum. Build and dependency
acquisition are outside measured execution.

```sh
python3 -m unittest discover -s scripts -p test_sustained_acceptance.py -v
KELVO_ACCEPTANCE_REV=$(git rev-parse HEAD)
KELVO_NATS_ARCHIVE=/absolute/path/nats-server-v2.15.0-linux-amd64.tar.gz
python3 scripts/sustained_acceptance.py \
  --expected-revision "$KELVO_ACCEPTANCE_REV" --nats-archive "$KELVO_NATS_ARCHIVE" \
  --mode smoke --duration 600 --output artifacts/sustained-smoke.json
python3 scripts/sustained_acceptance.py \
  --expected-revision "$KELVO_ACCEPTANCE_REV" --nats-archive "$KELVO_NATS_ARCHIVE" \
  --mode sustained --duration 7200 --smoke-report artifacts/sustained-smoke.json \
  --output artifacts/sustained-7200.json
```

Use `--binary`, `--sandbox` and `--go` for explicit VM toolchain paths. Every
output path must be new. Keep the outer controller alive: it owns final unit
shutdown and report reconciliation. Sustained mode refuses to launch without a
fully reconciled passing smoke from the identical harness source and binaries.
Its initial output identifies its exact unit,
control PID, private artifact directory and requested duration. Progress and
checkpoint files flush approximately once per minute. Do not edit the frozen VM
source while a run is active.

## Acceptance and interpretation

All ten gates must pass: startup, tenant isolation, saturation, gateway loss,
worker loss, broker loss, mixed load, slow readers, cancellations and cleanup.
Each complete minute must show query and refresh progress for both tenants.
Fault recovery counts status/transport observations. Only status GETs on the
original queued handle tolerate 429/503, within 25 seconds; SQL is not resubmitted
and results are claimed once. Missing, denied, terminal or late-positive status
responses cannot satisfy recovery.
Fenced workers may restart through the same [bounded supervisor](process-loss-acceptance.md).
It preserves the original handles, deadlines and configuration; supervision observations remain in the report.
Queue/dispatch, first-byte, query, slow-delivery and cancellation histograms must
contain samples. Overflow bins remain explicit; a missing tail is not zero.

Process-tree RSS sampling covers owned application parents and observed
descendants. Dedicated service cgroup accounting also includes the brokers and
Python controller. RSS may double-count shared pages and miss short peaks;
charged cgroup memory has a different scope. The report retains CPU counters,
OOM events, scratch peaks, query/refresh overlap and node-local queue histograms
across worker restarts. Dispatch observation uses polling and is an upper bound,
not precise distributed queue attribution.

Missing gates or samples, source mutation, stalled progress, OOM, uncertain
cleanup and skipped fault assertions fail reconciliation. Cleanup requires no
forced application kills, live descendants, managed scratch directories or
containment custody records; every owned broker must stop and the exact service
and cgroup must disappear. The final cgroup sample occurs after application and
broker shutdown.

Retain failed reports alongside successful ones. Initial smoke failures exposed
a restricted fixture serializer, an over-budget wide projection, handle eviction
during a cancellation assertion and an incorrect harness liveness path. These
are separate observations; a later passing run does not erase them.

This is synthetic single-host lifecycle and resource acceptance. It does not
certify WAN capacity, source-provider behavior, multihost availability, mixed
binary upgrades or deployment-wide production readiness. See the separate
[process-loss gates](process-loss-acceptance.md), [containment acceptance](process-containment.md)
and [Oracle profile](oracle-micro-capacity.md).

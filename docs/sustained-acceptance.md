# Sustained mixed-load acceptance

The [sustained runner](../scripts/sustained_acceptance.py) exercises a bounded,
two-tenant cluster for at least two hours. A passing smoke run cannot close the
[sustained-load gate](https://github.com/SyneHQ/kelvo-go/issues/7). Results apply
only to the recorded binary, source inventories, topology and resource budget.

The current runner adds real cross-source file joins and small durable exports
to the original workload. These additions require a new matching smoke and
two-hour report; the historical results below do not qualify them. The paired
DBAPI/database lane is separate from this generic Kelvo campaign.

## Recorded two-hour run

The [two-hour campaign on `0544d5f`](evidence/sustained-7200-0544d5f.json) passed strict reconciliation and all ten gates. Source, binaries and the prerequisite smoke matched. [Independent cleanup](evidence/sustained-7200-cleanup-0544d5f.json) confirmed exit 0, service/cgroup removal and the unchanged raw report.

| Observation | Recorded result |
| --- | --- |
| Mixed workload / total controller time | 7,200.162 / 7,214.150 seconds |
| Completed work | 19,545 queries, 960 refreshes, 240 slow readers, 239 cancellations |
| Fault gates | Saturation, gateway loss, worker loss and broker loss passed |
| Process sampling | 140,590 samples; zero hard read errors or diagnostic overflow |
| Charged memory / sampled process RSS peak | 910,630,912 / 752,504,832 bytes |
| Client/resource errors and OOMs | Zero |
| Cleanup | No forced application kills, live descendants, scratch directories or containment records; owned brokers stopped |

This is paced, two-tenant lifecycle evidence under a 2 CPU / 6 GiB cap. [Six component gates ran during the campaign](evidence/sustained-7200-overlap-0544d5f.json); two failed, and the first OTLP overlap observation arrived after that component exited. Later shutdown, tracing and child-timing changes have separate component evidence. This run does not establish their sustained capacity, joins/exports coverage, WAN performance, provider compatibility or deployment sizing.

## Recorded smoke

The [600-second smoke on `0544d5f`](evidence/sustained-smoke-0544d5f.json) passed
all ten gates with unchanged source and binaries. Its
[isolated build](evidence/sustained-build-0544d5f.json) passed 59 Python controls,
nine focused Go race tests (39 pass events), vet and both binary builds.

| Observation | Recorded result |
| --- | --- |
| Mixed workload interval | 600.113 seconds, two paced tenant clients |
| Completed work | 1,594 queries, 80 refreshes, 20 slow readers, 20 cancellations |
| Fault gates | Saturation, gateway loss, worker loss and broker loss passed |
| Process sampling | 11,754 samples; zero hard read errors or diagnostic overflow |
| Charged memory peak | 759,791,616 bytes under a 200% CPU / 6 GiB service cap |
| Client/resource errors and OOMs | Zero |
| Cleanup | No forced application kills, live descendants, scratch directories or containment records; owned brokers stopped, service and cgroup independently confirmed removed |

This smoke is lifecycle evidence, not maximum throughput or deployment sizing.
The separate matching two-hour result is recorded above; a smoke alone cannot
satisfy that gate.

## Earlier evidence

| Candidate | Result retained |
| --- | --- |
| [`b47ce87` smoke](evidence/sustained-smoke-b47ce87.json) | All ten gates passed; 1,571 queries and 11,761 process samples. Its [build receipt](evidence/sustained-build-b47ce87.json) remains available. |
| [`cbc3e25` smoke](evidence/sustained-smoke-cbc3e25.json) | All ten gates passed; 1,606 queries and 11,770 process samples. |
| [`40965ef` smoke](evidence/sustained-smoke-failed-40965ef.json) | Startup failed before workload; underlying metadata cause remains unknown. |
| [`0da7523` smoke](evidence/sustained-smoke-failed-0da7523.json) | Workload gates passed; strict acceptance rejected process sampling errors. |
| [`b09f2da` two-hour run](evidence/lease-recovery-sustained-failed-b09f2da.json) | Workload gates passed; strict acceptance rejected process sampling errors. |

`40965ef` [built and passed 56 controls](evidence/sustained-build-40965ef.json),
but `cluster-init` returned `cluster metadata unavailable`. No queries or
refreshes ran. [Diagnostics](evidence/sustained-startup-diagnostics-40965ef.json)
did not establish the failing operation. Internal cleanup then hit a worker
directory that startup never created; independent outer service/cgroup cleanup
passed. Later successes do not explain or erase this failure.

The [original TLS CI failure](evidence/tls-ci-37150306039-failure.json) is also
retained. [Corrected tests](evidence/tls-reader-deadlines-2bc7dd7.json) wait for
the pending read's own deadline; expiry of the old snapshot is insufficient.
All 41 selected tests / 97 pass events and vet passed. Runtime deadlines stayed
unchanged; the historical scheduler timing was not captured.

## Workload and limits

The runner provisions two gateways, two workers and three NATS brokers in one
private Linux systemd service. Each tenant has one million deterministic Parquet
rows with exact integer, NULL and tenant-marker checks. Interactive aggregates
alternate with joins against a separate ten-row CSV dimension. The join excludes
one dimension bucket and applies different per-tenant integer multipliers, so an
omitted join, ignored filter or cross-tenant dimension changes the exact result.
Both file adapters and native DuckDB execute; this is not a live database-provider
test. These queries run alongside scheduled full refreshes. Periodic slow readers verify complete
Arrow framing, types and values under delivery backpressure; explicit running
query cancellations must reject result replay.

Each tenant also has a paced export client. It submits the same file join through
`POST /v1/exports`, observes the original handle becoming ready, validates the
manifest, and verifies the one-part Arrow result twice through different gateways.
Both complete deliveries must match the manifest digest, exact integers/types and
each other's bytes. Foreign-tenant status, manifest, part and cancel requests must
return 404. Withdrawal must return a cancelled handle and subsequent manifest
access must return 409. New join/export submissions are never retried; an
ambiguous response fails the campaign and remains recorded.

Exports contain nine aggregate rows. This exercises durable export lifecycle and
repeat reads under mixed traffic; it does not establish wide-result export
throughput. The fixture enables explicit principal keys and adds only the export
broker namespace permissions to its existing tenant roles. Other runners keep
their existing fixture permissions by default.

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
| Export fill | One per tenant at a time, paced 20 seconds after each completed cycle |
| Export result | Nine rows, one part, 1 MiB encoded and decoded limits |
| Export admission class per worker | One background slot, 352 MiB accounted memory, 128 MiB scratch; within the worker's existing total |
| Export retention per worker | Eight entries, 32 MiB reserved storage, 60-second TTL |
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

All twelve gates must pass: startup, tenant isolation, saturation, gateway loss,
worker loss, broker loss, mixed load, file joins, durable exports, slow readers,
cancellations and cleanup. Each complete minute must show aggregate-query, join,
export and refresh progress for both tenants. The new HTTP counters reconcile
every successful join/export submission with one certified result cycle, two
export downloads, one withdrawal and the expected isolation refusals. Extra,
failed or lost responses cannot be discarded from a passing report. Per-class
latency histograms must match the workload counters.

After the measured interval, normal worker cleanup has up to 75 seconds to remove
all export entries and partial-publication markers. The runner does not delete
retained data to obtain a passing cleanup result. This drain stays inside the
existing `duration + 300` service watchdog, and resource observation continues
through it. A failed drain fails acceptance even when outer service cleanup succeeds.
Fault recovery counts status/transport observations. Only status GETs on the
original queued handle tolerate 429/503, within 25 seconds; SQL is not resubmitted
and results are claimed once. Missing, denied, terminal or late-positive status
responses cannot satisfy recovery.
Fenced workers may restart through the same [bounded supervisor](process-loss-acceptance.md).
It preserves the original handles, deadlines and configuration; supervision observations remain in the report.
Queue/dispatch, first-byte, query, slow-delivery and cancellation histograms must
contain samples. Overflow bins remain explicit; a missing tail is not zero.

Process-tree RSS sampling covers owned application parents and observed
descendants. The observer reads PID, start time, process state and RSS pages from
one `/proc/PID/stat` record, converts pages with the recorded system page size,
and rejects malformed or missing fields. It does not read `/proc/PID/status`
for a second RSS observation. Only explicit exited states and `ENOENT`/`ESRCH`
are treated as process disappearance; permission and other I/O errors still fail.

Stat RSS is an approximate diagnostic: Linux uses inexpensive per-CPU resident
counters. On the tested kernel, status `VmRSS` sums those counters more precisely,
so the two series should not be compared directly. RSS may double-count shared
pages and miss short peaks. Dedicated service `memory.peak` is the charged-memory
peak for the whole fixture, including brokers and the Python controller; it has
a different scope. The report retains CPU counters,
OOM events, scratch peaks, query/refresh overlap and node-local queue histograms
across worker restarts. Dispatch observation uses polling and is an upper bound,
not precise distributed queue attribution.

Missing gates or samples, source mutation, stalled progress, OOM, uncertain
cleanup and skipped fault assertions fail reconciliation. Cleanup requires no
forced application kills, live descendants, managed scratch directories or
containment custody records; every owned broker must stop and the exact service
and cgroup must disappear. The final cgroup sample occurs after application and
broker shutdown.

The outer controller uses a fresh `kelvo-sustained-<nonce>` unit and a separate
unpredictable Description marker. The live service verifies its exact unit,
marker, user, working directory and cgroup before setup. Cleanup checks those
same ownership fields before stopping the unit; it never deletes cgroups or
adopts another runner's unit namespace. Lookup errors, failed stops, a same-name
replacement or retained cgroup fail acceptance.

Retain failed reports alongside successful ones. Initial smoke failures exposed
a restricted fixture serializer, an over-budget wide projection, handle eviction
during a cancellation assertion and an incorrect harness liveness path. These
are separate observations; a later passing run does not erase them.

The [600-second smoke on `0da7523`](evidence/sustained-smoke-failed-0da7523.json)
also remains failed. All ten workload and cleanup gates passed, but strict
reconciliation rejected two `status/malformed` observations. Their cause was
not captured precisely enough to infer retrospectively. The source and binaries
were unchanged and the owned service and cgroup were removed. Its raw report
SHA-256 is `e1a0b43f46e96fb1adbd6869bb5d203986dad63bc8f5336379643842181247c8`.

The current observer removes a documented exit-time hazard: Linux may omit
memory fields from status after a task loses its memory descriptor, whereas stat
always emits its RSS field, using numeric zero when that descriptor is absent.
This explains the design change, not the cause of the historical observations.
The field definitions and precision caveats are documented in the
[Linux stat manual](https://man7.org/linux/man-pages/man5/proc_pid_stat.5.html),
[proc filesystem guide](https://docs.kernel.org/filesystems/proc.html), and the
tested kernel's upstream
[stat/status implementation](https://github.com/gregkh/linux/blob/v6.12.95/fs/proc/array.c),
[status memory counters](https://github.com/gregkh/linux/blob/v6.12.95/fs/proc/task_mmu.c)
and [RSS counter helpers](https://github.com/gregkh/linux/blob/v6.12.95/include/linux/mm.h).

This is synthetic single-host lifecycle and resource acceptance. It does not
certify WAN capacity, source-provider behavior, multihost availability, mixed
binary upgrades or deployment-wide production readiness. See the separate
[process-loss gates](process-loss-acceptance.md), [containment acceptance](process-containment.md)
and [Oracle profile](oracle-micro-capacity.md).

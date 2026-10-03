# Kelvo worker on Oracle E2.1.Micro

This measures one Kelvo **worker** on the micro VM. The Azure gateway, NATS,
source database and every SSH tunnel are outside its accounting. It does not
establish that a whole cluster or standalone deployment fits on the micro VM.

## Latest pinned campaign

The [final-runtime campaign](evidence/node-capacity-d144a43-r2.json) passed
**130/130 workload queries** across five metrics-disabled/enabled pairs, plus a
separate **3/3 preflight**. Each profile ran three serial queries and ten
simultaneous queued submissions through **one execution slot**. Exact Arrow
types, values, NULLs, framing/EOS and durable success passed for every result.
The workload returned **120,000,080 repeated rows** from one million source rows
and 265 zones; these are not distinct source records. The million-row projection
has three columns (`int64`, `int32`, `int64`). Source and result LZ4 were enabled;
wider rows and different compression change memory and transfer costs.

| Full-delivery workload | Metrics disabled median / p95 | Metrics enabled median / p95 |
| --- | ---: | ---: |
| Native million-row projection | 5.885 / 6.124 s | 6.098 / 6.541 s |
| Federated million-row projection | 8.790 / 9.820 s | 9.200 / 10.146 s |
| Federated CTE/zone join, eight result rows | 7.223 / 7.438 s | 7.524 / 7.645 s |
| Queued million-row operation, including assignment wait | 38.341 / 71.406 s | 38.718 / 73.109 s |

Median full-delivery rates were about **170k / 164k rows/s native**
and **114k / 109k rows/s federated**, for disabled/enabled metrics.

The first three rows contain five samples per mode; queued operations contain
50 per mode. p95 is empirical nearest rank, not a confidence bound. Timings include
SQL, queueing, dispatch, TLS and SSH transport; Arrow decoding is outside that
interval. Native SQL runs on Azure; federation runs on Oracle. This is not a
direct analytical-library comparison.

Median per-profile peaks were **230.7 / 230.5 MiB** for combined node/worker RSS
and **122.0 / 123.5 MiB** for charged cgroup memory, for disabled/enabled metrics. The
**640 MiB** worker cap includes the observer, with no swap and one execution
permit. Every profile had zero OOMs and complete process/scratch/containment
cleanup. Maximum active/shutdown sampling gap was **0.292 seconds**, within the
unchanged two-second gate. Post-run hashing is outside this interval; startup
setup remains in whole-cgroup counters. Executables used verified hardlinks; warm
shared file-cache pages may be charged outside a later profile's cgroup, while
combined RSS can count shared pages twice. The changed observer ordering also
changes the sampling boundary. Lower charged peaks than older trials do not
establish application-memory savings or a new minimum RAM requirement.

The [916-file source manifest](evidence/node-capacity-d144a43-source.json) pins
runtime `d144a437b824fef1cd908303fa220d1bdd581401`; the separately reviewed observer
is `9cad90497daa23b23543a8dbbea622449e313ecf`. Full binary/harness checksums and all
profile receipts are in the campaign evidence. No earlier run was pooled into it.

The temporary Azure control fixture was capped at **1 CPU / 1 GiB**; the preserved
ClickHouse source kept its separate **1.5 CPU / 4 GiB** cap. Independent checks
verified source-account/key removal, stopped gateway/brokers/tunnels and absent
worker cgroups, with source tables intact. Controller cleanup finished at
**18:19:11 UTC**; subsequent independent checks completed before the unchanged
**18:43:57 UTC** expiry. The fixture's explicit SIGTERM exit
143 is retained, followed by proof that its stopped unit was unloaded.

Coordinated Azure builds/tests were held during measurements; read-only staging
or hashing could occur. Caches were warm and uncontrolled, and Oracle CPU burst
availability and host steal varied. These results do not prove causal metrics
savings, minimum production RAM, whole-cluster fit, an SLA or later-runtime
readiness.

## Earlier attempts

| Pinned attempt | Outcome | Retained evidence |
| --- | --- | --- |
| `6749369`, original observer | Five pairs; 130/130 queries | [Baseline](evidence/node-capacity-6749369.json) |
| `b09f2da`, bounded campaign | Three complete pairs plus one profile; 91/91 queries; deadline guard stopped further launch | [Partial campaign](evidence/node-capacity-b09f2da.json) |
| `b09f2da`, two fresh preflights | 3/3 queries each; sampling gaps 2.635/2.986 s failed the two-second gate; no pairs launched | [Observer refusals](evidence/node-capacity-observer-gap-refusals.json) |
| `d144a43`, first corrected-observer trial | Two pairs; 52/52 queries; local ENOSPC stopped the next lease observation before launch; both hosts cleaned independently | [Controller refusal](evidence/node-capacity-d144a43-enospc.json) |

For historical context, the original `6749369` baseline produced these full-delivery
times. Its runtime and observer differ from the latest campaign; this table does
not establish a regression or speedup, and its profiles cannot be combined with
newer ones.

| Historical baseline workload | Metrics disabled median / p95 | Metrics enabled median / p95 |
| --- | ---: | ---: |
| Native million-row projection | 6.583 / 11.266 s | 5.925 / 6.042 s |
| Federated million-row projection | 9.348 / 9.923 s | 8.793 / 9.102 s |
| Federated CTE/zone join | 7.337 / 7.513 s | 7.097 / 7.534 s |
| Queued million-row operation | 39.006 / 73.383 s | 38.473 / 72.492 s |

Its median combined RSS peaks were 230.9/229.8 MiB; cgroup peaks were
183.4/181.1 MiB. The [baseline manifest](evidence/node-capacity-6749369-source.json)
and receipt retain full scope, identity and cleanup details.

## Reproduce the profile

Use the [worker observer](../scripts/node_capacity_worker.py) together with the
[exact-result client](../scripts/node_capacity_client.py). A resource report
alone does not prove that a query returned successfully. Retain every failed
profile alongside later results, including startup and controller failures.

The constrained profile uses one worker execution permit, a 640 MiB service
limit, no swap, 128 tasks and `GOMAXPROCS=1`. Query policy uses 128 MiB, one
DuckDB thread, a 60-second deadline, one million result rows and a 32 MiB
decoded/encoded result limit. The native child has a 192 MiB containment limit.
These are explicit test settings, not minimum production requirements.

## Prepare isolated inputs

Build on the designated Linux host and record the exact commit, source manifest,
build tags and binary checksums. Keep the source database, gateway and NATS
brokers outside the worker's cgroup. Use separate source credentials with SELECT
access only to the test tables. Preserve source data and unrelated services.

The worker directory contains `bin/kelvo`, `bin/kelvo-landlock`, `catalog.yml`,
`node-template.yml`, private TLS material and `node-environment.json`. The last
file is a private test-harness environment map, never a public configuration or
report. Only the declared NATS/source secret references are accepted. The
template must contain each of `@GROUP@`, `@STATE@`, `@SCRATCH@` and `@METRICS@`
exactly once; the observer substitutes its owned paths and the requested mode.
Use private permissions throughout the fixture and require verified TLS.

The client host needs Python, PyArrow, the reviewed protocol reader, private
gateway credentials and independent Arrow reference files. The projection
fixture contains `trip_id`, `pickup_zone_id` and `fare_cents`; the CTE joins the
million-row trips table to 265 zone records. Reference types, values and NULLs
must come from the source independently of Kelvo's result path.

Provision the same worker lease and query policy into NATS, gateway and node.
Changing only one YAML file is not a policy migration. A 30-second lease was
selected for the WAN fixture; failure detection is consequently slower than
with a five-second lease. The result-publication deadline remains three seconds.
Keep control and data transports separate when an SSH tunnel is required.

## Run matched profiles

Start `node_capacity_worker.py --directory ... --profile ... --metrics disabled`
on the worker VM. Run `node_capacity_client.py` with the matching profile/mode
and the client fixture directory on the separate host. When it finishes, create
the worker profile's `stop` file, then wait for service and cgroup cleanup.
The outer controller must remain alive and use bounded SSH keepalives/timeouts.
Use the [cleanup coordinator](../scripts/cleanup_coordinator.py) to attempt both
hosts before persisting local receipts. Keep each hard-expiry timer armed until
all of its host-specific cleanup claims pass, and retain authoritative reports
on the remote hosts. Its [six controls](../scripts/test_cleanup_coordinator.py)
cover local write failures, remote failures and malformed cleanup proofs.
Before reusing the fixture worker identity, observe its previous heartbeat and
wait until the configured fencing lease has expired. Clean process shutdown does
not delete that lease. Preserve any premature launch failure; never reset the
lease or automatically replay the failed launch to conceal it.

The client first checks a native million-row projection, a federated million-row
projection and a CTE/zone join. `--queued-operations 10` adds ten simultaneous
submissions, alternating five native and five federated million-row queries.
One execution permit remains in force: this measures queued concurrency, not
ten native engines running at once. Each handle is claimed once; the client
never silently replays an ambiguous result.
Metrics-enabled profiles separately record the one successful startup probe and
require the subsequent success-counter delta to equal the exact workload count.

Repeat at least five metrics-disabled/enabled pairs with the same binary,
normalized configuration, source data, references, limits and workload. Alternate
which mode runs first. Preserve client/observer input fingerprints and record
other host workloads, warm-cache conditions and the VM's actual CPU allocation.
Do not equate an Oracle micro-VM's visible logical CPUs with dedicated cores.

## Interpret the reports

Require complete unambiguous HTTP framing, the recognized durable-completion
header, one Arrow stream with EOS, exact types/values/NULLs and a successful
durable state for every case. Small fixed-length responses and chunked streams
are both valid only when their complete framing is verified. Resource acceptance
also requires unchanged inputs, usable samples, one observed native worker,
zero OOMs and complete owned-process, scratch and containment cleanup.

Report submission, observed assignment wait, first byte and full delivery
separately. Assignment polling gives an upper bound on queue wait. Full-delivery
timings include SQL execution, dispatch, TLS and network transfer; Arrow decoding
is outside that interval. DuckDB materializes execution before delivery.

Process RSS covers observed node/worker descendants and can double-count shared
pages or miss short peaks. Cgroup memory includes the Python observer and kernel
accounting but excludes the external source, gateway, NATS and SSH processes.
Shared file-cache pages may remain charged to another cgroup across repeated
profiles, so lower cgroup memory alone does not prove a smaller process footprint.
Report both, including sampling gaps and host steal time. A small metrics delta
within trial variability does not establish a meaningful memory or speed gain.

The corrected observer captures its final resource sample after node and worker
shutdown, before verifying the final configuration, executable and input hashes.
Those checks still decide acceptance, but their reads and allocations are outside
the active/shutdown sampling interval and its reported cgroup counters and peaks.
Startup setup remains included in whole-cgroup counters. Identity hashing uses
bounded 1 MiB reads. [Observer regression evidence](evidence/node-capacity-observer-regressions.json)
verifies the ordering, read bound and failure propagation; it does not establish
a successful capacity campaign. Reports from the previous observer must not be
pooled with pairs from the corrected observer. Preserve every earlier failed or
incomplete campaign and start new matched pairs with recorded observer fingerprints.

This profile does not replace [sustained mixed-load acceptance](sustained-acceptance.md),
provider conformance, multihost availability or production deployment validation.

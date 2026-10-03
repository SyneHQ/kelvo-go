# Kelvo worker on Oracle E2.1.Micro

This guide measures a Kelvo **worker** on the micro VM. The gateway, NATS brokers
and source database run on Azure; all SSH tunnel processes are also excluded from
the worker's accounting. It does not establish that the whole cluster or a
standalone deployment fits in the micro VM's memory.

## Current candidate rerun: 2026-10-03

Two [fresh preflights](evidence/node-capacity-observer-gap-refusals.json) subsequently
passed all three correctness queries each, but failed the unchanged resource
observer gate: maximum sampling gaps were **2.635 s and 2.986 s**, above its
2-second limit. Neither attempt launched a paired profile. The one retry reused
verified executable inodes to reduce duplicate file-cache pressure; that did not
resolve the refusal. These failures remain part of the evidence even if a later
observer implementation succeeds.

The frozen observer hashes the executable and inputs synchronously between its
periodic loop and shutdown sampling. Its ordering needs correction and validation;
the receipts do not establish that hashing explains every part of either gap.
Worker exits were zero with no OOMs or leaked processes. Independent cleanup
verified removal of both temporary source accounts, forwarding keys, services and
cgroups before the unchanged 18:43:57 UTC deadline. Fixture controllers retained
exit 143 from their explicit SIGTERM cleanup handler; that history is preserved.
After capture, only those two stopped units were reset and verified unloaded,
with absent cgroups; older failed units were left untouched.
Each temporary Azure control fixture was capped at 1 CPU / 1 GiB. The existing
ClickHouse source container remained separate, unchanged and excluded from that
cap and from the Oracle worker measurements. **Five-pair acceptance remains
blocked by observer evidence; no threshold was relaxed or further retry made.**

The [bounded candidate rerun](evidence/node-capacity-b09f2da.json) completed
**91/91 workload queries**, plus a separate **3/3 correctness preflight**.
Seven profiles produced three complete metrics-disabled/enabled pairs and one
unpaired metrics-enabled profile. Every completed profile passed exact values,
NULLs, Arrow completion, durable success and resource cleanup checks, with zero
OOMs under the same 640 MiB worker service cap and one execution slot.

The controller refused the next profile before launch to preserve its fixed
cleanup window. **Five-pair capacity acceptance remains incomplete.** These
results must not be combined with the earlier runtime's baseline to satisfy that
requirement. They represent 84,000,056 repeated workload result rows, excluding
the preflight, rather than distinct source records or a production capacity SLA.

The candidate is `b09f2daad7caccf5caa7e35df5805696c9bc13ba`, binary SHA-256
`26ea84328e850116f4d5f31ab758bea1f5c57b7161eca0d61ddd63f9d714af26`;
its [845-file source manifest](evidence/node-capacity-b09f2da-source.json) pins the
build. An initial package download timed out before any query ran; its resumed
transfer passed the complete checksum before extraction. No query was retried.
Independent cleanup checks verified source-account/key removal, preserved source
tables, successful control-service shutdown and removed tunnel cgroups at
15:32:24 UTC, before the unchanged 15:38:09 UTC fixture expiry.

## Pinned baseline: 2026-10-03

[Ten profiles](evidence/node-capacity-6749369.json) completed **130/130 queries**:
five matched metrics-disabled/enabled pairs, including ten bursts of ten
simultaneously submitted million-row operations. Each profile used **one execution slot**, three serial
queries, then a burst of ten queued submissions. Exact types, values, NULLs,
Arrow EOS and durable successful state passed for every response. These are
120,000,080 repeated result rows from a million-row taxi selection and 265 zones,
not 120 million distinct source records.

The tested runtime is `67493699eb0ec97857a685923df225ad19b0cf62`, binary SHA-256
`e6ace8d4c560198c801ba531952bc9e10b3d83ac36e1f6bc5950a329e765c71c`.
The [805-file source manifest](evidence/node-capacity-6749369-source.json) identifies
the exact build. Later runtime changes require new validation; this baseline does
not certify the current release. The host exposed two logical CPUs, fractional
E2.1.Micro CPU allocation and 951.3 MiB RAM. Source and result LZ4 were enabled.

| Full-delivery workload | Metrics disabled median / p95 | Metrics enabled median / p95 |
| --- | ---: | ---: |
| Native million-row projection | 6.583 / 11.266 s | 5.925 / 6.042 s |
| Federated million-row projection | 9.348 / 9.923 s | 8.793 / 9.102 s |
| Federated CTE/zone join, eight result rows | 7.337 / 7.513 s | 7.097 / 7.534 s |
| Queued million-row operation, including assignment wait | 39.006 / 73.383 s | 38.473 / 72.492 s |

The first three rows contain five samples per mode; queued rows contain 50 per
mode. p95 uses the empirical nearest rank, not a confidence bound. Arrow decoding
is outside delivery timing; SQL, dispatch, verified worker/gateway TLS and SSH
transport are included. Native SQL executes on Azure; federation executes on
Oracle. This does not compare Kelvo against a direct analytical-library baseline.

Median per-profile combined node/worker RSS peaks were **230.9 / 229.8 MiB**;
cgroup peaks were **183.4 / 181.1 MiB** for disabled/enabled metrics. The 640 MiB
worker service cap included its Python observer, with zero swap. Every profile
had one observed worker, zero OOMs and complete process, scratch and containment
cleanup. The temporary source user, forwarding key, brokers and tunnel cgroups
were removed; source tables were preserved. Cleanup finished at 14:49:02 UTC,
before the original 15:38:09 UTC fixture deadline.

Caches were warm and uncontrolled; other bounded Azure acceptance tests ran
concurrently, while Oracle CPU burst availability and host steal varied. These
five pairs show trial variability, not causal savings from enabling metrics.
They do not establish minimum production RAM, whole-cluster fit or an SLA.

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

This profile does not replace [sustained mixed-load acceptance](sustained-acceptance.md),
provider conformance, multihost availability or production deployment validation.

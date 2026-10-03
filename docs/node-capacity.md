# Kelvo worker on Oracle E2.1.Micro

This guide measures a Kelvo **worker** on the micro VM. The gateway, NATS brokers
and source database run on Azure; all SSH tunnel processes are also excluded from
the worker's accounting. It does not establish that the whole cluster or a
standalone deployment fits in the micro VM's memory.

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

# Federation capacity testing

The capacity fixture uses real NYC TLC Yellow Taxi records, the official taxi
zone lookup, and three custom Go adapters: ClickHouse, PostgreSQL and MySQL.
Use these workload-specific measurements to size a deployment. They are not a
general throughput guarantee or a production multi-tenant certification.

## Data and correctness

[Provenance](evidence/federation-capacity-provenance.json) records the original
download URLs, sizes, hashes, import settings and transformations. January–March
2019 contain **22,612,607 trips**. The official lookup contains **265 zones**.
The first million physical January records are projected into all three databases
for large join inputs. They are real records, not generated fact rows.

The original floating-point amounts are preserved. `fare_cents` is a separately
documented derived metric using half-to-even rounding; it is not an exact decimal
representation supplied by TLC. `trip_id` combines the source month and physical
row ordinal, giving the copied relations a stable join key. Source anomalies are
not silently dropped.

[Native reference results](evidence/federation-capacity-references.json) are
computed in ClickHouse over the same relations. Validation hashes exact column
values and NULL validity independently of Arrow batch boundaries and physical
integer width. Every successful measured export must match its native reference
and include the Arrow completion marker. Failed queries must not publish a
completed output file.

## Workloads

- A full fact scan joined to the taxi zone lookup, followed by grouped aggregates.
- The 22.6-million-row base relation joined to a one-million-row projection in
  ClickHouse, PostgreSQL or MySQL. Runtime filters can reduce fetched rows; the
  report records actual source rows instead of assuming the whole base was read.
- A three-adapter join with separately declared scan/thread limits.
- A full 22.6-million-row sort and export with 128 MiB and 256 MiB DuckDB budgets.
- ClickHouse transfer through a fixed-destination relay with 50 ms application
  delay and a shared 10 MiB/s response-body limit. PostgreSQL/MySQL traffic stays
  on its ordinary same-VM path in these mixed-source runs.
- A paced HTTP consumer, cancellation and subsequent single-slot recovery.
- Repeated queries through tenant-scoped NATS workers and two gateways.
- A real remote client over SSH forwarding with inner verified TLS.

## Measured CLI results — 2026-10-02

[All 14 capacity checks passed](evidence/federation-capacity.json) on the
[recorded image binary](evidence/federation-capacity-build.json). These are single
timed runs per CLI configuration, with warm/uncontrolled caches on four logical
Xeon Platinum 8573C CPUs and approximately 31.3 GiB physical memory. All 11 CLI
results and both HTTP exports matched the native reference values. The remaining
check exercised cancellation and admission recovery.

| Workload | DuckDB budget | Seconds | Sampled worker RSS | Peak allocated scratch |
| --- | ---: | ---: | ---: | ---: |
| Fact + 265 ClickHouse zones | 128 MiB | 2.751 | 265 MiB | 239 MiB |
| Fact + 265 ClickHouse zones | 256 MiB | 2.471 | 446 MiB | 166 MiB |
| Fact + 1M ClickHouse rows | 128 MiB | 0.844 | 147 MiB | 0 MiB |
| Fact + 1M ClickHouse rows | 256 MiB | 0.759 | 149 MiB | 0 MiB |
| Fact + 1M PostgreSQL rows | 256 MiB | 1.631 | 154 MiB | 0 MiB |
| Fact + 1M MySQL rows | 256 MiB | 1.638 | 154 MiB | 0 MiB |
| Fact + 1M PostgreSQL rows + MySQL zones | 256 MiB | 3.141 | 455 MiB | 337 MiB |
| Full sort and 22.6M-row export | 128 MiB | 7.835 | 717 MiB | 232 MiB |
| Full sort and 22.6M-row export | 256 MiB | 7.738 | 834 MiB | 162 MiB |

The three-adapter query used four threads/active scan slots; other CLI runs used
two. Every fact scan fetched all **22,612,607 rows**. The million-row joins fetched
another million rows and returned 253 groups; the zone and three-adapter joins
returned eight groups. These describe relation sizes, not an EXPLAIN-verified
physical build side. Scratch peaks include observed DuckDB temporary files; the
physical plan was not captured.

Each full-sort export contained **454,902,480 bytes** and all 22,612,607 rows,
giving **2.886–2.922 million output rows/s**. Source scan response bodies contained
458,044,512 bytes, excluding schema discovery and HTTP headers. Coordinator RSS
was separately about 61 MiB. The **717–834 MiB worker RSS**
is a sizing constraint: these host-process runs did not prove the workload fits
inside a 128, 256 or 512 MiB container. No memory setting in this matrix failed;
that does not establish a minimum viable memory budget.

With the ClickHouse hop delayed by 50 ms and capped at 10 MiB/s, the zone join
took **28.635 s**, and the PostgreSQL join took **36.275 s**. PostgreSQL traffic
was not paced. Complete ordered million-row HTTP exports took **0.247 s** without
pacing and **19.288 s** at a client read rate of 1 MiB/s; worker RSS was about
156/161 MiB. These loopback transfers are separate from the remote-client test.

Cancelling an active result stream recovered the single execution slot for a
real zone-count query in **0.093 s**. One transient HTTP 429 occurred while
asynchronous worker teardown still held the permit; the harness records it and
retries the unclaimed replacement within a bounded deadline. Cancellation
acknowledgment is not a promise that resource cleanup has already finished.

## Sustained tenant concurrency

[All 224 cluster jobs passed](evidence/federation-cluster-capacity.json) exact
reference checks with **zero errors**, using two tenants, two gateways, three
nodes and three NATS brokers on the same four-CPU VM. Each concurrency level ran
for a 120-second submission window. Jobs alternated between PostgreSQL/MySQL
zone and million-row joins; each fetched all 22,612,607 fact rows. Across the
suite that was **5,065,223,968 fact rows fetched**, not that many rows exported.

| Concurrent clients | Successful jobs including drain | Completions/s within window | p50 latency | p95 latency |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 58/58 | 0.475 | 2.430 s | 2.691 s |
| 2 | 80/80 | 0.650 | 3.053 s | 3.944 s |
| 4 | 86/86 | 0.683 | 5.634 s | 7.590 s |

Rates count only the 57/78/82 successful completions inside each submission
window. Remaining jobs completed during drain. Latencies and closed-loop pacing
include result verification and client HTTP/TLS setup. There is one interval per
concurrency level, with no confidence interval or long-duration soak claim.
Tenant counts were balanced 29/29, 40/40 and 43/43. Maximum observed simultaneous
query workers matched 1/2/4, and no query worker remained after any interval.

The largest sampled worker used **449.7 MiB RSS**. During the four-client interval,
all query workers together reached a **simultaneously sampled 1.34 GiB RSS**;
source databases, nodes, gateways and brokers are separate. Shared pages can be
counted in multiple process RSS values. The configured DuckDB budget was 256 MiB
per query with two threads; tenant A had two one-slot nodes and tenant B a
two-slot node. Tenant admission remained 16 nonterminal jobs, with four total
execution slots. No limits were raised between trials.

Throughput barely increased from two to four clients while p95 nearly doubled.
The four-client window recorded roughly 3.9 CPU cores of sampled process work
across the four-core host. This indicates little headroom for this co-located
workload; it does not measure scaling workers across separate machines.

[Separate preflight checks](evidence/federation-capacity-preflight.json) verified
tenant markers, cross-gateway retrieval, foreign-handle denial even with forged
tenant headers, and HTTP 429 when tenant A filled its 16 nonterminal handles.
Tenant B still completed a query. These admission negatives were excluded from
the sustained query rates. Public taxi copies in both tenants do not themselves
prove isolation; the independent identity and network controls supply that evidence.

## Actual remote-client transfer

[Three remote trials](evidence/federation-wan.json) streamed the same ordered
million-row result from the VM to the macOS client: **20,117,616 bytes** each.
Every complete body matched the exact SHA-256 of an independently decoded and
value-checked VM reference. The client retained only its rolling hash, byte count
and completion marker; no result file or Arrow dependency was needed locally.

| Trial | Complete export | First 64 KiB | Output rows/s | Response body MB/s |
| --- | ---: | ---: | ---: | ---: |
| 1 | 8.496 s | 2.250 s | 117,709 | 2.368 |
| 2 | 5.364 s | 1.812 s | 186,435 | 3.751 |
| 3 | 5.765 s | 1.799 s | 173,462 | 3.490 |

This is a real remote path with SSH forwarding and inner verified TLS. SSH,
network conditions, query execution and a new HTTPS connection per request are
included. It is not direct HTTPS capacity or an engine-only throughput figure.
Health requests took 0.834–0.843 s including TLS/server overhead; that is not a
pure RTT measurement. Final successful query-state verification followed the
export interval and is recorded separately.

The macOS client's stricter OpenSSL verification rejected the fixture CA's
missing key-usage extension. The fixture generator now explicitly sets CA key
usage and constraints. Reissuing that private test CA certificate with the same
key fixed validation; no verification flag was disabled and no service key was
rotated.

## Read the measurements correctly

`rows_fetched` counts rows delivered to Kelvo, not rows examined by the database.
An aggregate can fetch millions of rows and return only a handful. Export rows/s
uses the final output row count and includes query startup, execution and complete
delivery. The CLI export interval also includes file synchronization; reference
calculation and value verification are outside that interval.

The Go DuckDB driver materializes execution before Arrow delivery. A source's
bounded handoff does not bound the memory needed for joins, sorting or materialized
results. `memory_mb` configures DuckDB; it is not a whole-process RSS limit. Use
container memory and disk quotas in deployment. Sampled RSS/spill peaks can miss
short peaks and must not be treated as exact maxima. Do not sum independent
per-process peak measurements as if they occurred simultaneously.

The controlled relay is an application-level experiment. It does not simulate
packet loss, jitter, geographic routing or TCP behavior. The actual remote-client
test includes SSH encryption/buffering plus TLS, so it does not establish direct
HTTPS WAN capacity. Cache state is warm or uncontrolled, and the four-CPU test VM
also hosts the source databases and brokers.

Cluster capacity runs use tenant-bound host processes with Landlock and mTLS.
Separate container/network enforcement is checked by
[container acceptance](evidence/federation-capacity-container.json); the
load fixture itself is not a production container topology or multi-zone HA test.

## Reproduce on the test host

Use a Linux amd64 host with Landlock ABI 3+, Docker, Python/pyarrow and a
bridge-enabled image. Keep dataset import, image compilation and timed tests in
separate CPU windows. The fixture needs private scratch storage and a pre-existing
ClickHouse container named `kelvo-clickhouse`; PostgreSQL/MySQL use cached official
images. It never replaces the existing `kelvo_bench.fact_events` dataset.

The scripts separate data preparation from measurement:

| Script | Responsibility |
| --- | --- |
| [federation_relational_acceptance.py](../scripts/federation_relational_acceptance.py) | Private relational fixtures, read-only TLS users, exact values, joins, denial and cancellation checks |
| [federation_capacity.py](../scripts/federation_capacity.py) | TLC imports, native references, CLI joins/sorts, relay and slow-consumer trials |
| [federation_capacity_cluster.py](../scripts/federation_capacity_cluster.py) | Owned NATS/app lifecycle, explicit tenant source environments and resource settings |
| [federation_cluster_load.py](../scripts/federation_cluster_load.py) | Sustained cluster query/concurrency trials |
| [federation_wan_client.py](../scripts/federation_wan_client.py) | Remote export verification using a VM-decoded reference; no local result storage |

Run each script's `--help` for its action-specific arguments. Retain the private
fixture manifest until all dependent tests finish. Source credentials and temporary
CA material stay in private VM files; public reports contain measurements and
hashes. Stop only recorded fixture processes afterward, preserving the taxi data
for subsequent comparisons.

After this run, [cluster cleanup](evidence/federation-capacity-cleanup.json) stopped
all owned app/broker processes, and [relational cleanup](evidence/federation-relational-cleanup.json)
removed only the disposable source fixtures. The main ClickHouse server, all
22,612,607 trips, the million-row projection, the 265 zones and the original
100-million-row benchmark remained intact. Dataset files were preserved. Create
fresh relational fixtures and refresh their private source manifest for a rerun.

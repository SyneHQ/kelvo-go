# Oracle micro VM capacity testing

On an Oracle `VM.Standard.E2.1.Micro`, Kelvo completed all 30 native million-row
exports in ten-client cohorts at **469,800 output rows/s** with source and result
LZ4. After explicit row/byte-limit increases, all 30 four-million-row exports
also passed, at **443,421 output rows/s**. Both retained a **640 MiB service
memory cap**. Native SQL executed on a separate Azure source database; the
macOS client verified complete remote Arrow exports through SSH.

Ten simultaneous federated exports exhausted the cgroup and completed none.
Four execution permits completed all 12 admitted exports while rejecting 18
attempts with HTTP 429 across three ten-client cohorts. Result LZ4 saved output
bytes but made the separate federated CLI median slower. The full sort produced
no successful export; its retry ended in a deadline error. These are measurements
of this workload and network path, not a production capacity or multi-tenant
availability guarantee.

## Host and topology

The [host record](evidence/oracle-micro-host.json) identifies the Mumbai-region
shape, AMD EPYC 7551 processor, two guest logical CPUs, **951.3 MiB physical
memory** and no swap. [Oracle's Always Free documentation](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm)
lists this shape as **1/8 OCPU with additional CPU bursting** and 1 GB memory.
Two visible logical CPUs do not establish two dedicated cores. The harness set
`CPUWeight=20`, a relative scheduling weight, and did not configure a CPU quota.
Host CPU steal varied across the CLI trials, so the timings include variable
scheduling access. The existing Tailscale service remained active.

ClickHouse and all source data stayed on Azure. Oracle reached its private
ClickHouse HTTP endpoint through a pinned-host-key SSH forward. HTTP was confined
to loopback and the private source-container network; no database was installed
on the micro VM. The remote export client ran on macOS and used a separate
Mac-to-Oracle SSH forward. Both application HTTP hops were carried by SSH, with
**no inner HTTP TLS** in this experiment. This differs from the verified-TLS
remote test in [federation capacity testing](federation-capacity.md).

The sequential baseline ran with `MemoryMax=640M`, `MemorySwapMax=0`,
`TasksMax=96`, a 128 MiB query memory setting and two query threads. Each query
could use up to 512 MiB of temporary disk on a dedicated **2 GiB ext4 scratch
filesystem** mounted `nodev,nosuid,noexec`. The ten-client tests kept the 640 MiB
memory cap but used `TasksMax=256` and 64 MiB temporary disk per query. CLI and
HTTP workers explicitly used the sandbox launcher. Observed `NoNewPrivs` and
seccomp flags alone are not a complete Landlock policy verification.

The baseline runtime source revision was `e955597`, with binary SHA-256
`51ab67046f31debe0a9358c24d5ab732380796ddcb2eb0bd352c4c9220e61fff`.
The baseline CLI, full-sort and HTTP concurrency measurements use that binary.
The source-compression comparison below uses a separately identified build.

## Data and correctness

The source held **22,612,607 real NYC TLC trips**, a one-million-row projection
and the **265-row official taxi zone lookup**. The shared
[provenance](evidence/federation-capacity-provenance.json) records source files
and transformations, including the separately derived `fare_cents` metric and
stable trip identifiers. The micro VM did not generate replacement fact data.

[Source preflight](evidence/oracle-micro-source-preflight.json) confirmed the
configured read succeeded and a write was denied. All 15 completed CLI outputs
were [independently decoded and value-checked](evidence/oracle-micro-values-regular.json)
against the [native reference results](evidence/federation-capacity-references.json).
Canonical hashes account for exact values and NULL validity while allowing
different Arrow batches and physical integer widths. The byte hashes and Arrow
completion markers in the timing harness are additional checks, not substitutes
for decoded validation.

An [earlier unsuccessful fixture run](evidence/oracle-micro-fixture-failure.json)
is retained separately. Its 15 attempts reported `query_process_failed`; the
public report does not establish a more specific cause. Those attempts are not
included as successful timings.

## Measured CLI results — 2026-10-02

[All 15 baseline trials completed](evidence/oracle-micro-queries.json): three
sequential trials per configuration, with warm or uncontrolled caches. The
headline interval uses systemd process start and exit timestamps and includes
the environment wrapper, CLI execution and output file synchronization.
Reference calculation, output hashing and decoded validation are outside that
interval. The separate polled wall interval includes control polling delay and
is not used for the table.

| Workload | Output rows | Seconds, trials 1 / 2 / 3 | Median seconds | Sampled worker RSS | Cgroup memory peak | Allocated scratch peak |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Native zone join and aggregate | 8 | 1.334 / 1.454 / 1.455 | 1.454 | 77.7–77.8 MiB | 15.6–16.1 MiB | 0 MiB |
| Native ordered million-row export | 1,000,000 | 6.597 / 4.319 / 4.331 | 4.331 | 81.0–82.2 MiB | 42.0–42.7 MiB | 0 MiB |
| Federated ordered million-row export | 1,000,000 | 8.404 / 8.617 / 8.539 | 8.539 | 166.0–167.1 MiB | 108.1–116.4 MiB | 0 MiB |
| Federated fact + zones aggregate | 8 | 62.767 / 61.628 / 64.459 | 62.767 | 267.8–269.0 MiB | 288.7–327.8 MiB | 243.8–252.0 MiB |
| Federated fact + million-row join | 253 | 69.999 / 71.962 / 72.047 | 71.962 | 149.6–150.1 MiB | 69.7–73.1 MiB | 0 MiB |

Memory and scratch columns show the range of per-trial peaks. No OOM was
observed in these successful trials. Native execution performs the SQL on Azure
and transfers its final output. Federation transfers source rows to Oracle for
local computation; matching values do not make the placement or source work
identical. The native zone aggregate therefore transfers eight result rows,
whereas the federated version fetches the full fact relation.

| Federated workload | Rows fetched from source | Logical Arrow bytes fetched | Output rows |
| --- | --- | ---: | ---: |
| Ordered million | 1,000,000 sample rows | 20,000,000 | 1,000,000 |
| Fact + zones | 22,612,607 trips + 265 zones | 271,351,284 + 4,135 | 8 |
| Fact + million-row join | 22,612,607 trips + 1,000,000 sample rows | 361,801,712 + 12,000,000 | 253 |

`rows_fetched` counts rows delivered to Kelvo, not database storage rows examined.
These byte counts describe fetched Arrow data, not compressed network traffic.
The native and federated million-row result bodies were respectively
20,006,992 and 20,117,616 bytes, despite containing the same canonical values.

## Remote HTTP exports

[All six remote exports](evidence/oracle-micro-http.json) completed: three
alternating native/federated trials, each returning one million rows. The macOS
client streamed, hashed and discarded each response. Every body matched the
exact bytes of its independently decoded CLI reference, included the Arrow
completion marker, and had final API state `succeeded` with one million rows.
The client did not decode Arrow locally.

| Mode | Complete export seconds, trials 1 / 2 / 3 | Median seconds | First 64 KiB | Output rows/s | Response body MB/s |
| --- | --- | ---: | ---: | ---: | ---: |
| Native | 5.469 / 5.601 / 5.108 | 5.469 | 1.910–2.085 s | 178,528–195,755 | 3.572–3.916 |
| Federated | 10.487 / 9.727 / 10.486 | 10.486 | 6.885–7.368 s | 95,354–102,804 | 1.918–2.068 |

Export timing includes submission, query execution, delivery and rolling SHA-256
through the existing SSH forward. Final API state lookup is outside the export
interval. Each HTTP request opens a new connection. Health requests took about
0.061–0.063 seconds including HTTP/server overhead; that is not a pure RTT.
MB/s uses decimal megabytes of response body, excluding SSH and HTTP framing.

The [HTTP service resource report](evidence/oracle-micro-http-resources.json)
observed all six workers with a maximum of one worker at a time. The largest
sampled worker RSS was **151.1 MiB**, and coordinator plus worker reached a
**simultaneously sampled 211.6 MiB RSS**. Cgroup charged memory peaked at
**97.7 MiB**. No scratch allocation or memory/pids limit events were observed.
These are whole-service-interval measurements, including idle time and readiness
checks, not per-export peaks. The service cgroup was empty after the recorded
stop. Source and client SSH transport processes ran outside the Kelvo cgroup,
so their CPU and memory are excluded from those service figures.

## Full-sort attempts

The [full-sort retry](evidence/oracle-micro-limit-retry.json) conclusively exited
with status 1 and no successful export. A [separate diagnostic](evidence/oracle-micro-limit-diagnostic.json)
verified the exact public `Query deadline exceeded` message against the retained
stderr hash. The configured query deadline was **240 seconds**, but the verified
CLI process interval was **268.790 seconds** and the harness wall interval was
**269.388 seconds**. Deadline handling and process exit were delayed under the
observed host pressure; this run does not demonstrate a timely stop at 240
seconds.

The updated harness recorded and retried **nine status-command timeouts** and
ultimately captured a known final `exit-code` state. Cgroup memory peaked at
**628.3 MiB**; the retained `max`, `oom`, `oom_kill` and `oom_group_kill` counters
were all zero, and systemd did not report an OOM. This establishes a deadline
failure for this configuration, not a minimum memory requirement or an OOM.
The [retry validation report](evidence/oracle-micro-values-limit-retry.json)
contains no validated output and reports `all_passed=false`.

The [earlier 22.6-million-row sort attempt](evidence/oracle-micro-limit.json) did not
produce a verified final export or process exit. After about **129.9 seconds**,
the harness reported `systemd_command_timed_out` and
`final_unit_status_unavailable`. The last observed service state was still
running; the exact owned unit was then stopped successfully. This is a control
and observation failure, not proof that the engine exhausted memory or could
not finish the query.

Before cleanup, cgroup memory peaked at **624.1 MiB**, sampled worker RSS reached
**596.2 MiB**, allocated scratch reached **235.4 MiB**, and host available memory
fell to **85.3 MiB**. The last recorded `memory.events` counters for `max`, `oom`
and `oom_kill` were all zero. The report therefore does **not establish an OOM**,
a successful full sort, or a minimum viable memory limit. Sampling gaps reached
1.398 seconds, and final service status was unavailable.

The [limit validation file](evidence/oracle-micro-values-limit.json) contains an
empty result list. Its `all_passed` flag is not a successful value check for this
attempt. Later changes that retry control-command timeouts cannot retroactively
resolve this run's missing completion evidence.

## Ten-client remote exports

[All 30 native exports](evidence/oracle-micro-native10.json) matched their
million-row references in three synchronized ten-client cohorts. The service
allowed ten execution permits and 32 retained handles, with a 128 MiB query
memory setting, two threads and 64 MiB temporary disk per query. Whole-service
limits were 640 MiB memory, zero swap and 256 tasks. Each cohort independently
observed ten overlapping client result requests, and the
[server sampler](evidence/oracle-micro-native10-resources.json) independently
observed **ten simultaneous query workers**.

| Native cohort | Verified exports | Cohort seconds | Aggregate output rows/s | Export p50 | Export p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 10/10 | 37.864 | 264,104 | 37.310 s | 37.796 s |
| 2 | 10/10 | 38.070 | 262,672 | 37.946 s | 38.004 s |
| 3 | 10/10 | 37.944 | 263,545 | 37.831 s | 37.875 s |

Across all three cohorts, **30 million verified output rows** and 600,209,760
response-body bytes were delivered in a summed cohort interval of 113.878
seconds: **263,439 output rows/s**, or **5.271 MB/s**. Overall successful-export
p50 was 37.796 seconds and p95 was 38.004 seconds. There were no HTTP 429s,
disconnects or failed exports. The server remained healthy after each cohort;
no memory or pids limit events were observed, and its cgroup was empty after
the recorded stop. Native SQL still executed on Azure, so these results describe
concurrent remote exports through Kelvo, not ten local federated computations.

Oracle advertises up to **50 Mbps internet bandwidth** for this shape, versus
480 Mbps for its private same-region network. The measured 5.271 MB/s is about
**42.2 Mbps of Arrow response bodies**. That is useful context for this remote
path, but the ratio alone does not prove network saturation or identify the
bottleneck. At roughly 20 response-body bytes per output row, one million
output rows/s would require about 160 Mbps before HTTP/SSH overhead; engine-only
rows/s should not be used as an expected export rate for this VM and path.

The [ten-client federated attempt](evidence/oracle-micro-fed10.json), using the
same million-row result and service limits, completed **0/10 exports**. All ten
POSTs returned 201 and ten workers overlapped, but every result request lost its
connection before receiving response headers or body bytes. There were no 429s
or successful result HTTP 200s. The cohort ended after 45.935 seconds, including
unsuccessful cancellation attempts. No further ten-worker federated cohorts
were run. That interval is not a successful-export latency.

Here the [server evidence confirms a cgroup OOM](evidence/oracle-micro-fed10-resources.json):
memory reached exactly **640 MiB**, `memory.events` recorded `max=700`, `oom=12`,
`oom_kill=7` and `oom_group_kill=1`, and systemd reported `Result=oom-kill` with
signal 9. The configured `OOMPolicy=kill` stopped the service, including its
coordinator and active workers. The final status was available, and cleanup
verified an empty cgroup. This is a service-wide failure at the tested limit,
not graceful per-query admission rejection. Host available memory fell to
49.7 MiB, and the largest sampling gap was 0.700 seconds.

[Reducing federation admission to four permits](evidence/oracle-micro-fed4.json)
completed **all 12 admitted exports** across three further ten-client cohorts.
All 30 POSTs created handles, but each cohort's result requests produced four
complete, reference-matched exports and six explicit HTTP 429
`RESOURCE_EXHAUSTED` responses. The harness counted those **18 rejections as
failed exports** and cancelled their handles; it did not queue or retry them.
The client result was therefore **12/30 successful attempts**, with no
disconnects or failures after HTTP 200.

| Four-permit federated cohort | Verified exports / attempts | HTTP 429 | Cohort seconds | Aggregate output rows/s | Successful export p50 | Successful export p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4/10 | 6 | 37.543 | 106,545 | 37.065 s | 37.469 s |
| 2 | 4/10 | 6 | 33.024 | 121,124 | 32.726 s | 32.956 s |
| 3 | 4/10 | 6 | 36.597 | 109,297 | 35.994 s | 36.496 s |

The three cohorts delivered 12 million verified rows in 107.164 seconds of
summed cohort time: **111,978 output rows/s**, or **2.253 MB/s**. The
[server report](evidence/oracle-micro-fed4-resources.json) observed four workers
at once, verified both initial limits and final service state, and recorded no
control timeouts, OOMs or memory/pids limit events. The server remained healthy
after every cohort and its cgroup was empty after the recorded stop. All other
service/query limits matched the ten-permit tests.

| Ten-client mode | Largest individual worker RSS | Simultaneously sampled worker RSS sum | Coordinator + workers RSS sum | Cgroup charged memory peak |
| --- | ---: | ---: | ---: | ---: |
| Native, three cohorts | 63.9 MiB | 621.3 MiB | 693.6 MiB | 141.8 MiB |
| Federated, ten permits, one failed cohort | 114.1 MiB | 1,088.3 MiB | 1,146.2 MiB | 640.0 MiB |
| Federated, four permits, three cohorts | 146.0 MiB | 555.1 MiB | 608.8 MiB | 431.9 MiB |

These peaks cover each complete service-monitoring interval, including idle
time. Worker sums are sampled together, not sums of independent per-worker
peaks; the columns can still peak at different times. RSS counts shared pages
in multiple processes and can therefore exceed the cgroup limit or physical
memory without representing that much unique allocation. Charged page-cache
ownership also differs from RSS, as explained below. No scratch allocation was
observed in these runs. Source database and both SSH transport processes are
outside these service CPU and memory measurements.

The parallel harness synchronizes both handle creation and result requests and
performs no retries. Export latency includes submission, the results barrier,
complete delivery and hashing; final-state verification is separate. Its
p50/p95 use linear interpolation over successful exports only. Aggregate
throughput counts verified outputs over the full cohort interval, including
verification and cleanup, and excludes the health checks between cohorts.
These short cohorts do not establish sustained capacity or an optimal admission
limit. Four permits avoided the observed ten-worker OOM for this workload;
intermediate admission levels were not measured.

In the standalone API, POST creates a retained handle without taking an
execution permit. GET results acquires a permit without waiting; full capacity
returns HTTP 429 and leaves the handle unclaimed. There is no automatic execution
queue. The permit lasts through response completion and worker teardown, and
HTTP 200 alone does not prove a complete export. Completed and cancelled handles
remain until the configured five-minute TTL and count toward the 32-handle limit.

## Source-only LZ4 comparison

[Nine uncompressed-source trials](evidence/oracle-micro-none.json) and
[nine LZ4-source trials](evidence/oracle-micro-lz4.json) used the same rebuilt
binary, SHA-256
`5331decbbba421b0d7bc4ce4b8e9ca294d7f714b01a7a6ea5947613f32983fb3`.
The [build record](evidence/oracle-micro-lz4-build.json) identifies revision
`81851d5`, verifies 181 source files against the checkout, and records the
`duckdb_arrow,duckbridge` tags plus `-trimpath` and stripped-symbol build flags.
This differs from the earlier baseline binary: its smaller uncompressed RSS
must not be attributed to LZ4.

Only ClickHouse's Arrow source response changed, through
`options.arrow_compression: lz4_frame`. Local worker IPC and final CLI outputs
remained uncompressed. The 640 MiB cgroup cap, zero swap, 128 MiB query setting,
two threads, 512 MiB temporary-disk budget, SQL and source rows were unchanged
between the two source-codec configurations. All 18 outputs passed independent
canonical validation, recorded for [none](evidence/oracle-micro-values-none.json)
and [LZ4](evidence/oracle-micro-values-lz4.json). Each query's final output byte
hash was identical across both codecs and all six trials.

| Workload | Source codec | Source response bytes | Median CLI seconds (range) | Median service CPU seconds (range) | Sampled worker RSS range |
| --- | --- | ---: | ---: | ---: | ---: |
| Native ordered million | none | 20,256,960 | 5.242 (4.524–5.734) | 0.434 (0.374–0.511) | 49.2–50.2 MiB |
| Native ordered million | LZ4 | 8,738,872 | 2.753 (2.722–3.506) | 0.434 (0.432–0.471) | 59.6–64.8 MiB |
| Federated ordered million | none | 20,256,480 | 8.643 (8.556–8.935) | 1.194 (1.109–1.265) | 133.6–135.7 MiB |
| Federated ordered million | LZ4 | 8,734,712 | 6.007 (5.615–6.186) | 1.289 (1.068–1.331) | 147.3–152.6 MiB |
| Federated fact + zones | none | 277,104,736 | 72.671 (63.149–81.962) | 32.299 (11.237–34.489) | 232.7–283.6 MiB |
| Federated fact + zones | LZ4 | 106,741,528 | 49.115 (44.121–50.672) | 31.436 (30.802–34.700) | 257.5–272.0 MiB |

Source response bytes fell by **56.9%**, **56.9%** and **61.5%**, respectively.
These are encoded source body bytes consumed, excluding HTTP/SSH framing and,
for federation, discovery requests. They differ from logical Arrow bytes and
from final output bytes. The zone aggregate still fetched 22,612,607 trips and
265 zones and returned only eight rows.

Service CPU seconds come from systemd's whole-cgroup accounting, including the
wrapper, coordinator and worker. They exclude source-side compression on Azure
and both SSH transports. LZ4 raised the sampled RSS range for both million-row
queries; smaller network bodies do not imply smaller process memory. All 18
queries completed with known final status and no observed OOM.

The uncompressed trials ran first, followed by LZ4; the runs were not interleaved
or randomized. Cache state and CPU scheduling were uncontrolled. For example,
host steal fractions during the zone trials ranged from 7.9–21.0% without LZ4
and 33.9–42.9% with it, and uncompressed service CPU varied from 11.237 to
34.489 seconds. The table supports this observed reduction in source bytes and
elapsed time, not a general compression speedup or CPU-cost guarantee.

## Result compression and byte limits

The [engine-wide result option](usage.md#opt-in-result-compression),
`--result-compression lz4_frame`, is separate from source compression and defaults
to `none`. It compresses public Arrow IPC record buffers across native,
federated and accelerated result paths. Cluster deployments use the provisioned
tenant `policy.limits.result_compression`; the node compresses once and the
gateway relays the bytes. The local worker pipe remains uncompressed. Clients
must support LZ4-compressed Arrow IPC; this is not HTTP `Content-Encoding`.

[Real cluster acceptance](evidence/cluster-compression-acceptance.json) passed
an eight-batch, 16,384-row DuckDB query with exact integer, Unicode and NULL
checks, Arrow completion, and matching node/client counts of 136,768 encoded
bytes. This verifies the exercised cluster result path outside the micro VM;
it is not a micro VM throughput measurement.

`--max-bytes` uses the same configured value to bound cumulative decoded Arrow
buffer bytes and encoded result bytes independently. Compression does not
multiply that cap: an 80 MiB decoded result compressed to 20 MiB still exceeds
a 32 MiB byte limit. A larger permitted result requires an explicit byte-limit
increase, plus an appropriate row limit. Increasing those limits does not raise
the 640 MiB `MemoryMax`, add RAM or increase the DuckDB memory setting. It also
does not establish that a larger materialized result fits on this host.

## CLI result-compression comparison

[Result-none](evidence/oracle-micro-result-none.json) and
[result-LZ4](evidence/oracle-micro-result-lz4.json) used the same final
[result-capable build](evidence/oracle-micro-result-build.json), with binary
SHA-256 `fefdba4eaa4b2ac99eaf2878d30cfe80ddeacaf1df4fac1c52182f9ca944d6ac`.
The build record identifies runtime change `b1a36c0`, verifies 184 source files
against checkout `8df16c0`, and records package tests, focused race/vet checks
and real cluster acceptance. This is a third binary, separate from both earlier
comparison builds.

ClickHouse source LZ4 was enabled in **both** result configurations; only the
public result codec changed. Native source bodies stayed at 8,738,872 bytes and
federated source bodies at 8,734,712 bytes. Each configuration ran three native
and three federated million-row exports with a 32 MiB result cap, 128 MiB query
setting, two threads and the same 640 MiB service limit. All 12 outputs passed
independent canonical checks, recorded for
[none](evidence/oracle-micro-values-result-none.json) and
[LZ4](evidence/oracle-micro-values-result-lz4.json).

| Workload | Result codec | Encoded output bytes | Median CLI seconds (range) | Median service CPU seconds (range) | Sampled coordinator + worker RSS range |
| --- | --- | ---: | ---: | ---: | ---: |
| Native ordered million | none | 20,006,992 | 2.840 (2.346–3.539) | 0.406 (0.385–0.527) | 115.8–121.4 MiB |
| Native ordered million | LZ4 | 8,759,064 | 2.575 (2.094–3.091) | 0.735 (0.603–0.955) | 113.4–119.2 MiB |
| Federated ordered million | none | 20,117,616 | 5.921 (5.632–6.350) | 1.352 (1.247–1.960) | 196.3–203.1 MiB |
| Federated ordered million | LZ4 | 9,289,424 | 6.485 (5.885–7.223) | 2.186 (1.762–2.242) | 201.4–203.8 MiB |

Result bytes fell by **56.2% native** and **53.8% federated**, but the federated
CLI median became **9.5% slower**. Median whole-service CPU increased in both
modes. Compression therefore saved output bytes without consistently reducing
CLI latency on this small host. These are local file exports with synchronization,
not remote client transfers. Trials ran with none first and LZ4 second, with
uncontrolled caches and shared-CPU scheduling; three trials cannot isolate a
general CPU or latency effect.

The RSS column uses a simultaneous coordinator-plus-worker sample for each
trial. Public result encoding happens in the coordinator, so worker RSS alone
would omit part of its cost. Whole-cgroup CPU includes both processes and the
wrapper, while excluding Azure and SSH work. No OOM or temporary scratch
allocation was observed in these 12 trials.

## Explicitly larger native export

[Three four-million-row CLI exports](evidence/oracle-micro-four-million.json)
completed with source and result LZ4 on the same final binary. Each used
`--max-rows 4000000` and `--max-bytes 134217728` (**128 MiB**), while retaining
the **640 MiB service cap**, **128 MiB query setting**, two threads and 512 MiB
temporary-disk allowance. These were explicit policy changes from the
million-row/32 MiB result configuration; compression did not raise a limit.

| Trial | Output rows | CLI seconds | Encoded output bytes | Sampled worker RSS | Cgroup memory peak |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 4,000,000 | 8.563 | 35,065,408 | 73.1 MiB | 81.6 MiB |
| 2 | 4,000,000 | 7.691 | 35,065,408 | 74.3 MiB | 89.6 MiB |
| 3 | 4,000,000 | 8.073 | 35,065,408 | 72.8 MiB | 79.4 MiB |

The [independent reference](evidence/oracle-micro-four-million-reference.json)
selects the first four million physical January rows using
`source_month=201901 AND source_row < 4000000`, ordered by the existing
`(source_month, source_row)` storage key. It projects three integer columns:
`trip_id`, `pickup_zone_id` and `fare_cents`. [All three decoded exports](evidence/oracle-micro-values-four-million.json)
matched its exact values, row count and NULL validity. All three Kelvo output
byte hashes also matched each other; the separately encoded direct ClickHouse
reference has a different IPC byte hash.

Each result contained 80,000,000 logical Arrow buffer bytes and 35,065,408
encoded output bytes; the source response contained 35,033,336 bytes. The
median CLI interval was 8.073 seconds. Coordinator plus worker reached
122.6–123.7 MiB sampled RSS across the trials; no OOM or temporary scratch
allocation was observed. This is a storage-key-ordered native export with the
SQL executed on Azure, not a successful four-million-row local federated sort.

An [earlier reference-preparation query](evidence/oracle-micro-four-million-reference-failure.json)
used a global `ORDER BY trip_id LIMIT 4000000` and hit ClickHouse's 512 MiB
source memory limit on Azure. That was direct reference preparation, not a
Kelvo throughput trial, and is retained separately from the corrected query
and successful exports.

## Ten-client remote result-compression comparison

Both final-binary profiles completed **30/30 native million-row exports** with
exact reference bytes, Arrow completion and successful final API state:
[result-none](evidence/oracle-micro-result-native10-none.json) and
[result-LZ4](evidence/oracle-micro-result-native10-lz4.json). Each ran three
ten-client cohorts over the same Mac-to-Oracle SSH path. Source LZ4 remained
enabled in both, while the public result codec changed. Each service allowed
ten workers and 32 retained handles, with a 32 MiB result cap, 128 MiB query
setting, two threads, 64 MiB temporary disk per query, 256 tasks and the same
640 MiB memory cap with zero swap.

| Result codec | Verified exports | Cohort seconds, 1 / 2 / 3 | Aggregate output rows/s | Export p50 | Export p95 | Response-body MB/s |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| none | 30/30 | 42.236 / 40.179 / 41.130 | 242,827 | 40.382 s | 42.166 s | 4.858 |
| LZ4 | 30/30 | 25.212 / 18.123 / 20.522 | 469,800 | 20.335 s | 25.118 s | 4.115 |

Each profile delivered 30 million verified output rows. Encoded bodies totalled
600,209,760 bytes without result compression and 262,771,920 with it. The summed
cohort intervals were 123.545 and 63.857 seconds, respectively: **1.93 times the
observed output-row throughput** with result LZ4. Body MB/s is lower for LZ4
because each row occupies fewer encoded bytes; it does not count decoded data
or SSH framing. Neither profile had rejections, disconnects or failed exports,
and both remained healthy after every cohort.

The independent resource reports for
[none](evidence/oracle-micro-result-native10-none-resources.json) and
[LZ4](evidence/oracle-micro-result-native10-lz4-resources.json) observed all 30
workers per profile and **ten workers at once**. Initial limits and final
service state were verified, with no control timeouts, OOMs, memory/pids limit
events or scratch allocation. Both service cgroups were empty after stopping.

| Result codec | Sampled-interval cgroup CPU | Largest individual worker RSS | Simultaneous worker RSS sum | Coordinator + workers RSS sum | Cgroup charged memory peak |
| --- | ---: | ---: | ---: | ---: | ---: |
| none | 15.378 s | 69.7 MiB | 645.3 MiB | 691.0 MiB | 377.3 MiB |
| LZ4 | 25.673 s | 74.3 MiB | 662.8 MiB | 717.5 MiB | 295.2 MiB |

Cgroup CPU is the delta between the first and last counter observations over
the service-monitoring interval, including readiness and idle time. It excludes
the source database and both SSH transports. CPU increased even while elapsed
time fell. The lower charged-memory peak with LZ4 coexisted with a higher RSS
sum; shared pages and page-cache ownership still prevent treating these as the
same memory quantity. RSS columns can peak at different samples.

None ran first and LZ4 second. This paired test supports the observed benefit
for this native remote-export workload, not a blanket speedup across engines,
queries, clients or network conditions. The separate federated CLI comparison
above was slower with result LZ4.

## Ten-client four-million-row remote exports

[All 30 larger remote exports](evidence/oracle-micro-result-native10-four-million.json)
matched the independently validated four-million-row CLI reference, including
exact encoded bytes, Arrow completion and successful API state. The same final
binary used source and result LZ4, ten execution permits, 32 retained handles,
four million allowed rows and a **128 MiB result-byte cap**. The **640 MiB service
cap** and **128 MiB query setting** remained unchanged, as did the two query
threads, 64 MiB temporary disk per query, 256 tasks and zero swap used by the
other ten-client result profiles.

| Cohort | Verified exports | Cohort seconds | Aggregate output rows/s | Export p50 | Export p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 10/10 | 90.550 | 441,743 | 90.474 s | 90.477 s |
| 2 | 10/10 | 67.812 | 589,866 | 67.709 s | 67.740 s |
| 3 | 10/10 | 112.261 | 356,314 | 112.155 s | 112.187 s |

The client received **120 million verified output rows** and **1,051,962,240
encoded bytes** in 270.623 seconds of summed cohort time: **443,421 output
rows/s**, or **3.887 MB/s**. Overall export p50 was 90.474 seconds and p95 was
112.173 seconds. These were 30 repeated exports of the same four-million-row
selection, not 120 million distinct source records. There were no HTTP 429s,
disconnects or failed exports, and the server remained healthy after each
cohort.

The [resource report](evidence/oracle-micro-result-native10-four-million-resources.json)
observed all 30 workers and a maximum of **ten at once**. The largest individual
worker reached **74.4 MiB RSS**, all workers together reached a simultaneously
sampled **713.9 MiB RSS**, and coordinator plus workers reached **744.8 MiB RSS**.
Cgroup charged memory peaked at **423.4 MiB**, and host available memory reached
a minimum of **200.9 MiB**. Shared-page and page-cache accounting explains why
the memory quantities differ; the RSS sums are not unique physical allocation.

Sampled-interval cgroup CPU was 93.046 seconds, excluding Azure and SSH work.
Initial limits and final service state were known, with no control timeouts,
OOMs, memory/pids limit events or scratch allocation. The service cgroup was
empty after stopping. The maximum sampling gap was 1.082 seconds and host CPU
steal occupied 36.7% of observed host ticks. Cohort times varied from 67.812 to
112.261 seconds, so the aggregate is not a stable per-cohort rate or a production
service-level guarantee.

## Read the measurements correctly

Process RSS and cgroup charged memory answer different questions. Previously
cached executable or library pages can remain charged to another cgroup while
appearing in this process's RSS. The harness hashes the binary outside the
measured service before launching it, which can warm those pages. This helps
explain why some cgroup peaks are below worker RSS; the native zone trial's
roughly 16 MiB cgroup peak is **not a 16 MiB process footprint**. Cgroup accounting
also includes charged page cache. Shared pages may appear in multiple RSS
figures, and independently sampled peaks must not be added as simultaneous use.

The Go DuckDB driver materializes execution before Arrow delivery. Bounded source
handoffs and a slow client do not bound join, sort or result-materialization
memory. The 128 MiB DuckDB setting is not a whole-process memory limit. A warm
run under a cgroup cap does not establish the same footprint after a cold start
or under unrelated host memory pressure.

Process and scratch sampling normally runs every 100 ms and can miss short
peaks. Host MemAvailable and steal time include unrelated activity. Three trials
per configuration provide observed variation, not confidence intervals, a soak
test, dedicated CPU capacity, source-database sizing or production tenancy/HA
acceptance. The separate Azure database and both SSH hops are part of the
measured architecture and must remain explicit in any comparison.

## Reproduce on an isolated test host

Use the scripts' `--help` for bounded arguments and private fixture requirements.
Keep source provisioning, binary compilation and timed workloads in separate
windows. Match the recorded binary and source reference hashes before comparing
results, and retain failed attempts alongside successes.

| Script | Responsibility |
| --- | --- |
| [federation_micro_benchmark.py](../scripts/federation_micro_benchmark.py) | Bounded systemd CLI trials, process/cgroup accounting and retained output artifacts |
| [federation_micro_server.py](../scripts/federation_micro_server.py) | Loopback HTTP service, limits, independent worker sampling and exact-service cleanup |
| [federation_micro_http.py](../scripts/federation_micro_http.py) | Sequential remote exports with byte/reference and final-state verification |
| [federation_micro_parallel.py](../scripts/federation_micro_parallel.py) | Synchronized remote client cohorts, bounded deadlines and separately counted failures |

Source credentials, bearer tokens, SSH keys and detailed logs belong in private
fixture files. Public evidence should contain measurements and hashes. Stop
only the recorded test services and forwards, preserving existing services and
source data.

[Final cleanup](evidence/oracle-micro-cleanup.json) stopped the owned test
services and source SSH forward, removed the exact temporary authorized key
and test reader, and deleted private fixture credentials/state. The dedicated
scratch filesystem was unmounted, its loop device detached and its image
removed. Existing Tailscale remained active. Source counts were preserved:
22,612,607 trips, one million sample rows, 265 zones and the original
100-million-row `fact_events` table. Compiled binaries, harnesses, public reports
and source datasets were retained.

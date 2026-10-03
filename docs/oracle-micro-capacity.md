# Oracle micro VM capacity

For the newer tenant-isolated worker and NATS queue campaign, see
[Kelvo worker on Oracle E2.1.Micro](node-capacity.md). That pinned baseline places
the gateway, NATS and source on Azure and reports worker-only accounting.

On `VM.Standard.E2.1.Micro`, Kelvo completed **30/30 native million-row exports at 469,800 output rows/s** with source/result LZ4. Explicitly larger limits allowed **30/30 four-million-row exports at 443,421 rows/s**, still under a **640 MiB service cap**. SQL ran on Azure; a macOS client verified complete remote exports through SSH.

The limits matter: ten simultaneous federated exports caused a service OOM (**0/10 completed**); four permits completed **12/30 attempts**, with 18 HTTP 429s. Result LZ4 slowed the separate federated CLI median, and no full-sort attempt produced a validated export. These are workload/path observations, not production capacity or availability guarantees.

## Host and topology

[Host](evidence/oracle-micro-host.json): Mumbai-region AMD EPYC 7551, two guest logical CPUs, **951.3 MiB RAM**, no swap. [Oracle documents](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm) **1/8 OCPU with bursting**, 1 GB memory, up to 50 Mbps internet/480 Mbps private same-region bandwidth. Two visible CPUs are not two dedicated cores. `CPUWeight=20` applied; no CPU quota was configured. CPU steal varied and existing Tailscale stayed active.

No source database ran on Oracle. Pinned-key SSH carried Oracle-to-Azure ClickHouse and Mac-to-Oracle client traffic. Both application HTTP hops had **no inner TLS**; this differs from the [verified-TLS remote campaign](federation-capacity.md). Source/SSH CPU and memory are outside the Kelvo service measurements.

| Setting | Sequential CLI | Ten-client services |
| --- | --- | --- |
| Service | 640 MiB memory, zero swap, 96 tasks | Same memory/swap; 256 tasks |
| Query | 128 MiB, two threads, 512 MiB temporary disk | 128 MiB, two threads, 64 MiB temporary disk |
| Scratch | Dedicated 2 GiB ext4, `nodev,nosuid,noexec` | Same owned scratch filesystem |
| Isolation | Explicit sandbox launcher | Explicit sandbox launcher |

Observed `NoNewPrivs`/seccomp flags alone do not verify the entire Landlock policy.

## Data and builds

[Provenance](evidence/federation-capacity-provenance.json): **22,612,607 real trips**, one-million-row projection, **265 zones**, stable IDs and separately derived half-even `fare_cents`. Original amounts/anomalies remain. [Source preflight](evidence/oracle-micro-source-preflight.json) verified read allowed/write denied.

| Campaign | Revision / binary SHA-256 |
| --- | --- |
| Baseline CLI/sort/HTTP | `e955597` / `51ab67046f31debe0a9358c24d5ab732380796ddcb2eb0bd352c4c9220e61fff` |
| Source-codec comparison | [Build](evidence/oracle-micro-lz4-build.json): `81851d5`, 181 verified files, `duckdb_arrow,duckbridge`, trimpath/stripped / `5331decbbba421b0d7bc4ce4b8e9ca294d7f714b01a7a6ea5947613f32983fb3` |
| Result-codec/larger exports | [Build](evidence/oracle-micro-result-build.json): runtime `b1a36c0`, 184 files checked against `8df16c0` / `fefdba4eaa4b2ac99eaf2878d30cfe80ddeacaf1df4fac1c52182f9ca944d6ac` |

Do not attribute RSS differences between these three binaries to compression. Canonical validation checks exact values/NULLs independently of batching/integer width; byte hashes/EOS are additional checks.

## Measured CLI results — 2026-10-02

[15/15 baseline trials](evidence/oracle-micro-queries.json) passed [independent decoding](evidence/oracle-micro-values-regular.json) against [native references](evidence/federation-capacity-references.json). Each configuration ran three sequential trials with warm/uncontrolled caches. Seconds measure process start to exit, including wrapper/fsync; reference work, hashing, decoding and orchestration polling are outside. Memory/scratch columns show ranges of per-trial peaks; no OOM was observed.

| Workload | Output rows | Seconds, trials 1 / 2 / 3 | Median seconds | Sampled worker RSS | Cgroup memory peak | Allocated scratch peak |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| Native zone join and aggregate | 8 | 1.334 / 1.454 / 1.455 | 1.454 | 77.7–77.8 MiB | 15.6–16.1 MiB | 0 MiB |
| Native ordered million-row export | 1,000,000 | 6.597 / 4.319 / 4.331 | 4.331 | 81.0–82.2 MiB | 42.0–42.7 MiB | 0 MiB |
| Federated ordered million-row export | 1,000,000 | 8.404 / 8.617 / 8.539 | 8.539 | 166.0–167.1 MiB | 108.1–116.4 MiB | 0 MiB |
| Federated fact + zones aggregate | 8 | 62.767 / 61.628 / 64.459 | 62.767 | 267.8–269.0 MiB | 288.7–327.8 MiB | 243.8–252.0 MiB |
| Federated fact + million-row join | 253 | 69.999 / 71.962 / 72.047 | 71.962 | 149.6–150.1 MiB | 69.7–73.1 MiB | 0 MiB |

| Federated workload | Rows fetched from source | Logical Arrow bytes fetched | Output rows |
| --- | --- | ---: | ---: |
| Ordered million | 1,000,000 sample rows | 20,000,000 | 1,000,000 |
| Fact + zones | 22,612,607 trips + 265 zones | 271,351,284 + 4,135 | 8 |
| Fact + million-row join | 22,612,607 trips + 1,000,000 sample rows | 361,801,712 + 12,000,000 | 253 |

Fetched rows are adapter deliveries, not storage rows examined; these are logical Arrow bytes, not compressed traffic. Native/federated million-row bodies were 20,006,992 / 20,117,616 bytes despite identical values. Native SQL executes on Azure; federation computes on Oracle. [Earlier fixture failure](evidence/oracle-micro-fixture-failure.json): all 15 attempts reported `query_process_failed`; no more specific cause is established.

## Remote HTTP exports

[6/6 exports](evidence/oracle-micro-http.json) matched independently decoded CLI reference bytes, Arrow EOS and final API `succeeded` state. The Mac streamed/hashed/discarded responses without local decoding. Times include submission, execution, delivery and hashing over SSH; final-state lookup is separate, with a new HTTP connection per request.

| Mode | Complete export seconds, trials 1 / 2 / 3 | Median seconds | First 64 KiB | Output rows/s | Response body MB/s |
| --- | --- | ---: | ---: | ---: | ---: |
| Native | 5.469 / 5.601 / 5.108 | 5.469 | 1.910–2.085 s | 178,528–195,755 | 3.572–3.916 |
| Federated | 10.487 / 9.727 / 10.486 | 10.486 | 6.885–7.368 s | 95,354–102,804 | 1.918–2.068 |

Health calls took 0.061–0.063 s including server overhead, not pure RTT. Decimal MB/s excludes HTTP/SSH framing. [Service resources](evidence/oracle-micro-http-resources.json): six workers observed, maximum one at once, largest worker 151.1 MiB RSS, simultaneous coordinator+worker 211.6 MiB RSS, charged peak 97.7 MiB. No observed scratch/memory/pids events; stopped cgroup empty. These cover the monitoring interval, not individual export peaks.

## Full-sort failures

| Attempt | Outcome | Pressure/observation |
| --- | --- | --- |
| [Initial 22.6M sort](evidence/oracle-micro-limit.json) | No verified export/exit. At about 129.9 s: `systemd_command_timed_out`, final status unavailable; owned unit then stopped. | Charged memory 624.1 MiB; worker RSS 596.2 MiB; scratch 235.4 MiB; host available 85.3 MiB; sampling gap 1.398 s. Last max/oom/oom_kill counters zero: not proof of OOM. |
| [Retry](evidence/oracle-micro-limit-retry.json) | Exit 1, [verified deadline error](evidence/oracle-micro-limit-diagnostic.json). 240 s query deadline; actual process 268.790 s, harness 269.388 s. | Nine status-command timeouts recovered; charged peak 628.3 MiB, max/oom/oom_kill/oom_group_kill zero. This did not demonstrate timely cancellation at 240 s. |

[Initial validation](evidence/oracle-micro-values-limit.json) has an empty result list: `all_passed` is not successful validation. [Retry validation](evidence/oracle-micro-values-limit-retry.json) has no output and `all_passed=false`. Neither attempt establishes a minimum memory requirement; later harness fixes do not repair missing historical evidence.

## Ten-client baseline exports

[Native: 30/30](evidence/oracle-micro-native10.json), with [ten overlapping workers verified](evidence/oracle-micro-native10-resources.json). Ten execution permits, 32 retained handles; service/query settings above.

| Native cohort | Verified exports | Cohort seconds | Aggregate output rows/s | Export p50 | Export p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 10/10 | 37.864 | 264,104 | 37.310 s | 37.796 s |
| 2 | 10/10 | 38.070 | 262,672 | 37.946 s | 38.004 s |
| 3 | 10/10 | 37.944 | 263,545 | 37.831 s | 37.875 s |

Totals: 30 million rows, 600,209,760 body bytes, 113.878 summed cohort seconds: **263,439 rows/s**, **5.271 MB/s**; overall p50/p95 37.796/38.004 s. No rejection/disconnect/failure or memory/pids event; healthy after each cohort, stopped cgroup empty. This is remote native export, not ten local analytical computations.

The observed 5.271 MB/s is approximately 42.2 Mbps of response bodies, but that alone does not prove network saturation. At about 20 bytes/row, one million exported rows/s needs about 160 Mbps before HTTP/SSH overhead.

[Federation with ten permits](evidence/oracle-micro-fed10.json): **0/10** after ten accepted POSTs/overlapping workers. Every result connection failed before headers/body; no 429 or successful 200. The 45.935 s cohort includes unsuccessful cancellation, not successful-export latency. No further ten-worker cohorts ran. [Server report](evidence/oracle-micro-fed10-resources.json) proves service-wide OOM: 640 MiB, max=700, oom=12, oom_kill=7, oom_group_kill=1, `Result=oom-kill`, signal 9. `OOMPolicy=kill` stopped coordinator/workers; host available reached 49.7 MiB, largest sampling gap 0.700 s; cleanup verified empty cgroup.

[Four permits](evidence/oracle-micro-fed4.json): **12/30 successful attempts**. All POSTs created handles, but each cohort delivered four validated exports and six 429 `RESOURCE_EXHAUSTED` responses. The 18 rejected exports were counted as failures and cancelled, without retry/queueing; no failure followed HTTP 200.

| Four-permit federated cohort | Verified exports / attempts | HTTP 429 | Cohort seconds | Aggregate output rows/s | Successful export p50 | Successful export p95 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 4/10 | 6 | 37.543 | 106,545 | 37.065 s | 37.469 s |
| 2 | 4/10 | 6 | 33.024 | 121,124 | 32.726 s | 32.956 s |
| 3 | 4/10 | 6 | 36.597 | 109,297 | 35.994 s | 36.496 s |

Totals: 12 million verified rows / 107.164 s = **111,978 rows/s**, **2.253 MB/s**. [Resources](evidence/oracle-micro-fed4-resources.json) confirmed four workers, initial/final limits, no control timeout/OOM/memory/pids event, healthy service and empty stopped cgroup. Intermediate permit counts were not measured.

| Ten-client mode | Largest individual worker RSS | Simultaneously sampled worker RSS sum | Coordinator + workers RSS sum | Cgroup charged memory peak |
| --- | ---: | ---: | ---: | ---: |
| Native, three cohorts | 63.9 MiB | 621.3 MiB | 693.6 MiB | 141.8 MiB |
| Federated, ten permits, one failed cohort | 114.1 MiB | 1,088.3 MiB | 1,146.2 MiB | 640.0 MiB |
| Federated, four permits, three cohorts | 146.0 MiB | 555.1 MiB | 608.8 MiB | 431.9 MiB |

These are service-interval peaks including idle time. Worker sums are simultaneous, but columns may peak at different times. No scratch was observed. Shared pages can make RSS sums exceed physical memory or the cgroup cap; source/SSH usage is excluded.

The harness synchronizes handle/results barriers and does not retry. Successful-export p50/p95 use linear interpolation; aggregate rows/s includes verification/cleanup but excludes inter-cohort health checks. Standalone POST reserves a handle; GET results takes a nonwaiting execution permit. Full capacity returns 429 with the handle unclaimed. Permits last through teardown; completed/cancelled handles remain for the five-minute TTL and count toward 32. HTTP 200 alone is not complete delivery.

## Source-only LZ4 comparison

[None](evidence/oracle-micro-none.json) / [LZ4](evidence/oracle-micro-lz4.json): nine trials each on the same source-codec build. Only `options.arrow_compression: lz4_frame` changed; local worker IPC/final files stayed uncompressed. Limits, SQL and rows were identical. All 18 outputs passed canonical [none](evidence/oracle-micro-values-none.json) / [LZ4](evidence/oracle-micro-values-lz4.json) validation; each query's final byte hash matched across codecs and all six trials.

| Workload | Source codec | Source response bytes | Median CLI seconds (range) | Median service CPU seconds (range) | Sampled worker RSS range |
| --- | --- | ---: | ---: | ---: | ---: |
| Native ordered million | none | 20,256,960 | 5.242 (4.524–5.734) | 0.434 (0.374–0.511) | 49.2–50.2 MiB |
| Native ordered million | LZ4 | 8,738,872 | 2.753 (2.722–3.506) | 0.434 (0.432–0.471) | 59.6–64.8 MiB |
| Federated ordered million | none | 20,256,480 | 8.643 (8.556–8.935) | 1.194 (1.109–1.265) | 133.6–135.7 MiB |
| Federated ordered million | LZ4 | 8,734,712 | 6.007 (5.615–6.186) | 1.289 (1.068–1.331) | 147.3–152.6 MiB |
| Federated fact + zones | none | 277,104,736 | 72.671 (63.149–81.962) | 32.299 (11.237–34.489) | 232.7–283.6 MiB |
| Federated fact + zones | LZ4 | 106,741,528 | 49.115 (44.121–50.672) | 31.436 (30.802–34.700) | 257.5–272.0 MiB |

Source bodies shrank 56.9%, 56.9%, 61.5%; figures exclude framing/discovery. The zone query still fetched 22,612,607 trips + 265 zones for eight outputs. Whole-service CPU excludes Azure compression and SSH; million-row RSS increased with LZ4. No OOM occurred.

None ran first, then LZ4, without randomization/cache control. Zone-run host steal was 7.9–21.0% / 33.9–42.9%; uncompressed CPU varied 11.237–34.489 s. These observations do not establish a general compression speedup or CPU cost.

## Result compression and byte limits

[Result compression](usage.md#opt-in-result-compression) defaults to `none`; `--result-compression lz4_frame` covers native/federated/accelerated public Arrow buffers. Cluster nodes encode once under `policy.limits.result_compression`; gateways relay. Local worker IPC stays uncompressed. Clients need Arrow LZ4 support; this is not HTTP `Content-Encoding`.

`--max-bytes` independently caps decoded Arrow bytes and encoded result bytes at the same value. **80 MiB decoded → 20 MiB encoded still fails a 32 MiB cap.** Explicit row/byte changes do not raise service RAM or DuckDB settings. [Separate cluster acceptance](evidence/cluster-compression-acceptance.json) validated eight batches/16,384 rows, exact integer/Unicode/NULL values, EOS and 136,768 encoded node/client bytes; that is not a micro throughput result.

## CLI result-compression comparison

[None](evidence/oracle-micro-result-none.json) / [LZ4](evidence/oracle-micro-result-lz4.json): source LZ4 enabled in both; source bodies stayed 8,738,872 native / 8,734,712 federated bytes. Each codec ran three native + three federated trials under a 32 MiB result cap, 128 MiB query setting, two threads and 640 MiB service cap. All 12 passed canonical [none](evidence/oracle-micro-values-result-none.json) / [LZ4](evidence/oracle-micro-values-result-lz4.json) checks.

| Workload | Result codec | Encoded output bytes | Median CLI seconds (range) | Median service CPU seconds (range) | Sampled coordinator + worker RSS range |
| --- | --- | ---: | ---: | ---: | ---: |
| Native ordered million | none | 20,006,992 | 2.840 (2.346–3.539) | 0.406 (0.385–0.527) | 115.8–121.4 MiB |
| Native ordered million | LZ4 | 8,759,064 | 2.575 (2.094–3.091) | 0.735 (0.603–0.955) | 113.4–119.2 MiB |
| Federated ordered million | none | 20,117,616 | 5.921 (5.632–6.350) | 1.352 (1.247–1.960) | 196.3–203.1 MiB |
| Federated ordered million | LZ4 | 9,289,424 | 6.485 (5.885–7.223) | 2.186 (1.762–2.242) | 201.4–203.8 MiB |

Bytes fell 56.2% native / 53.8% federated, but federated CLI median was **9.5% slower** and CPU medians rose. These are local fsynced exports; none ran first, caches/scheduling uncontrolled. Coordinator+worker RSS is simultaneous; no OOM/scratch observed. Compression did not consistently lower latency or memory.

## Explicitly larger native export

[3/3 four-million-row CLI exports](evidence/oracle-micro-four-million.json), source/result LZ4, explicit `--max-rows 4000000 --max-bytes 134217728` (128 MiB). Service cap stayed 640 MiB, query 128 MiB, two threads, temporary disk 512 MiB. Compression itself raised no limit.

| Trial | Output rows | CLI seconds | Encoded output bytes | Sampled worker RSS | Cgroup memory peak |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 4,000,000 | 8.563 | 35,065,408 | 73.1 MiB | 81.6 MiB |
| 2 | 4,000,000 | 7.691 | 35,065,408 | 74.3 MiB | 89.6 MiB |
| 3 | 4,000,000 | 8.073 | 35,065,408 | 72.8 MiB | 79.4 MiB |

[Reference](evidence/oracle-micro-four-million-reference.json): first four million physical January rows, `source_month=201901 AND source_row < 4000000`, ordered by storage key `(source_month, source_row)`, projecting `trip_id`, `pickup_zone_id`, `fare_cents`. [All decoded outputs](evidence/oracle-micro-values-four-million.json) matched exact values/NULLs; Kelvo byte hashes matched each other, not the differently encoded direct reference.

Each output: 80,000,000 logical bytes / 35,065,408 encoded; source body 35,033,336 bytes. Median 8.073 s; combined RSS 122.6–123.7 MiB; no OOM/scratch. This is Azure native SQL, not a successful local four-million-row federated sort. [Earlier reference preparation](evidence/oracle-micro-four-million-reference-failure.json) using global `ORDER BY trip_id LIMIT 4000000` failed Azure ClickHouse's 512 MiB limit; retain it separately from timed trials.

## Ten-client remote result-compression comparison

[None](evidence/oracle-micro-result-native10-none.json) / [LZ4](evidence/oracle-micro-result-native10-lz4.json) each passed **30/30** exact-byte/EOS/final-state checks on the final build. Source LZ4 stayed on. Both used ten workers, 32 handles, 32 MiB result cap and unchanged ten-client service/query limits.

| Result codec | Verified exports | Cohort seconds, 1 / 2 / 3 | Aggregate output rows/s | Export p50 | Export p95 | Response-body MB/s |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| none | 30/30 | 42.236 / 40.179 / 41.130 | 242,827 | 40.382 s | 42.166 s | 4.858 |
| LZ4 | 30/30 | 25.212 / 18.123 / 20.522 | 469,800 | 20.335 s | 25.118 s | 4.115 |

Each profile exported 30 million verified rows. Total bodies: 600,209,760 / 262,771,920 bytes; summed cohorts: 123.545 / 63.857 s. LZ4 delivered **1.93× observed output-row throughput** with fewer bytes/row; body MB/s excludes decoded data/framing. No failures/rejections/disconnects. None ran first; this benefit is specific to this remote native workload, unlike slower federated CLI results above.

[None resources](evidence/oracle-micro-result-native10-none-resources.json) / [LZ4 resources](evidence/oracle-micro-result-native10-lz4-resources.json) observed all 30 workers/profile, ten at once, valid initial/final states, no control timeout/OOM/memory/pids/scratch event, and empty stopped cgroups.

| Result codec | Sampled-interval cgroup CPU | Largest individual worker RSS | Simultaneous worker RSS sum | Coordinator + workers RSS sum | Cgroup charged memory peak |
| --- | ---: | ---: | ---: | ---: | ---: |
| none | 15.378 s | 69.7 MiB | 645.3 MiB | 691.0 MiB | 377.3 MiB |
| LZ4 | 25.673 s | 74.3 MiB | 662.8 MiB | 717.5 MiB | 295.2 MiB |

CPU is the first-to-last sampled cgroup counter delta, including readiness/idle and excluding Azure/SSH. LZ4 used more CPU despite less elapsed time. Lower charged memory coexisted with higher RSS sums; shared-page/cache ownership prevents equating them.

## Ten-client four-million-row exports

[30/30 validated exports](evidence/oracle-micro-result-native10-four-million.json), source/result LZ4, four-million-row/128 MiB result limits. Ten permits, 32 handles, 640 MiB service, 128 MiB query, two threads, 64 MiB temp/query, 256 tasks and zero swap remained unchanged.

| Cohort | Verified exports | Cohort seconds | Aggregate output rows/s | Export p50 | Export p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 10/10 | 90.550 | 441,743 | 90.474 s | 90.477 s |
| 2 | 10/10 | 67.812 | 589,866 | 67.709 s | 67.740 s |
| 3 | 10/10 | 112.261 | 356,314 | 112.155 s | 112.187 s |

Totals: **120 million repeated output rows**, 1,051,962,240 encoded bytes, 270.623 cohort seconds: **443,421 rows/s**, **3.887 MB/s**, p50/p95 90.474/112.173 s. These are 30 copies of one four-million-row selection, not 120 million distinct source records. No 429/disconnect/failure; healthy after each cohort.

[Resources](evidence/oracle-micro-result-native10-four-million-resources.json): 30 workers observed, ten maximum; largest worker RSS 74.4 MiB, simultaneous worker sum 713.9 MiB, coordinator+workers 744.8 MiB, charged peak 423.4 MiB, minimum host available 200.9 MiB. CPU delta 93.046 s excludes Azure/SSH. No control timeout/OOM/memory/pids/scratch event; stopped cgroup empty. Sampling gap 1.082 s, host steal 36.7%; 67.812–112.261 s cohort variation is not a stable service-level guarantee.

## Interpret and reproduce

RSS counts shared pages; charged cgroup memory includes owned cache and may omit executable pages charged elsewhere. Pre-trial binary hashing can warm those pages. A roughly 16 MiB charged native-zone peak is **not a 16 MiB process footprint**. Never sum independent peaks or equate RSS with unique physical allocation.

DuckDB materializes before Arrow delivery. A 128 MiB engine setting, bounded source handoff or slow consumer does not bound whole-process join/sort/result memory. Warm trials do not establish cold-start fit. Normal sampling is 100 ms and can miss peaks; host available/steal include unrelated work. Three trials are not confidence intervals, a soak test, source sizing or HA certification.

| Script | Responsibility |
| --- | --- |
| [federation_micro_benchmark.py](../scripts/federation_micro_benchmark.py) | Bounded systemd CLI trials, process/cgroup accounting and retained output artifacts |
| [federation_micro_server.py](../scripts/federation_micro_server.py) | Loopback HTTP service, limits, independent worker sampling and exact-service cleanup |
| [federation_micro_http.py](../scripts/federation_micro_http.py) | Sequential remote exports with byte/reference and final-state verification |
| [federation_micro_parallel.py](../scripts/federation_micro_parallel.py) | Synchronized remote client cohorts, bounded deadlines and separately counted failures |

```sh
python3 scripts/federation_micro_benchmark.py --help
python3 scripts/federation_micro_server.py --help
python3 scripts/federation_micro_parallel.py --help
```

Use isolated Linux fixtures, matched build/reference hashes, private source/token/SSH files and separate build/import/timing windows. Keep failed attempts; publish only sanitized measurements/hashes. Stop recorded services/forwards only.

[Final cleanup](evidence/oracle-micro-cleanup.json) removed owned services, source forward, temporary authorized key/reader, private credentials, scratch mount/loop/image. Tailscale stayed active. Preserved: 22,612,607 trips, million-row sample, 265 zones, original 100-million-row `fact_events`, binaries, harnesses and reports.

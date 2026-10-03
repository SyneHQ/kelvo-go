# Federation capacity

Real NYC TLC data exercises ClickHouse, PostgreSQL and MySQL adapters. **14/14 capacity checks and 224/224 sustained cluster jobs passed.** These are workload-specific sizing measurements, not a production multi-tenant, HA or multi-host scaling certification.

## Fixture and validation

[Provenance](evidence/federation-capacity-provenance.json): **22,612,607 January–March 2019 trips**, **265 zones**, and the first million physical January rows copied to all three databases. Stable `trip_id` combines source month/row ordinal. Original Float64 amounts/anomalies remain; half-even `fare_cents` is derived, not TLC exact-decimal money.

Every successful export must match [native ClickHouse references](evidence/federation-capacity-references.json) by exact values and NULL validity, independent of batching/integer width, and include Arrow EOS. Failures must not publish completed files.

## CLI results — 2026-10-02

[Capacity report](evidence/federation-capacity.json), [binary](evidence/federation-capacity-build.json): one timed run per configuration, warm/uncontrolled caches, four Xeon Platinum 8573C logical CPUs, approximately 31.3 GiB RAM. All 11 CLI results and both HTTP exports matched references; the remaining check covered cancellation/recovery.

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

Three-adapter runs used four threads/active scan slots; others used two. Every fact scan fetched all 22,612,607 rows. Million-row joins fetched another million and returned 253 groups; zone/three-adapter joins returned eight. No physical plan/build side was captured.

Full sorts returned 22,612,607 rows and 454,902,480 bytes: **2.886–2.922 million output rows/s**. Source bodies were 458,044,512 bytes excluding discovery/headers; coordinator RSS was about 61 MiB. Worker RSS of **717–834 MiB** means the 128/256 MiB DuckDB settings did not prove fit in 128, 256 or 512 MiB containers or establish a minimum budget.

| Transport/control check | Observed result |
| --- | --- |
| ClickHouse relay: 50 ms application delay, shared 10 MiB/s body limit | Zone join 28.635 s; PostgreSQL join 36.275 s. PostgreSQL/MySQL stayed on their ordinary same-VM path. |
| Ordered million-row loopback HTTP export | 0.247 s unpaced; 19.288 s with 1 MiB/s reader; worker RSS about 156/161 MiB. |
| Cancel active stream, then real zone-count query | Slot recovered in 0.093 s. One transient HTTP 429 while teardown retained the permit; bounded retry succeeded. Cancellation acknowledgment is not cleanup completion. |

## Sustained tenant concurrency

[224 exact-validated jobs, zero errors](evidence/federation-cluster-capacity.json): two tenants, two gateways, three nodes and three NATS brokers on one four-CPU VM. Each level used one 120-second submission window. Alternating PostgreSQL/MySQL joins fetched **5,065,223,968 fact rows** across the suite; those are fetched, not exported rows.

| Concurrent clients | Successful jobs including drain | Completions/s within window | p50 latency | p95 latency |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 58/58 | 0.475 | 2.430 s | 2.691 s |
| 2 | 80/80 | 0.650 | 3.053 s | 3.944 s |
| 4 | 86/86 | 0.683 | 5.634 s | 7.590 s |

Rates count 57/78/82 in-window completions; remaining jobs finished during drain. Tenant counts were 29/29, 40/40 and 43/43. Latencies/closed-loop pacing include verification and new HTTP/TLS setup. No query workers remained; maximum overlapping workers were 1/2/4.

Largest worker RSS: **449.7 MiB**; simultaneous worker sum at four clients: **1.34 GiB**. Source databases, nodes, gateways and brokers are separate. Each query had 256 MiB DuckDB/two threads; tenant A used two one-slot nodes, tenant B one two-slot node. Admission stayed at 16 nonterminal jobs and four execution slots throughout.

At four clients, approximately 3.9 sampled CPU cores were busy. Throughput barely improved over two clients while p95 nearly doubled. This co-located workload had little headroom; one short interval per level is not a soak test or cross-machine scaling measurement.

[Separate preflight](evidence/federation-capacity-preflight.json) checked tenant markers, cross-gateway retrieval, foreign-handle denial with forged headers, and 429 at tenant A's 16-handle limit while tenant B succeeded. These negatives are excluded from query rates. Identical public data alone does not prove isolation; [container/network acceptance](evidence/federation-capacity-container.json) is separate from the host-process Landlock/mTLS load topology.

## Actual remote-client transfer

[Three remote trials](evidence/federation-wan.json) delivered the same ordered million-row result (**20,117,616 bytes** each) over SSH forwarding with inner verified TLS. Each complete body matched the SHA-256 of an independently decoded VM reference. The macOS client retained only a rolling hash/count/EOS check, without local Arrow decoding or result storage.

| Trial | Complete export | First 64 KiB | Output rows/s | Response body MB/s |
| --- | ---: | ---: | ---: | ---: |
| 1 | 8.496 s | 2.250 s | 117,709 | 2.368 |
| 2 | 5.364 s | 1.812 s | 186,435 | 3.751 |
| 3 | 5.765 s | 1.799 s | 173,462 | 3.490 |

Times include SSH, network, SQL and new HTTPS connection setup; final query-state verification follows the export interval. Health requests took 0.834–0.843 s including TLS/server overhead, not pure RTT. This is not direct HTTPS or engine-only capacity. The initial fixture CA failed strict macOS OpenSSL checks for missing key usage; reissuing its certificate with the same key fixed it without disabling verification or rotating a service key.

## Measurement limits

Output rows/s includes startup, execution and complete delivery; CLI also includes fsync. Reference calculation/value validation are outside timing. Fetched rows are not database rows examined, and a small aggregate can fetch millions.

DuckDB materializes before Arrow delivery. Its memory setting and bounded source handoffs do not bound whole-process joins/sorts/results. Use deployment memory/disk controls. Simultaneous RSS may count shared pages twice; sampled RSS/scratch can miss short peaks. Never sum independent peaks as simultaneous use.

The relay models application delay/body bandwidth only, not packet loss, jitter, geographic routing or TCP. Caches were warm/uncontrolled, and sources/brokers shared the VM. These trials do not certify multi-zone HA or production tenancy.

## Reproduce

Use a dedicated Linux amd64 test host with Landlock ABI 3+, Docker, Python/PyArrow and a bridge-enabled image. Provision private scratch, existing `kelvo-clickhouse`, and cached official PostgreSQL/MySQL images. Keep imports/builds separate from timing; preserve existing `kelvo_bench.fact_events`.

| Script | Responsibility |
| --- | --- |
| [federation_relational_acceptance.py](../scripts/federation_relational_acceptance.py) | Private relational fixtures, read-only TLS users, exact values, joins, denial and cancellation checks |
| [federation_capacity.py](../scripts/federation_capacity.py) | TLC imports, native references, CLI joins/sorts, relay and slow-consumer trials |
| [federation_capacity_cluster.py](../scripts/federation_capacity_cluster.py) | Owned NATS/app lifecycle, explicit tenant source environments and resource settings |
| [federation_cluster_load.py](../scripts/federation_cluster_load.py) | Sustained cluster query/concurrency trials |
| [federation_wan_client.py](../scripts/federation_wan_client.py) | Remote export verification using a VM-decoded reference; no local result storage |

```sh
python3 scripts/federation_capacity.py --help
python3 scripts/federation_capacity_cluster.py --help
python3 scripts/federation_cluster_load.py --help
```

Keep private fixture manifests until dependent tests finish; credentials/CA material stay private. Publish measurements/hashes and retain failures. Stop only recorded fixture processes.

[Cluster cleanup](evidence/federation-capacity-cleanup.json) and [relational cleanup](evidence/federation-relational-cleanup.json) removed disposable services/fixtures while preserving ClickHouse, all 22,612,607 trips, million-row projection, 265 zones, original 100-million-row fixture and dataset files. Reruns need fresh relational fixtures and source manifests.

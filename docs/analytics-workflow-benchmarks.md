# Analytical workflow benchmarks

**84/84 measured attempts passed:** four taxi analyses, 28 host/engine configurations, three repetitions each. Every output matched a direct ClickHouse reference and independently reconstructed expectations. See the [reconciled ledger](evidence/analytics-workflow-summary.json) and [decoded validation](evidence/analytics-validation-measured.json).

On identical Azure Parquet files, Kelvo had the lowest complete-command median for three workflows; Polars won route economics. Hotspot Kelvo/DuckDB ranges overlap. These timings include startup/imports and do not isolate SQL execution speed. Live Azure hotspot federation reached its **640 MiB cap**; the campaign establishes no memory headroom or production capacity guarantee.

## Inputs and comparison boundaries

The [dataset](evidence/analytics-workflow-dataset.json) contains **22,612,607 January–March 2019 NYC Yellow Taxi trips** and **265 official zones**. Identical normalized Parquet files contain 372,839,618 and 4,964 bytes. Preparation/refresh is outside measurement; filtering happens inside each query.

| Workflow | Contract | Validated output |
| --- | --- | --- |
| Daily rolling KPIs | `daily_rolling_kpis`: daily aggregation, calendar fill and trailing seven-day windows | 90 days |
| Borough/hour hotspots | `borough_hour_hotspots`: zone aggregates and top-three ranking within each borough/hour | 31,843 rows |
| Route economics | `route_economics`: valid-distance/amount cohort, at least 100 trips per route, top ten per pickup borough | 53 routes |
| Monthly zone momentum | `monthly_zone_momentum`: pickup-month aggregates, previous-month comparison and ranking | 530 rows |

The [workflow contract](../benchmarks/ride-hailing/workflows.yml) and [SQL guide](cte-workflow-plan.md) define predicates, joins, windows and ties. Audit counts: 800 out-of-quarter timestamps (572 before, 228 after), 22,611,807 eligible trips, and 873 source-file/pickup-month mismatches. Disjoint route exclusions remove 28,421 negative amounts, 153,625 nonpositive distances and 73 distances above 100, leaving 22,429,688 usable known-pair trips. Of 7,126 qualifying routes, ranking returns 53; eight borough labels do not guarantee 80 results.

| Panel | Execution | Measured boundary |
| --- | --- | --- |
| A: live native | Kelvo on Azure or Oracle; analytical SQL executes in Azure ClickHouse | Local client/gateway process, source execution wait, source transport, Arrow output and persistence. Source database CPU and memory are outside the local service cgroup. |
| B: live federation | Kelvo/DuckDB on Azure or Oracle; scans through the ClickHouse Arrow adapter | Local analytical work, projected/filtered source transfer and result delivery. Actual fetched rows and bytes are shown below. |
| C: same Parquet files | Kelvo, direct DuckDB and Polars on Azure | Fresh process, actual local Parquet reads, query work, conversion to Arrow, serialization and durable output. |

Only panel C compares libraries on the same host/files. Oracle native SQL still executes on Azure. The four direct ClickHouse references are correctness preparation, not a timed performance baseline.

## Same-file results: Azure

Each configuration passed 3/3 attempts. Seconds measure systemd process start to exit: wrapper, imports, setup, Arrow serialization and fsync included; orchestration polling excluded. Memory cells are the largest per-trial peak, not necessarily the median-time trial. MiB = 1,048,576 bytes.

| Workflow | Engine | Returned rows | Process seconds: median (min–max) | Peak sampled combined RSS, MiB | Peak charged cgroup memory, MiB |
| --- | --- | ---: | ---: | ---: | ---: |
| Daily rolling KPIs | Kelvo | 90 | 0.484 (0.479–0.613) | 182.0 | 87.8 |
| Daily rolling KPIs | DuckDB | 90 | 0.634 (0.634–0.650) | 162.9 | 92.5 |
| Daily rolling KPIs | Polars | 90 | 0.636 (0.634–0.639) | 322.3 | 123.5 |
| Borough/hour hotspots | Kelvo | 31,843 | 1.035 (0.912–1.286) | 278.4 | 184.1 |
| Borough/hour hotspots | DuckDB | 31,843 | 1.052 (1.049–1.054) | 272.4 | 197.0 |
| Borough/hour hotspots | Polars | 31,843 | 1.464 (1.453–1.485) | 558.0 | 486.3 |
| Route economics | Kelvo | 53 | 1.497 (1.456–1.510) | 225.0 | 132.9 |
| Route economics | DuckDB | 53 | 1.816 (1.776–1.947) | 215.5 | 144.0 |
| Route economics | Polars | 53 | 1.064 (1.064–1.090) | 651.8 | 362.9 |
| Monthly zone momentum | Kelvo | 530 | 0.606 (0.503–0.611) | 188.0 | 124.7 |
| Monthly zone momentum | DuckDB | 530 | 0.691 (0.652–0.718) | 206.3 | 132.2 |
| Monthly zone momentum | Polars | 530 | 0.749 (0.746–0.760) | 408.5 | 185.8 |

## Live results: Azure and Oracle

Each configuration passed 3/3 attempts. Oracle uses pinned-key SSH forwarding to Azure ClickHouse. Source database and SSH CPU/RAM sit outside the measured local cgroup. Work placement, pushdown and network differ, so these are separate execution paths.

| Host | Workflow | Kelvo mode | Returned rows | Process seconds: median (min–max) | Peak sampled combined RSS, MiB | Peak charged cgroup memory, MiB |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Azure | Daily rolling KPIs | Native | 90 | 0.627 (0.601–0.713) | 120.0 | 42.6 |
| Azure | Daily rolling KPIs | Federated | 90 | 1.508 (1.503–1.576) | 190.2 | 100.7 |
| Azure | Borough/hour hotspots | Native | 31,843 | 2.171 (2.158–2.225) | 119.8 | 53.0 |
| Azure | Borough/hour hotspots | Federated | 31,843 | 4.096 (3.911–4.186) | 429.4 | 640.0 |
| Azure | Route economics | Native | 53 | 1.822 (1.736–2.381) | 120.1 | 42.7 |
| Azure | Route economics | Federated | 53 | 3.180 (3.040–3.219) | 204.4 | 116.9 |
| Azure | Monthly zone momentum | Native | 530 | 0.911 (0.883–0.994) | 120.5 | 44.2 |
| Azure | Monthly zone momentum | Federated | 530 | 1.861 (1.826–1.928) | 192.1 | 106.8 |
| Oracle | Daily rolling KPIs | Native | 90 | 1.480 (1.430–1.521) | 104.2 | 29.3 |
| Oracle | Daily rolling KPIs | Federated | 90 | 33.490 (33.418–45.073) | 169.3 | 72.6 |
| Oracle | Borough/hour hotspots | Native | 31,843 | 3.727 (3.590–3.912) | 104.6 | 44.9 |
| Oracle | Borough/hour hotspots | Federated | 31,843 | 42.922 (40.883–57.103) | 379.3 | 347.4 |
| Oracle | Route economics | Native | 53 | 2.358 (2.352–2.392) | 102.2 | 22.0 |
| Oracle | Route economics | Federated | 53 | 62.275 (62.246–66.823) | 177.5 | 78.7 |
| Oracle | Monthly zone momentum | Native | 530 | 1.630 (1.619–1.630) | 103.4 | 22.8 |
| Oracle | Monthly zone momentum | Federated | 530 | 40.274 (39.612–41.441) | 167.2 | 68.9 |

Azure hotspot federation hit exactly 640.0 MiB in trial 2 with 19 `memory.events.max` events, but no OOM/kill. Sampled scratch peaked at 195.7 MiB there and 217.4 MiB on Oracle; other cases sampled zero. All 84 attempts had verified exits and empty trial cgroups, with no observed OOM, OOM-kill or task-limit events. Sampling can miss transient usage.

## Source transfer

Counts below were identical in all three repetitions on both hosts. Fetched rows are adapter deliveries, not database storage rows examined; native columns report response-body bytes only.

| Workflow | Native response bytes | Federated fetched rows | Federated logical Arrow bytes | Federated response bytes |
| --- | ---: | ---: | ---: | ---: |
| Daily rolling KPIs | 4,808 | 22,611,807 | 452,236,140 | Unavailable |
| Borough/hour hotspots | 636,928 | 22,612,072 | 361,798,308 | 149,051,920 |
| Route economics | 5,408 | 22,583,916 | 813,020,688 | 342,829,640 |
| Monthly zone momentum | 23,728 | 22,612,072 | 542,692,764 | 219,291,392 |

Sum each federation entry once: it may already aggregate repeated scans. Routes include 530 zone rows from two scans; hotspot/monthly include 265. Logical Arrow, source-response and final-output bytes differ. Response counters exclude discovery and HTTP/SSH/TLS framing; never add the top-level counter again. Daily response bytes are unavailable because an unused zero-scan zone entry lacks that counter. Missing is not zero; a small aggregate result can require a large source scan.

## Python baseline stages

[Driver](../scripts/analytics_library_baseline.py) medians, seconds. Compute + Arrow includes reads, execution, conversion, serialization and fsync. Driver elapsed ends before final metrics writing/process exit; do not sum phase medians to reconstruct complete-command time.

| Workflow | Engine | Imports | Setup | Compute + Arrow | Teardown | Driver elapsed |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Daily rolling KPIs | DuckDB | 0.073 | 0.032 | 0.393 | 0.009 | 0.530 |
| Daily rolling KPIs | Polars | 0.111 | <0.001 | 0.393 | <0.001 | 0.526 |
| Borough/hour hotspots | DuckDB | 0.073 | 0.032 | 0.813 | 0.011 | 0.950 |
| Borough/hour hotspots | Polars | 0.111 | <0.001 | 1.208 | <0.001 | 1.340 |
| Route economics | DuckDB | 0.073 | 0.032 | 1.560 | 0.017 | 1.706 |
| Route economics | Polars | 0.111 | <0.001 | 0.814 | <0.001 | 0.946 |
| Monthly zone momentum | DuckDB | 0.073 | 0.032 | 0.439 | 0.011 | 0.576 |
| Monthly zone momentum | Polars | 0.111 | <0.001 | 0.502 | <0.001 | 0.635 |

DuckDB materializes before Arrow delivery. Polars requested `engine="streaming"` but may use in-memory operators. First-batch timings and engine counters retain their original scopes in the evidence.

## Resources and limitations

| Setting | Azure same-file panel | Azure / Oracle live panels |
| --- | --- | --- |
| Service memory cap | 1536 MiB | 640 MiB |
| Query timeout | 180 s | 300 s |
| Engine settings | Kelvo `--memory-mb 1024`; DuckDB `1024 MB`; Polars OOC memory/disk `1024 MB` / `2048 MB` | Native `--memory-mb 512`; federated `--memory-mb 128` |
| Common service limits | Two-thread environment; CPU quota 200%, 100,000 µs period; CPUWeight 20; TasksMax 96; zero swap | Same |
| Output and disk | Arrow IPC with `lz4_frame`; 64 MiB output byte cap; nominal 2048 MiB temp setting | Same |

[Hosts](evidence/analytics-workflow-hosts.json): Azure x86_64, four visible CPUs, 31.35 GiB RAM; ClickHouse 26.9.7.9 container capped at 4 GiB/1.5 CPUs. Oracle exposes two logical CPUs and 951.3 MiB RAM, but its [entitlement is 1/8 OCPU](oracle-micro-capacity.md#host-and-topology). A 200% quota does not increase that entitlement. Host CPU steal was 1.19–37.01% on Oracle and 0% on Azure; Oracle Tailscale remained active.

[Runtime](evidence/analytics-workflow-runtime.json): Python 3.11.2, DuckDB 1.5.6, Polars 1.44.2, PyArrow 25.0.1, PyYAML 6.0.3, with binary/script/native-library hashes. Internal engine/OOC settings are separate from the service cap and do not guarantee process or disk limits.

RSS is a simultaneous process sum that can double-count shared pages. Charged cgroup memory includes charged cache and may exclude pages charged elsewhere. Pre-trial hashing warms executable pages outside the service. Neither metric establishes cold-start RAM or explains their difference as page cache alone.

Caches were uncontrolled, without drops. Trials ran sequentially in recorded rotating order after preflights. Sampling targeted 0.1 s; maximum gaps were 0.102 s (Azure snapshots), 0.103 s (Azure live), 0.200 s (Oracle), with no recorded sampling errors. The shared ext4 filesystem held 2,040,373,248 bytes for outputs, metadata and scratch; sampled soft limits were not separately reserved 2 GiB quotas.

Preparation, installation, references, hashing and value validation are outside timing. Queries use recorded wallclock buckets without timezone/DST conversion. `fare_cents` is half-even rounding of Float64 `total_amount * 100`, not original exact-decimal money or platform profit. Input rows, fetched rows and returned rows are distinct. Three repetitions establish median/range, not p95 or general throughput.

## Evidence and verification

The harness leaves `canonical_validation.state = pending_external`. Final reconciliation binds report/case/workflow/engine/trial identities and measured artifact hashes to independent decoded validation; the summarizer itself only reads JSON.

| Evidence | Scope |
| --- | --- |
| [Reconciled summary](evidence/analytics-workflow-summary.json) | All 84 planned/recorded/verified attempts, exact timing distributions, resource maxima, source counters and artifact bindings. |
| [Azure snapshots](evidence/analytics-azure-snapshot.json), [Azure live](evidence/analytics-azure-live.json), [Oracle live](evidence/analytics-oracle-live.json) | The three measured execution reports: 36, 24 and 24 attempts. |
| [Measured validation](evidence/analytics-validation-measured.json) | Exact canonical results, raw cohort audit and independently reconstructed aggregate/window/ranking expectations; all passed. |
| [Native references](evidence/analytics-native-references.json) | Four direct ClickHouse reference preparations, outside performance timing. |
| [Azure snapshot preflight](evidence/analytics-azure-snapshot-preflight.json), [Azure live preflight](evidence/analytics-azure-live-preflight.json), [Oracle preflight](evidence/analytics-oracle-live-preflight.json) | 28 separate one-trial checks, excluded from measured timing distributions. |
| [Azure preflight validation](evidence/analytics-validation-azure-preflight.json), [Oracle preflight validation](evidence/analytics-validation-oracle-preflight.json) | Separate decoded-value checks for preflights. |
| [Dataset](evidence/analytics-workflow-dataset.json), [hosts](evidence/analytics-workflow-hosts.json), [runtime](evidence/analytics-workflow-runtime.json) | Frozen inputs, source topology, versions and code/native-library fingerprints. |
| [Evidence regression checks](evidence/analytics-evidence-regression.json) | Five passing JSON checks: baseline, artifact tampering rejection, missing-attempt retention, failed resource-peak retention and conflicting-validation rejection. These did not rerun benchmarks or decode Arrow. |
| [Cleanup and preservation](evidence/analytics-workflow-cleanup.json) | 112 preflight/measured artifact pairs retained with verified hashes; temporary source access, credentials, tunnels and owned loop scratch removed. Source tables, normalized snapshots and Tailscale were retained. |

## Reproduce on Linux

1. Provision the recorded source, private reader, forwarding, catalogs, matching `bin/kelvo` and `bin/kelvo-landlock`, and bounded scratch filesystem. Use Linux cgroup v2/systemd with noninteractive sudo. Keep builds separate from timed work.
2. Under a new `BENCH_ROOT`, stage `scripts/`, `benchmarks/ride-hailing/`, private mode-0700 `private/` and `runs/`, `reports/`, `catalog-files.yml`, and `catalog-live.yml`. Install the pinned Python packages outside timing. [Prepare normalized data](../scripts/prepare_analytics_dataset.py) from the frozen provenance into a new `datasets/` directory.
3. [Prepare manifests](../scripts/prepare_analytics_manifests.py), then [native references](../scripts/analytics_native_references.py). Azure uses `machine: azure`, `profile: standard`, `panels: [snapshot, live]`; Oracle uses `machine: oracle`, `profile: oracle-micro`, `panels: [live]`. Catalogs reference environment names; credentials stay in private mode-0600 files. [Federation setup](federation.md).
4. Run one-trial preflights, resolve failures, then run separate three-trial measurement manifests. Use new report names; do not merge warmups into results.

```sh
python3 scripts/analytics_workflow_benchmark.py \
  --manifest "$BENCH_ROOT/private/manifests/snapshot-measure.json" \
  --workdir "$BENCH_ROOT/runs" --output "$BENCH_ROOT/reports/snapshot-measure.json"
```

5. Transfer Oracle reports, `measurement.json` and `result.arrow` with the `reports/`/`runs/` layout preserved. Oracle live runs need provenance, not local Parquet or analytical libraries. Prepare validation manifests and run [independent validation](../scripts/validate_analytics_workflows.py) on Azure outside timers; publish only sanitized results.

```sh
python3 scripts/validate_analytics_workflows.py \
  --manifest "$BENCH_ROOT/private/validation-measure.json" \
  --output "$BENCH_ROOT/reports/validation-measure.json" --scratch "$BENCH_ROOT/runs"
python3 scripts/summarize_analytics_workflows.py --help
```

The [summarizer](../scripts/summarize_analytics_workflows.py) requires campaign labels matching validation. Exit status 1 may still produce a useful ledger of failed/unstarted attempts. Archive Arrow files and hashes before releasing scratch; keep manifests, environment files, invocation files and detailed logs private.

# Analytical workflow benchmarks

**All 84 measured attempts passed:** four taxi analyses across 28 host/engine
configurations, with three verified repetitions each, zero missing attempts and
zero recorded failures. Every result matched both the direct ClickHouse
reference and independently reconstructed expectations from the raw fixture.
The [reconciled summary](evidence/analytics-workflow-summary.json) retains every
attempt and binds its measured Arrow hash to the
[separate value validation](evidence/analytics-validation-measured.json).

On the same Azure Parquet inputs, Kelvo had the lowest complete-command median
for daily KPIs, borough/hour hotspots and monthly momentum; Polars was fastest
for route economics. These timings include process startup and, for the Python
baselines, imports and setup. The hotspot Kelvo/DuckDB ranges overlap. The live
Azure federated hotspot case completed correctly but reached its **640 MiB
charged-memory cap**, so this campaign does not establish memory headroom.

## Inputs and comparison boundaries

The input is the full **22,612,607-row January–March 2019 NYC Yellow Taxi
fixture**, with the **265-row official zone lookup**. The same-file panel reads
identical raw normalized Parquet files: 372,839,618 bytes of trips and 4,964 bytes
of zones, with hashes in the
[dataset evidence](evidence/analytics-workflow-dataset.json). Timestamp cohorts
and other analytical exclusions happen inside the queries. Snapshot preparation
and refresh are outside measurement.

| Workflow | Contract | Validated output |
| --- | --- | --- |
| Daily rolling KPIs | `daily_rolling_kpis`: daily aggregation, calendar fill and trailing seven-day windows | 90 days |
| Borough/hour hotspots | `borough_hour_hotspots`: zone aggregates and top-three ranking within each borough/hour | 31,843 rows |
| Route economics | `route_economics`: valid-distance/amount cohort, at least 100 trips per route, top ten per pickup borough | 53 routes |
| Monthly zone momentum | `monthly_zone_momentum`: pickup-month aggregates, previous-month comparison and ranking | 530 rows |

The [workflow contract](../benchmarks/ride-hailing/workflows.yml) and
[SQL/semantics plan](cte-workflow-plan.md) define the exact predicates, joins,
windows and tie ordering. The raw audit reconciles 800 timestamps outside the
quarter (572 before, 228 after), leaving 22,611,807 eligible trips. A separate
873 eligible rows have a source-file/pickup-month mismatch. Route exclusions
are disjoint: 28,421 negative derived amounts, then 153,625 nonpositive
distances and 73 distances above 100, leaving 22,429,688 usable known-pair
trips. Of 7,126 routes meeting the 100-trip threshold, the requested ranking
returns 53 rows; eight borough labels do not imply 80 qualifying outputs.

| Panel | Execution | Measured boundary |
| --- | --- | --- |
| A: live native | Kelvo on Azure or Oracle; analytical SQL executes in Azure ClickHouse | Local client/gateway process, source execution wait, source transport, Arrow output and persistence. Source database CPU and memory are outside the local service cgroup. |
| B: live federation | Kelvo/DuckDB on Azure or Oracle; scans through the ClickHouse Arrow adapter | Local analytical work, projected/filtered source transfer and result delivery. Actual fetched rows and bytes are shown below. |
| C: same Parquet files | Kelvo, direct DuckDB and Polars on Azure | Fresh process, actual local Parquet reads, query work, conversion to Arrow, serialization and durable output. |

Only panel C compares libraries on the same host and files. Native execution on
Oracle still runs the analytical SQL on Azure. Host, network and pushdown
changes are different execution paths, not isolated library comparisons. The
four direct ClickHouse reference runs are correctness preparation only; there
is no measured direct-ClickHouse performance baseline in this campaign.

## Same-file results: Azure

All 12 configurations below verified **3/3 planned attempts**. Time is the
systemd process start-to-exit duration, including the wrapper, imports, result
serialization and fsync; status-poll/orchestration wall time is excluded. Each
memory cell is the maximum observed per-trial peak across the three recorded
attempts, not necessarily from the trial with the median time. MiB means
1,048,576 bytes.

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

The complete-command advantage includes different startup paths; it does not
establish faster isolated SQL execution. Kelvo's hotspot median was 1.035 s
versus DuckDB's 1.052 s, with overlapping ranges. Polars completed routes in
1.064 s median versus Kelvo's 1.497 s and DuckDB's 1.816 s. Three repetitions
support the displayed median and range, not a p95 or a broad performance claim.

## Live results: Azure and Oracle

All 16 configurations below verified **3/3 planned attempts**. Both hosts use
the same source database on Azure. Oracle reaches it through pinned-key SSH
forwarding. Source database CPU/RAM and SSH transport processes are outside the
local measured cgroup; native and federated results therefore represent
separate placements of computation and transfer.

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

Native medians were lower for all four workflows on both hosts. Federation
fetched roughly 22.6 million source rows per query; Oracle also experienced
variable CPU availability. These observations do not isolate network,
compression, source execution or local computation as the cause of the gap.

**Azure hotspot federation reached exactly 640.0 MiB charged memory in trial
2**, with 19 `memory.events.max` events. It passed without an OOM event or kill;
reaching the limit is still evidence of memory pressure. Its sampled scratch
allocation peaked at 195.7 MiB; the Oracle counterpart peaked at 217.4 MiB.
Other cases sampled zero scratch allocation, which does not rule out transient
files between samples. All 84 measured attempts had verified exits and empty
trial cgroups after cleanup, with no observed OOM, OOM-kill or task-limit events.

## Source transfer

These values were identical across all three repetitions on both live hosts.
They describe the adapter boundary, not database storage rows scanned. Native
fetched-row and logical-byte counters are unavailable; its column below is the
reported encoded response body. The final returned rows are in the result
tables above.

| Workflow | Native response bytes | Federated fetched rows | Federated logical Arrow bytes | Federated response bytes |
| --- | ---: | ---: | ---: | ---: |
| Daily rolling KPIs | 4,808 | 22,611,807 | 452,236,140 | Unavailable |
| Borough/hour hotspots | 636,928 | 22,612,072 | 361,798,308 | 149,051,920 |
| Route economics | 5,408 | 22,583,916 | 813,020,688 | 342,829,640 |
| Monthly zone momentum | 23,728 | 22,612,072 | 542,692,764 | 219,291,392 |

Federation totals sum each recorded entry **once**: an entry can already
aggregate repeated scan invocations, so multiplying by `scans` would overcount.
Routes include 530 dimension rows from two zone scans; hotspots and monthly
include 265 dimension rows. Logical Arrow bytes differ from encoded source
response bytes and final output bytes. Response counters exclude HTTP/SSH/TLS
framing and federation discovery; the top-level counter is never added to
federation entries. Daily's total is unavailable because its unused zero-scan
zone entry lacks the response-byte counter; missing is not zero. A small native
aggregate response does not imply a small database scan.

## Python baseline stages

The [library driver](../scripts/analytics_library_baseline.py) records the
following stage medians, in seconds, inside each fresh Python process.
Compute + Arrow includes Parquet reads, execution, result conversion, IPC
serialization and fsync. Driver elapsed ends before final metrics writing and
process exit, so it covers less than the complete process timer above.

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

Do not sum phase medians to reconstruct the median process duration. The
pinned Go DuckDB path materializes execution before Arrow delivery. Polars
requested `engine="streaming"` but can use in-memory operators where streaming
is unsupported; neither result delivery nor that setting proves every
operator streams. Per-trial first-Arrow-batch timings and engine-specific
counters remain in the evidence with their original scopes.

## Resources and limitations

| Setting | Azure same-file panel | Azure / Oracle live panels |
| --- | --- | --- |
| Service memory cap | 1536 MiB | 640 MiB |
| Query timeout | 180 s | 300 s |
| Engine settings | Kelvo `--memory-mb 1024`; DuckDB `1024 MB`; Polars OOC memory/disk `1024 MB` / `2048 MB` | Native `--memory-mb 512`; federated `--memory-mb 128` |
| Common service limits | Two-thread environment; CPU quota 200%, 100,000 µs period; CPUWeight 20; TasksMax 96; zero swap | Same |
| Output and disk | Arrow IPC with `lz4_frame`; 64 MiB output byte cap; nominal 2048 MiB temp setting | Same |

The [host record](evidence/analytics-workflow-hosts.json) reports Azure x86_64
with four visible logical CPUs and 31.35 GiB visible RAM; its ClickHouse
26.9.7.9 source container has a 4 GiB memory cap and 1.5 CPU allocation. Oracle
`VM.Standard.E2.1.Micro` exposes two logical CPUs and 951.3 MiB visible RAM,
but its [burstable entitlement is 1/8 OCPU](oracle-micro-capacity.md#host-and-topology),
not two dedicated cores. A 200% cgroup quota does not increase that entitlement.
Measured host-wide CPU steal ranged from 1.19% to 37.01% on Oracle and was 0%
on Azure. Oracle also retained its existing active Tailscale exit-node service
throughout the campaign.

Python 3.11.2, DuckDB 1.5.6, Polars 1.44.2, PyArrow 25.0.1 and PyYAML 6.0.3
were pinned. Binary, script and installed native-library hashes are in the
[runtime evidence](evidence/analytics-workflow-runtime.json). Engine memory
settings and Polars's version-specific OOC controls are separate from the
whole-service cap and are not independent process or disk guarantees.

RSS is the simultaneous sampled sum over service processes and can count shared
pages more than once. Charged cgroup memory is a separate kernel high-water
measure that includes charged page cache and can omit shared executable pages
charged elsewhere. Executable hashing before trials warms pages outside the
service. Neither metric alone establishes cold-start RAM requirements, and
their difference does not measure page cache.

Caches were uncontrolled, with no cache drops. Preflights and reference
preparation preceded the measurements. Trials ran sequentially with rotating
engine order recorded in each schedule. Sampling targeted 0.1 s; maximum gaps
were 0.102 s for Azure snapshots, 0.103 s for Azure live and 0.200 s for Oracle
live, with no recorded sampling errors. Sampling can miss short-lived peaks.
The shared ext4 benchmark filesystem had 2,040,373,248 bytes of capacity;
retained outputs, metadata and scratch shared it. Per-trial scratch limits were
sampled soft limits, not separately reserved 2 GiB quotas.

Fixture preparation, package installation, reference generation, hashing and
canonical validation were outside the timed interval. The queries use recorded
wallclock buckets, with no timezone/DST reinterpretation. Derived `fare_cents`
uses half-even rounding of binary Float64 `total_amount * 100`, preserving the
original amount separately. It is not original exact-decimal monetary data or
platform profit. Input size, adapter fetched rows and grouped output rows are
distinct quantities; these tables do not claim aggregate input-row throughput.

## Evidence and verification

The execution harness leaves `canonical_validation.state = pending_external`:
execution and byte checks alone do not establish value correctness. The final
summary joins report/profile, case, workflow, engine and trial identities, and
requires the measured artifact hash to match the independently decoded file.
All 84 passed the exit-status, service-limit, cleanup, reference, raw-audit and
independent-expectation checks. The summary reconciles saved JSON and does not
itself rerun Arrow decoding.

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

Run the following steps on the designated Linux VM. The recorded Azure
environment used **Python 3.11.2**, **DuckDB 1.5.6**, **Polars 1.44.2**,
**PyArrow 25.0.1**, and **PyYAML 6.0.3**. The
[host evidence](evidence/analytics-workflow-hosts.json) records the tested
Linux kernels and architectures. Use cgroup v2, systemd and passwordless sudo
for the bounded runner. These instructions do not run a build on macOS.

Use a new owned benchmark directory, with already built matching `bin/kelvo`
and `bin/kelvo-landlock` executables. The preparation helper expects these
relative paths beneath it:

| Path | Required contents |
| --- | --- |
| `scripts/` | This revision's benchmark, preparation, validation and shared helper scripts. |
| `benchmarks/ride-hailing/` | Frozen workflow YAML and both SQL dialects. |
| `private/` | Owned mode-0700 directory for source environment and generated manifests. |
| `runs/` | Owned mode-0700 directory on the separately provisioned bounded disk filesystem. |
| `reports/` | Existing report output directory; each report filename must be new. |
| `datasets/` | New normalized files and their generated `provenance.json`; Oracle live-only preparation needs the provenance file, not local Parquet copies. |
| `catalog-files.yml` | Catalog binding file sources `trips` and `zones` to the two normalized Parquet files. |
| `catalog-live.yml` | Registered read-only ClickHouse connection, normally `taxi`, exposing the same fixture tables. |

The helpers do not provision the source database, source reader, SSH forwarding,
catalogs, binaries, or disk filesystem. Prepare these before measurement using
the [federation configuration](federation.md) and recorded fixture provenance.
The benchmark's shared filesystem is approximately 2 GiB; preserve its actual
capacity in reports. A normal directory on a larger filesystem does not
reproduce that capacity constraint.

Set `BENCH_ROOT` to that absolute directory, `REPO_ROOT` to this checkout on the
VM, and `BENCH_SOURCE_DIR` to the directory containing the already downloaded
original monthly Parquet files and zone CSV. The dataset-preparation output
directory must not already exist. Install the pinned Python packages outside
trial timing, retaining the installation record privately:

```bash
set -eu
test "$(uname -s)" = Linux
: "${BENCH_ROOT:?Set the new owned benchmark root}"
: "${REPO_ROOT:?Set the Linux checkout path}"
: "${BENCH_SOURCE_DIR:?Set the original dataset directory}"
umask 077
python3.11 -m venv "$BENCH_ROOT/venv"
"$BENCH_ROOT/venv/bin/python" -m pip install \
  --report "$BENCH_ROOT/private/package-install.json" \
  duckdb==1.5.6 polars==1.44.2 pyarrow==25.0.1 PyYAML==6.0.3

"$BENCH_ROOT/venv/bin/python" "$BENCH_ROOT/scripts/prepare_analytics_dataset.py" \
  --source-directory "$BENCH_SOURCE_DIR" \
  --provenance "$REPO_ROOT/docs/evidence/federation-capacity-provenance.json" \
  --output-directory "$BENCH_ROOT/datasets"
```

The source fingerprints are checked before conversion. This produces
source-equivalent normalized local Parquet inputs; it does not measure snapshot
refresh or acceleration-cache construction. Preserve the interpreter version
and sanitized package/wheel hashes from the installation record alongside
`preparation-evidence.json`. A dependency version alone is not a native-wheel
fingerprint. Do not publish private manifests or the raw installation record.

Create an Azure public configuration at `$BENCH_ROOT/public.yml`:

```yaml
schema_version: 1
machine: azure
profile: standard
panels: [snapshot, live]
connection: taxi
```

Set `BENCH_DATABASE` to the existing validated source database identifier and
`BENCH_ENV_FILE` to its existing mode-0600 environment JSON. That private file
contains `KELVO_SOURCE_WORKFLOW_URL`, `KELVO_SOURCE_WORKFLOW_USER` and
`KELVO_SOURCE_WORKFLOW_PASSWORD`; the live catalog references environment
variable names. Credentials never belong in YAML, command arguments or
published reports.

```bash
"$BENCH_ROOT/venv/bin/python" "$BENCH_ROOT/scripts/prepare_analytics_manifests.py" \
  --root "$BENCH_ROOT" --config "$BENCH_ROOT/public.yml" prepare \
  --source-database "$BENCH_DATABASE" --environment-file "$BENCH_ENV_FILE"

"$BENCH_ROOT/venv/bin/python" "$BENCH_ROOT/scripts/analytics_native_references.py" \
  --manifest "$BENCH_ROOT/private/manifests/reference-manifest.json" \
  --output-directory "$BENCH_ROOT/references" \
  --report "$BENCH_ROOT/reports/native-references.json"
```

The [manifest helper](../scripts/prepare_analytics_manifests.py) writes new
private manifests and sanitized `preparation-evidence.json`. Its snapshot
profile fixes a 1536 MiB service cap, two threads and a 1024 MB DuckDB internal
budget; Polars uses the recorded version-specific OOC settings. The live
profile fixes a 640 MiB service cap. Both use the same 200% CPU quota, zero
swap, 96-task cap, result codec and shared filesystem policy. Report engine
budgets separately from those service limits.

Run each one-trial preflight sequentially:

```bash
for panel in snapshot live; do
  "$BENCH_ROOT/venv/bin/python" "$BENCH_ROOT/scripts/analytics_workflow_benchmark.py" \
    --manifest "$BENCH_ROOT/private/manifests/$panel-preflight.json" \
    --workdir "$BENCH_ROOT/runs" \
    --output "$BENCH_ROOT/reports/$panel-preflight.json"
done
```

For the Oracle live panel, stage the same binaries, scripts, workflow contract
and dataset provenance under its owned root. Use `machine: oracle`,
`profile: oracle-micro`, and `panels: [live]`; point the private source URL at
the verified forwarding endpoint. Prepare and run `live-preflight.json` there
with Python and PyYAML. The execution runner itself uses only the standard
library and never decodes Arrow. Do not install the analytical libraries or
copy/execute Parquet inputs on Oracle merely to run this live panel.

After the Oracle process exits, transfer its report and each matching
`runs/analytics-*/*/measurement.json` and `result.arrow` into
`$BENCH_ROOT/incoming-oracle-preflight/`, preserving the `reports/` and `runs/`
layout. Keep private invocation JSON, environment files and driver sidecars
out of that transfer. Run canonical validation on Azure, outside all benchmark
timers:

```bash
"$BENCH_ROOT/venv/bin/python" "$BENCH_ROOT/scripts/prepare_analytics_manifests.py" \
  --root "$BENCH_ROOT" --config "$BENCH_ROOT/public.yml" validation \
  --name preflight --references-directory "$BENCH_ROOT/references" \
  --run azure-snapshot "$BENCH_ROOT/reports/snapshot-preflight.json" "$BENCH_ROOT/runs" \
  --run azure-live "$BENCH_ROOT/reports/live-preflight.json" "$BENCH_ROOT/runs" \
  --run oracle-live "$BENCH_ROOT/incoming-oracle-preflight/reports/live-preflight.json" \
                    "$BENCH_ROOT/incoming-oracle-preflight/runs"

"$BENCH_ROOT/venv/bin/python" "$BENCH_ROOT/scripts/validate_analytics_workflows.py" \
  --manifest "$BENCH_ROOT/private/validation-preflight.json" \
  --output "$BENCH_ROOT/reports/validation-preflight.json" \
  --scratch "$BENCH_ROOT/runs"
```

Preserve and resolve preflight failures before proceeding. Use the separate
three-trial `snapshot-measure.json` and `live-measure.json` manifests for final
measurements, with new correspondingly named report files. The same harness
command applies with `preflight` replaced by `measure`; the manifest records
all trial counts and execution order. Keep all hosts' timed work sequential,
with no concurrent build, reference generation or validation.

Transfer the completed Oracle measured artifacts into
`incoming-oracle-measure/`, then repeat validation preparation with
`--name measure`, the three measured report paths, and that measured import
root. Run the validator on `private/validation-measure.json`, writing
`reports/validation-measure.json`. Do not reuse preflight report filenames or
combine preflight and measured trials in the timing distribution.

The [offline summarizer](../scripts/summarize_analytics_workflows.py) consumes
only the final JSON reports. Campaign labels must match the `--run` labels
used to prepare validation:

```bash
python3 "$BENCH_ROOT/scripts/summarize_analytics_workflows.py" \
  --campaign "azure-snapshot=$BENCH_ROOT/reports/snapshot-measure.json" \
  --campaign "azure-live=$BENCH_ROOT/reports/live-measure.json" \
  --campaign "oracle-live=$BENCH_ROOT/incoming-oracle-measure/reports/live-measure.json" \
  --validation "$BENCH_ROOT/reports/validation-measure.json" \
  --output "$BENCH_ROOT/reports/summary-measure.json"
```

Exit status 1 may accompany a valid summary containing failed or unstarted
attempts. Inspect its ledger and counts rather than discarding the file.
Before releasing the shared scratch filesystem, archive retained Arrow files
and measurements outside that filesystem and verify their fingerprints. Public
publication uses sanitized execution, validation and summary JSON; source
environment, raw manifests, invocation files and logs stay private.

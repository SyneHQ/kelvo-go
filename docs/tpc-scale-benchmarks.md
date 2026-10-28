# TPC-H and TPC-DS scale benchmarks

These runs are **derived from** TPC-H and TPC-DS. They are not audited TPC results and report no TPC metric (QphH, QphDS, price/performance). Data and query texts come from DuckDB's `tpch`/`tpcds` extensions.

**Headline.** Kelvo returned the DuckDB-reference answer for every trial of all 22 TPC-H queries at SF1, SF10 and SF30, and all 99 TPC-DS queries at SF1 and SF10. That is 924 of 924 Kelvo-over-Parquet trials, across two hosts. Kelvo runs DuckDB, so this checks the gateway path (catalog, attach, Arrow delivery, IPC output). It does not test a second engine. Kelvo's in-engine time was 4–14% above DuckDB Python's. Kelvo's complete-process times were still lower, because a Go binary starts in about 0.2 s less than `python -c "import duckdb"`.

## Results

Seconds are complete-process wall time: median (min–max) over 3 trials, from process start to exit, including Arrow IPC file output and fsync. "Sum" adds per-query medians over the queries where the system and DuckDB both passed all trials. Geomean is per-query median / DuckDB median.

| Suite | SF | Host | DuckDB Python: passed, sum s | Kelvo Parquet: passed, sum s, geomean | Kelvo snapshot: passed, geomean | Trino 479: passed (dialect / memory / mismatch), geomean |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| TPC-H | 1 | AMD | 22/22, 7.00 | 22/22, 4.63, 0.64 | 22/22, 2.00 | 21/22 (1/0/0), 2.48 |
| TPC-H | 1 | Intel | 22/22, 5.29 | 22/22, 3.69, 0.68 | 22/22, 1.90 | 21/22 (1/0/0), 2.61 |
| TPC-H | 10 | AMD | 22/22, 31.62 | 22/22, 29.48, 0.92 | 0/22 (refresh failed) | 19/22 (1/2/0), 3.67 |
| TPC-H | 10 | Intel | 22/22, 22.28 | 22/22, 20.69, 0.91 | 0/22 (refresh failed) | 19/22 (1/2/0), 4.39 |
| TPC-H | 30 | AMD | 22/22, 85.11 | 22/22, 85.40, 0.99 | not run | 14/22 (1/7/0), 3.56 |
| TPC-H | 100 | AMD | attempted, lost (see below) | | | |
| TPC-DS | 1 | Intel | 99/99, 22.98 | 99/99, 15.03, 0.63 | 99/99, 1.98 | 79/99 (10/0/10), 3.01 |
| TPC-DS | 10 | Intel | 99/99, 73.00 | 99/99, 66.17, 0.85 | 0/99 (refresh failed) | 80/99 (10/0/9), 4.88 |

Hosts: AMD = AMD EPYC 7763; Intel = Intel Xeon 6973P-C. Both are shared GitHub-hosted Azure VMs with 4 vCPU, 15.6 GiB RAM and kernel 6.17.0-1022-azure. Absolute times are not comparable across the two hosts.

### Where Kelvo's time goes

From the engine statistics Kelvo prints, `duration_ns` covers prepare, execution and Arrow delivery. Process time minus `duration_ns` is the median time outside the engine:

| Run | Kelvo outside engine, median | Kelvo prepare (attach/bind), median | DuckDB Python outside query (imports, connect, views), median | In-engine sum, Kelvo / DuckDB |
| --- | ---: | ---: | ---: | ---: |
| TPC-H SF1 AMD | 0.044 s | 0.012 s | 0.205 s | 11.6 / 8.0 s |
| TPC-H SF10 AMD | 0.045 s | 0.038 s | 0.218 s | 85.7 / 79.6 s |
| TPC-H SF30 AMD | 0.046 s | 0.099 s | 0.253 s | 255.6 / 241.1 s |
| TPC-DS SF10 Intel | 0.036 s | 0.040 s | 0.176 s | 189.2 / 165.4 s |

The process-level "Kelvo is faster" result comes from the startup gap. Inside the engine, Kelvo is slower by 4–14%. The cause is source attachment for every catalog table, the bounded `LIMIT` wrapper and Arrow delivery through the gateway. Prepare time grows with scale factor. As queries get longer, the startup advantage shrinks: the SF30 geomean is 0.99, which is parity. **Kelvo does not execute SQL faster than DuckDB.**

### TPC-H SF10, per query (AMD)

| Query | DuckDB Python s | Kelvo s | Trino s | Kelvo answer | Kelvo peak cgroup MiB | Kelvo peak tree RSS MiB | DuckDB peak cgroup MiB |
| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| q01 | 1.67 (1.65–1.68) | 1.63 (1.63–1.63) | 3.75 (3.24–7.33) | exact 3/3 | 121 | 275 | 106 |
| q02 | 0.49 (0.48–0.51) | 0.40 (0.39–0.46) | 3.59 (3.31–6.23) | exact 3/3 | 169 | 324 | 162 |
| q03 | 1.29 (1.25–1.32) | 1.24 (1.20–1.28) | 5.68 (5.44–6.68) | exact 3/3 | 216 | 371 | 202 |
| q04 | 0.84 (0.83–0.86) | 0.80 (0.78–0.80) | 3.66 (3.59–4.10) | exact 3/3 | 191 | 346 | 205 |
| q05 | 1.56 (1.52–1.57) | 1.52 (1.50–1.55) | 10.01 (9.61–10.53) | exact 3/3 | 168 | 323 | 148 |
| q06 | 0.78 (0.78–0.79) | 0.70 (0.69–0.82) | 1.41 (1.22–1.94) | exact 3/3 | 95 | 248 | 86 |
| q07 | 1.50 (1.49–1.52) | 1.50 (1.46–1.52) | 14.86 (14.44–15.48) | exact 3/3 | 259 | 413 | 242 |
| q08 | 1.80 (1.80–1.81) | 1.82 (1.77–1.87) | 12.07 (11.80–12.62) | exact 3/3 | 207 | 362 | 176 |
| q09 | 3.09 (3.02–3.09) | 2.97 (2.96–3.02) | failed 3/3: 4 GB query memory | exact 3/3 | 1244 | 1396 | 1227 |
| q10 | 1.48 (1.46–1.51) | 1.42 (1.41–1.44) | 4.27 (4.21–5.53) | exact 3/3 | 499 | 631 | 494 |
| q11 | 0.48 (0.46–0.49) | 0.35 (0.35–0.36) | 2.25 (2.22–2.77) | exact 3/3 | 130 | 286 | 133 |
| q12 | 0.88 (0.88–0.90) | 0.79 (0.79–0.80) | 1.96 (1.92–2.57) | exact 3/3 | 137 | 291 | 120 |
| q13 | 1.80 (1.79–1.82) | 1.71 (1.69–1.73) | 4.48 (4.47–4.89) | exact 3/3 | 598 | 751 | 587 |
| q14 | 1.07 (1.07–1.08) | 1.03 (1.03–1.05) | 1.87 (1.72–1.92) | exact 3/3 | 275 | 430 | 255 |
| q15 | 1.09 (1.09–1.12) | 1.03 (1.02–1.04) | failed 3/3: dialect | exact 3/3 | 223 | 380 | 210 |
| q16 | 0.52 (0.52–0.52) | 0.41 (0.41–0.41) | 1.43 (1.30–2.22) | exact 3/3 | 327 | 462 | 316 |
| q17 | 1.53 (1.52–1.57) | 1.51 (1.51–1.52) | 9.66 (9.34–10.35) | exact 3/3 | 138 | 294 | 111 |
| q18 | 1.98 (1.85–2.03) | 1.96 (1.82–1.98) | 16.41 (16.12–17.61) | exact 3/3 | 899 | 1053 | 887 |
| q19 | 1.42 (1.41–1.44) | 1.35 (1.34–1.35) | 2.06 (1.97–2.51) | exact 3/3 | 244 | 397 | 234 |
| q20 | 1.29 (1.25–1.31) | 1.24 (1.21–1.27) | 4.54 (4.53–6.32) | exact 3/3 | 159 | 313 | 137 |
| q21 | 4.47 (3.45–5.24) | 3.64 (3.60–3.97) | failed 3/3: 4 GB query memory | exact 3/3 | 555 | 704 | 524 |
| q22 | 0.56 (0.56–0.57) | 0.47 (0.46–0.47) | 1.32 (1.17–1.78) | exact 3/3 | 164 | 319 | 150 |

Every Kelvo and DuckDB trial at every scale stayed under the 8 GiB cap. Peak charged cgroup memory was 5.0 GiB at TPC-H SF30 and 4.8 GiB at TPC-DS SF10. The sampled temp-directory spill was 0 bytes in all of them. Sampled tree RSS can exceed charged cgroup memory, because RSS counts shared and file-backed pages that the cgroup charges differently.

### Concurrency: TPC-H SF10 through `kelvo serve` (AMD)

`kelvo serve --concurrency 4`, with a per-query `--memory-mb 1536 --threads 4` and the whole server in one 8 GiB / 400% CPU scope. Each stream runs the 22 queries in its own seeded shuffle. When a results claim gets HTTP 429 ("Execution capacity is unavailable"), the client counts a refusal, sleeps 0.25 s, and claims the same handle again. Latency runs from create to the last result byte, waits included. All 638 queries succeeded. The server cgroup recorded no `memory.max` or OOM events.

| Streams | Queries | Succeeded | Failed | 429 refusals (retried) | Wall s | Throughput q/h | p50 s | p95 s | p99 s | Server peak cgroup MiB |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 22 | 22 | 0 | 0 | 28.5 | 2784 | 1.28 | 2.73 | 3.65 | 2807 |
| 4 | 88 | 88 | 0 | 0 | 112.0 | 2828 | 4.97 | 11.22 | 14.41 | 3602 |
| 8 | 176 | 176 | 0 | 5629 | 362.4 | 1749 | 5.02 | 92.54 | 151.87 | 2866 |
| 16 | 352 | 352 | 0 | 33077 | 723.1 | 1753 | 5.75 | 155.77 | 239.21 | 3170 |

Four queries of four threads each fill four vCPUs, so throughput is flat from 1 to 4 streams. Above the admission limit, `serve` does not queue. It refuses, and a polling client gets no fairness: one query was refused 1,250 times at 16 streams. Tail latency explodes, and throughput fell 38% from the 4-stream level. That throughput drop has not been diagnosed. One candidate is idle permits between polls plus an uneven tail, with fewer than four queries running near the end. The p50 holds near 5 s, but p95/p99 are queueing artifacts of this client policy. A production deployment needs a client that backs off with jitter, or server-side queueing.

## Methodology

- **Data.** DuckDB 1.5.6 runs `CALL dbgen(sf)` / `CALL dsdgen(sf)` into a file-backed database, then writes one zstd Parquet file per table with 122,880-row groups. Row counts, bytes and SHA-256 hashes are in each report's `dataset`. The same files feed every system.
- **Queries.** DuckDB's bundled `tpch_queries()` (22) and `tpcds_queries()` (99), unmodified, written by `export-sql`. Their SHA-256 hashes are pinned in [workflows.yml](../benchmarks/tpc/workflows.yml). No system gets a rewritten dialect variant.
- **Systems.**
  - *DuckDB Python*: a fresh `python -I` process. It creates `read_parquet` views, runs the query, fetches Arrow, and writes an fsynced Arrow IPC file.
  - *Kelvo (Parquet)*: a fresh `kelvo query --mode federated` process over `type: parquet` sources, writing an fsynced IPC file.
  - *Kelvo (snapshot)*: the same process, reading `accelerate refresh` copies of each table. Refresh time is excluded and recorded under `setup`.
  - *Trino*: `trinodb/trino:479` as one long-lived single-node container (Hive connector, file metastore, native local filesystem over hard links to the same Parquet). Container start, JVM warm-up and DDL are excluded, which favours Trino. Timing is client-side, from execute to the last row fetched over HTTP. No IPC file is written.
- **Limits.** These are the same for each system. Each Kelvo/DuckDB trial runs in its own transient systemd scope with `MemoryMax=8G`, `MemorySwapMax=0`, `CPUQuota=400%` and a fresh `TMPDIR`. Engine settings: `memory_limit` 6144 MB, 4 threads, 60,000 MB temp. Trino runs in a container with `--memory 8g --memory-swap 8g --cpus 4`, a 6 GiB heap, `query.max-memory-per-node=4GB`, `task.concurrency=4` and spill disabled. Query timeout: 900 s.
- **Order.** For each query, three trials, with system order rotated per trial and DuckDB first in trial 0. Caches were not dropped, so results are warm-OS-cache after the first trial.
- **Answers (D3).** The reference is DuckDB Python's trial-0 answer on the same files. Integers, strings, dates, NULLs, row count and column count must match exactly. A decimal on either side may differ by at most half a unit of the coarser declared scale. Floats may differ by at most a 1e-9 relative error. Rows are compared in order first. If only tie order differs, the verdict is `matched_unordered` and it is reported (none occurred). Any other difference is a `mismatch` and counts as a failure. Timing medians use only trials with matching answers. Memory peaks are maxima over all trials, failures included.
- **Measurements.** A 100 ms sampler polls the scope's `memory.peak`/`memory.current`, the RSS summed over its `cgroup.procs`, `memory.events` and the temp-directory size. Process wait blocks in `waitpid`, so no poll quantization is added.

Harness: [tpc_benchmark.py](../scripts/tpc_benchmark.py), [tpc_summarize.py](../scripts/tpc_summarize.py), unit tests [test_tpc_benchmark.py](../scripts/test_tpc_benchmark.py), evidence checks [test_tpc_evidence.py](../scripts/test_tpc_evidence.py). Kelvo binary SHA-256 `879cd53f0ded540e…` (full value in each report), built with `go build -tags duckdb_arrow` from `cargo` at `18dda549`. Python 3.12.3, DuckDB 1.5.6, PyArrow 25.0.1, Trino client 0.336.0.

## Failures, kept

- **Kelvo snapshots at SF10 (TPC-H and TPC-DS): 0 queries passed.** At TPC-H SF10, the full-table `accelerate refresh` of `lineitem` (60.0M rows) was OOM-killed at the 8 GiB cap: `memory.peak` reached 8 GiB, `oom_kill`=1, and the query was cancelled. The pinned DuckDB path materializes the result before Arrow delivery. At TPC-DS SF10, the `inventory` refresh (133M rows) failed with `Query result exceeds row limit`, because Kelvo caps `max_rows` at 100,000,000. The harness names every table in `--sources`, so every query then failed with `DATASET_UNAVAILABLE` ("Accelerated dataset is missing, stale, or requires a refresh"), including queries that never read the failed table. SF30 snapshots were not attempted: `lineitem` has 180M rows, over the row cap.
- **Kelvo snapshots are about 2× slower than Kelvo over the original Parquet at SF1.** The snapshot writer produced 2,048-row Snappy row groups (2,931 groups for SF1 `lineitem`) instead of the source's 122,880-row zstd groups. Queries that read `lineitem` pay about 0.5 s more in the engine. Queries that skip it (q02, q11, q13, q16, q22) are within 0.1 s.
- **Trino dialect failures:** 1 TPC-H query (q15: the DuckDB text uses a CTE alias Trino does not resolve) and 10 TPC-DS queries (q16, q32, q58, q72, q83, q92, q94, q95: `date` compared with `varchar` or `date + integer`; q70, q86: `grouping()` in `ORDER BY`). These are failures of DuckDB-dialect texts on Trino, not engine defects. The suites carry no Trino-dialect variant.
- **Trino memory failures:** TPC-H q09 and q21 at SF10, and 7 queries at SF30, hit `EXCEEDED_LOCAL_MEMORY_LIMIT` at the 4 GB per-node query memory. Spill was disabled.
- **Trino mismatches: TPC-DS q07, q18, q26, q27, q47, q57, q63 and q89.** Trino's `AVG(decimal)` returns a `decimal(…,2)`. These cases sit exactly on the half-unit boundary (for example, reference `51.105` (DOUBLE) against Trino `51.11`), so they exceed the pre-declared tolerance and stay as mismatches. q66 (a 6-scale decimal against a DOUBLE) and q44 (different rows under `ORDER BY … LIMIT` with ties) also mismatch. At SF10, one of these became exact and the count is 9.
- **Discarded runs:** two runs were discarded and rerun in full. The first had umask 0002 on the runner. The second used a harness that polled `wait()` with 50 ms quantization. A first concurrency attempt livelocked: its client created a new handle on every 429 until `--max-queries` filled. The client was fixed to reclaim the same handle. None of the discarded data is published.

**TPC-H SF100 was attempted but lost.** Generation completed: `lineitem` alone was 16.1 GB of Parquet. The run reached 105 of 198 planned trials before the scratch runner expired at about 00:37Z, and it took the report and data with it. Nothing from it is published. For that reason, no SF100 number or claim appears here. Rerunning it needs a host that lives longer than about 2 h for generation plus run.

## What this proves, and what it does not

It proves:

- Over these 121 query texts and five scale points, Kelvo's gateway path returns the same answers as DuckDB Python, including decimals, NULLs and ordering. Every trial was exact.
- With identical 8 GiB / 4-thread caps, Kelvo adds a small, measurable cost in the engine: 4–14% in sum. Its process-level startup is about 0.2 s less than a Python driver's. On single short queries that makes the end-to-end command faster, and the advantage disappears by SF30.
- Peak memory is reported at every scale: Kelvo stayed within 5.1 GiB charged at SF30, with no spill observed.
- `kelvo serve` with admission set to 4 completed every query at 16 concurrent streams without memory events. It refuses rather than queues, and the tables show the tail latency and throughput that follow for a naive polling client.

It does not prove:

- That Kelvo is faster than DuckDB. It executes with DuckDB.
- Anything about Trino at its best. The queries are DuckDB-dialect, query memory was capped at 4 GB without spill, and the deployment is a single node. Trino is built to scale out. Its numbers here only describe "the same box, the same caps, the same texts".
- Cold-cache performance, or performance on dedicated hardware. These are shared, noisy cloud VMs with uncontrolled caches. The two hosts have different CPUs, so compare systems within a row, not across hosts.
- Production capacity, or official TPC metrics. Three trials give a median and a range, not a confidence interval.
- Snapshot acceleration at scale. It failed at SF10 on this cap and on the row limit.

## Evidence

| File | Contents |
| --- | --- |
| [tpc-h-sf1-amd.json](evidence/tpc-h-sf1-amd.json), [tpc-h-sf1-intel.json](evidence/tpc-h-sf1-intel.json) | TPC-H SF1, 4 systems × 22 queries × 3 trials, on two hosts |
| [tpc-h-sf10-amd.json](evidence/tpc-h-sf10-amd.json), [tpc-h-sf10-intel.json](evidence/tpc-h-sf10-intel.json) | TPC-H SF10 on two hosts, including the snapshot refresh failure |
| [tpc-h-sf30-amd.json](evidence/tpc-h-sf30-amd.json) | TPC-H SF30, DuckDB / Kelvo / Trino |
| [tpc-ds-sf1-intel.json](evidence/tpc-ds-sf1-intel.json), [tpc-ds-sf10-intel.json](evidence/tpc-ds-sf10-intel.json) | TPC-DS SF1 and SF10 |
| [tpc-h-sf10-concurrency-amd.json](evidence/tpc-h-sf10-concurrency-amd.json) | Every concurrent query's latency, refusals and errors, plus server scope peaks |
| [tpc-h-sf1-amd.log](evidence/tpc-h-sf1-amd.log) | Full per-trial log of the SF1 run |

Each run report has the dataset manifest, host facts, limits, binary and harness hashes, and every trial's process time, verdict, error text, engine statistics, peak cgroup memory, sampled tree RSS, spill bytes and OOM events.

## Reproduce

```sh
python3 -m venv venv && venv/bin/pip install duckdb==1.5.6 pyarrow==25.0.1 trino==0.336.0
go build -tags duckdb_arrow -o kelvo ./cmd/kelvo
venv/bin/python scripts/tpc_benchmark.py export-sql
venv/bin/python scripts/tpc_benchmark.py generate --suite tpch --sf 10 --out data/tpch-sf10
umask 022   # Kelvo rejects group-writable state files
venv/bin/python scripts/tpc_benchmark.py run --suite tpch --data data/tpch-sf10 --kelvo ./kelvo \
  --workdir runs/tpch-sf10 --output tpc-h-sf10.json --timeout 900
venv/bin/python scripts/tpc_benchmark.py concurrency --data data/tpch-sf10 --kelvo ./kelvo \
  --workdir runs/conc --output conc.json --engine-memory-mb 1536
python3 scripts/tpc_summarize.py --per-query tpc-h-sf10.json conc.json
python3 scripts/test_tpc_evidence.py
```

This needs Linux with cgroup v2, passwordless `sudo systemd-run`, and Docker for Trino.

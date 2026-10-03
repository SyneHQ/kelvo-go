# Taxi CTE workflows

Four reproducible analyses over **22,612,607 real January–March 2019 NYC TLC trips** and **265 official zones**. See [provenance](evidence/federation-capacity-provenance.json), [measured results](analytics-workflow-benchmarks.md), and the authoritative [ordered schemas, limits and invariants](../benchmarks/ride-hailing/workflows.yml).

The fixture lacks riders, drivers, bookings, cancellations, online supply, payment methods and dropoff timestamps. These queries describe recorded trips and charges; they cannot establish retention, conversion, utilization, duration, speed or platform profit.

## Choose a workflow

| Workflow ID | DuckDB SQL | ClickHouse SQL |
| --- | --- | --- |
| `daily_rolling_kpis` | [SQL](../benchmarks/ride-hailing/daily_rolling_kpis.sql) | [SQL](../benchmarks/ride-hailing/daily_rolling_kpis.clickhouse.sql) |
| `borough_hour_hotspots` | [SQL](../benchmarks/ride-hailing/borough_hour_hotspots.sql) | [SQL](../benchmarks/ride-hailing/borough_hour_hotspots.clickhouse.sql) |
| `route_economics` | [SQL](../benchmarks/ride-hailing/route_economics.sql) | [SQL](../benchmarks/ride-hailing/route_economics.clickhouse.sql) |
| `monthly_zone_momentum` | [SQL](../benchmarks/ride-hailing/monthly_zone_momentum.sql) | [SQL](../benchmarks/ride-hailing/monthly_zone_momentum.clickhouse.sql) |

| Workflow | What it computes | Required checks |
| --- | --- | --- |
| Daily rolling KPIs | Daily aggregates, 90-day calendar fill, current day plus six preceding days | Exactly 90 rows; `window_days` grows 1–7 then stays 7; recompute windows from independent daily counts. Negative amounts remain included. |
| Borough/hour hotspots | Known pickup zones, borough/hour denominators, top three zones | Rank by trips descending then `zone_id` ascending; consecutive ordinal ranks, not dense ranks. Do not call displayed top-three counts total demand. |
| Route economics | Known pickup/dropoff pairs, valid-distance charge totals, at least 100 usable trips, top ten routes per pickup borough | Rank by amount descending, trips descending, pickup/dropoff IDs ascending. Distance-band counts sum to trips; totals reconcile before ranking. |
| Monthly momentum | 265 zones × three months, zero filling, previous-month counts and signed change | February/March return 530 rows. January stays in the grid before filtering; `trip_change = trips - previous_trips`; ties use `zone_id ASC`. |

Output row limits are 90, 100000, 1000 and 1000 respectively. Eight borough labels imply theoretical bounds of 2160 × 8 × 3 = 51,840 hotspots and 80 routes, not guaranteed result counts. Preserve the contract's ordered Int64/string columns, actual null validity and final sorting.

## Keep data semantics identical

- Preserve original rows/anomalies and stable `trip_id`, `source_month`, `source_row`. Snapshot files must not be prefiltered or preaggregated.
- `pickup_unix_us` is original naive wallclock, not verified UTC. Use `[1546300800000000, 1554076800000000)` and source files 201901–201903. Day/hour divisors are 86,400,000,000 / 3,600,000,000; keys span 17897–17986 / 429528–431687. No timezone/DST reinterpretation. Monthly buckets use pickup month, not file month.
- `fare_cents` is once-derived half-even rounding of Float64 `total_amount * 100`; null/non-finite amounts become null. Output name is `total_amount_cents`. Do not reround independently or describe this as original exact-decimal fare revenue.
- Route amounts must be present/nonnegative; distances must be in `(0,100]`. Bands are `[0,1)`, `[1,5)`, `[5,100]`, intersected with that cohort. Missing amounts are not zero-valued source records.
- Use only trusted identifiers for `{trips}`/`{zones}`: live federation uses `taxi.trips`/`taxi.zones`; native uses physical fixtures. Never substitute credentials or unvalidated SQL.

The dialect differences are integer division (`//`/`intDiv`), day spine (`range`/`numbers`), casts, and previous-month `LAG`/nullable `lagInFrame` with an explicit full-partition frame. Explicit zero filling avoids configurable ClickHouse join-null behavior.

The source filter compiler supports exact integer/boolean comparisons and null tests, but not the required Float64 pushed predicates. Routes therefore use conditional aggregation for distance validity and push integer/time/amount predicates only. Record actual fetched rows; do not describe distance validity as source pushdown.

## Validate before comparing

1. Audit source-file counts, unique trip IDs, 265 unique non-null zone IDs, dimension nulls, timestamp exclusions/month mismatches, unmatched keys, amount anomalies, and distance null/NaN/infinity/range exclusions. Keep exclusion stages disjoint.
2. Bound signed sums and absolute sums with independent Python integers. Abort or agree a wider schema if Int64 cannot hold results.
3. Compute direct ClickHouse references outside timing. Independently reconstruct counts, sums, windows, lookup joins and tie ordering from the raw normalized Parquet fixture.
4. Check every measured output's ordered columns/rows, exact values, UTF-8, actual NULL validity, row count, canonical hash, invariants and Arrow EOS. Keep IPC byte hashes separate: batching/compression can change bytes without changing values.
5. Retain failures/timeouts without publishing incomplete outputs. A copied Kelvo result is not independent ground truth.

Native Polars expressions are allowed if semantics match. Sort before rolling/shift operations, use `min_samples=1` for the first six rolling outputs, and cast counts to signed Int64 before subtraction. Spines/grids belong inside timed computation; only reference/preparation work stays outside.

## Compare equivalent work

| Panel | Execution | Data and interpretation |
| --- | --- | --- |
| Live native, Azure | Kelvo delegates SQL to Azure ClickHouse | Reports gateway/export overhead plus source SQL; database work is outside the Kelvo process/cgroup |
| Live native, Oracle | Kelvo delegates the same SQL to Azure over the recorded source path | Includes remote source transport; does not imply Oracle performed the analytical SQL |
| Live federated, Azure | Kelvo/DuckDB computes from registered ClickHouse scans | Reports projected columns, pushed integer filters and actual fetched rows/bytes; do not assume all 22.6M rows were fetched |
| Live federated, Oracle | Same logical query computed by Kelvo/DuckDB on the micro VM | Measures local computation plus source transport under the declared micro limits |
| Shared Parquet, Azure | Kelvo over the snapshot, direct DuckDB, native Polars | All read identical raw normalized trips/zones files, with identical input hashes, logical cohorts and final results; optimizer plans may differ |

For same-file comparisons, include fresh startup, imports/setup, reads, joins/windows/sort, Arrow conversion and fsync. Use the same host/files/output codec/threads/external cap, with each engine's internal limits recorded separately. Never preload one library outside its timer.

Run at least three sequential trials after a separate warmup, rotating profile order. Keep complete-export medians/ranges, success counts, CPU, simultaneous sampled RSS, charged memory, scratch, OOM/timeouts, fetched/output rows and bytes. Source work and SSH accounting stay separate. Use the [benchmark guide](analytics-workflow-benchmarks.md#reproduce-on-linux) for commands; this design alone authorizes no throughput or production claim.

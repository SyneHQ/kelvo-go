# Real taxi CTE workflow plan

This document defines the four-workflow campaign and its exact SQL/result
contracts. Execution results and measurement boundaries are recorded separately
in the [workflow benchmark report](analytics-workflow-benchmarks.md). The input is
**22,612,607 real NYC TLC Yellow Taxi records from January–March 2019** and the
**265-row official zone lookup**, with the provenance in
[federation-capacity-provenance.json](evidence/federation-capacity-provenance.json).

These are ride-hailing-style operational analyses of recorded taxi trips. The
fixture has no riders, drivers, bookings, cancellations, online supply, payment
methods or dropoff timestamps. Do not infer conversion, retention, driver
utilization, trip duration, speed, platform revenue or profit from these fields.

## Shared data and semantics

The existing `trips` relation contains `trip_id`, `source_month`, `source_row`,
`pickup_unix_us`, pickup/dropoff zone IDs, `passenger_count`, `trip_distance`,
`total_amount` and `fare_cents`. The zone relation contains `zone_id`, `borough`,
`zone` and `service_zone`. Source rows and anomalous values are preserved.

The original pickup timestamp is a **naive source wallclock value**, persisted
as integer microseconds. It is not a verified UTC instant. All four queries use
the same numeric interval, `[1546300800000000, 1554076800000000)`, representing
the original source wallclock dates 2019-01-01 through 2019-03-31. They also
restrict `source_month` to the three source files, 201901–201903. Null pickup
timestamps and timestamps outside the interval do not enter these analyses;
their counts must appear in the preflight audit.

Day and hour buckets use integer division of the stored microsecond value by
86,400,000,000 and 3,600,000,000 respectively. All selected timestamps are
positive, so integer-division semantics agree. Calendar day keys are 17897
through 17986, and hour keys are 429528 through 431687. No timezone conversion,
DST correction or server-local timezone function is permitted. Display these
as recorded wallclock buckets, not UTC trip times. Monthly analysis uses the
pickup value's month, not the source file's month; report file/month mismatches.

`fare_cents` is the existing, once-derived half-to-even rounding of
`total_amount * 100`, with null/non-finite source amounts mapped to null. It is
not the original fare component or an exact monetary decimal supplied by TLC.
Outputs call it **`total_amount_cents`** and retain a count of measured amounts
where relevant. Zero-filling an aggregate does not imply missing amounts were
actually zero. Do not redo rounding independently in each engine.

All output metrics are signed 64-bit integers, UTF-8 strings and, where a
reference exposes them, explicit null validity. The four final result contracts
below require non-null values. Distances are Float64 inputs used only for exact
comparisons with the representable constants 0, 1, 5 and 100; no floating-point
aggregate or tolerance is needed. Average charges and shares can be displayed
later from the exact numerator/count columns; they are not rounded inside the
measured queries.

Before timing, verify and retain an audit of:

- The three source-file counts, total 22,612,607 trips, 265 unique non-null zone
  IDs, dimension string null counts, and unique fixture `trip_id` values.
- Timestamp nulls, values before/after the quarter, and file-month versus
  pickup-month mismatches, with in-scope and excluded counts summing to the input.
- In-scope null or unmatched pickup/dropoff IDs. Inner joins retain known lookup
  entries, including genuine `Unknown` or other published labels; no invented
  dimension row substitutes for an unmatched key.
- Null and negative `fare_cents`, and distance nulls, NaNs, infinities, nonpositive
  values and values above 100. Record disjoint stage counts for the route cohort
  so exclusions are auditable without double counting.
- The signed sum and sum of absolute non-null `fare_cents` values. Exact aggregate
  outputs must fit Int64; abort or agree a common wider schema before running
  if the bound fails, rather than allowing an engine's overflow behavior to
  define correctness.

## SQL files and dialect contract

Each SQL file uses only two substitution tokens, `{trips}` and `{zones}`. The
integration harness must substitute validated, trusted relation identifiers;
these are not user values or credential-bearing URLs. For live federation the
relations are `taxi.trips` and `taxi.zones`; native ClickHouse uses the corresponding
physical fixture tables. Snapshot runs bind the same relation names to the same
raw normalized Parquet files, without prefiltering or aggregating them.

| Workflow ID | DuckDB SQL | ClickHouse SQL |
| --- | --- | --- |
| `daily_rolling_kpis` | [SQL](../benchmarks/ride-hailing/daily_rolling_kpis.sql) | [SQL](../benchmarks/ride-hailing/daily_rolling_kpis.clickhouse.sql) |
| `borough_hour_hotspots` | [SQL](../benchmarks/ride-hailing/borough_hour_hotspots.sql) | [SQL](../benchmarks/ride-hailing/borough_hour_hotspots.clickhouse.sql) |
| `route_economics` | [SQL](../benchmarks/ride-hailing/route_economics.sql) | [SQL](../benchmarks/ride-hailing/route_economics.clickhouse.sql) |
| `monthly_zone_momentum` | [SQL](../benchmarks/ride-hailing/monthly_zone_momentum.sql) | [SQL](../benchmarks/ride-hailing/monthly_zone_momentum.clickhouse.sql) |

The intentional dialect differences are limited to integer division (`//`
versus `intDiv`), the 90-row day spine (`range` versus `numbers`), integer casts,
and the previous-month window (`LAG` versus nullable `lagInFrame` with an explicit
full-partition frame). Dimension identifiers, quarter bounds, CASE predicates,
aggregate inputs, tie ordering and final projections are otherwise equivalent.
Missing right-hand aggregates are explicitly zero-filled, avoiding reliance
on ClickHouse's configurable left-join null behavior.

The current native-adapter filter compiler accepts exact integer/boolean source
comparisons and null tests, but rejects required Float64 pushed comparisons.
The route query therefore expresses distance validity through **conditional
aggregation**, with integer/time/amount filters in `WHERE`. This preserves the
intended usable-distance cohort without adding unsupported floating predicates
to the source scan. Both dialect files use the same formulation. Native source
SQL and library inputs must not receive a cleaner, prefiltered dataset. Record
actual fetched rows and residual computation; do not silently describe this
choice as a pushed distance filter. Engine code changes are outside this plan.

## Workflow 1: daily and trailing-seven-day KPIs

`daily_rolling_kpis` filters the quarter, groups recorded trips and derived
total amounts by day, left joins an explicit 90-day calendar, and computes
trailing windows over the current row and six preceding calendar rows. It
answers how recorded demand and measured charges changed over a calendar week.
Negative recorded amounts remain included here; this is not the restricted
route cohort.

Output columns, in order, are `day_bucket`, `trips`, `measured_amount_trips`,
`total_amount_cents`, `window_days`, `trailing_7d_trips`,
`trailing_7d_measured_amount_trips`, and `trailing_7d_total_amount_cents`.
All are non-null Int64. Sort by `day_bucket`; `max_rows=90`.

The reference must contain exactly 90 consecutive day keys. Daily trip counts
must sum to the audited in-scope count, measured counts must not exceed trips,
and amount sums must match the independent in-scope integer sum. `window_days`
is 1–7 for the first seven days and 7 thereafter. Recompute every rolling result
with a Python deque over the independently obtained daily counts; missing days
must consume a calendar position instead of stretching the window to seven
nonempty days.

For a native Polars implementation, group lazily, join the day spine inside the
timed work, zero-fill, explicitly sort by day, and use seven-row rolling sums
with `min_samples=1`. The first six outputs must not become null because of a
library's default full-window requirement.

## Workflow 2: hourly pickup hotspots within each borough

`borough_hour_hotspots` groups each recorded hour and known pickup zone, computes
the full borough/hour trip total using a partition window, and ranks zones by
trip count descending with `zone_id` ascending as the deterministic tie-breaker.
It returns the top three zones in each populated borough/hour. This describes
observed pickup concentration, not unmet demand or available drivers.

Output columns are `hour_bucket` (Int64), `borough` (string), `zone_id` (Int64),
`zone_name` (string), `trips` (Int64), `borough_hour_trips` (Int64), and `zone_rank`
(Int64), all non-null. Sort by `hour_bucket, borough, zone_rank, zone_id`;
`max_rows=100000`. With the existing eight borough labels, the theoretical
quarter bound is 2160 hours × 8 labels × 3 zones = 51,840 rows; verify the actual
dimension label count during preflight.

Every partition has at most three results and consecutive ranks starting at
one. The borough/hour denominator is identical for its returned zones and at
least their displayed trip-count sum. Independently group counts without
ranking, sort the small per-hour dictionaries using the specified tie-breaker,
and compare every selected key and count. The complete unranked grouped count
must equal the audited in-scope known-pickup count. Do not sum only the displayed
top-three counts and call that total demand.

Polars may use group aggregation, partition sums and ordinal ranks after a full
deterministic sort. Do not use dense rank, because equal trip counts would then
change the number of selected zones.

## Workflow 3: route charge summaries and distance mix

`route_economics` starts with in-scope trips whose derived amount is present and
nonnegative. Conditional sums include only recorded distances in `(0, 100]`
miles. It groups pickup/dropoff zone pairs, retains routes with at least 100
usable trips, joins the official lookup twice, and returns the top ten routes
per pickup borough by derived total amount descending, then trips descending,
then pickup and dropoff IDs ascending. The threshold is a declared analytical
choice, not a data-dependent optimization.

The output includes measured counts and derived charge totals, plus the distance
mix `[0,1)`, `[1,5)`, and `[5,100]` intersected with the usable `(0,100]` cohort.
This is not fare-component revenue, driver earnings, profit or a speed measure.
Excluded negative amounts are not assumed to be errors or refunds; they simply
fall outside this explicitly named cohort.

Output columns are `pickup_borough` (string), `pickup_zone_id` (Int64),
`pickup_zone_name` (string), `dropoff_borough` (string), `dropoff_zone_id` (Int64),
`dropoff_zone_name` (string), `trips`, `total_amount_cents`, `lt_1_mile_trips`,
`ge_1_lt_5_mile_trips`, `ge_5_le_100_mile_trips`, and `route_rank` (all remaining
columns Int64). All are non-null. Sort by
`pickup_borough, route_rank, pickup_zone_id, dropoff_zone_id`; `max_rows=1000`.
The current eight pickup borough labels imply at most 80 output rows.

For every result, the three distance-band counts must sum exactly to `trips`,
`trips >= 100`, and the amount must be nonnegative. Independently aggregate
usable raw rows by the pair of IDs, join unique lookup dictionaries, and rank
using the full four-part order. Reconcile pre-threshold counts and amounts to
the audited usable-distance, known-both-zones cohort. Preserve the raw distance
values in the shared snapshot; a Polars expression may apply equivalent
filters during its timed lazy plan, but must not load precleaned inputs.

## Workflow 4: monthly zone momentum

`monthly_zone_momentum` derives pickup month from the numeric timestamp bounds,
aggregates counts and amounts, constructs the full 265-zone × 3-month grid,
zero-fills missing activity, and uses a previous-month window. It returns
February and March counts, their preceding month, the signed trip-count change,
and a deterministic change rank within each month/borough. A previous month
with zero trips remains zero; no division-by-zero growth percentage is created.

Output columns are `month` (Int64), `borough` (string), `zone_id` (Int64),
`zone_name` (string), `trips`, `previous_trips`, `trip_change`,
`measured_amount_trips`, `total_amount_cents`, and `zone_rank` (all remaining
columns Int64). All final values are non-null, including `previous_trips`:
January exists in the grid before it is excluded from final output. Sort by
`month, borough, zone_rank, zone_id`; `max_rows=1000`, expected rows 530.

Each of the 265 lookup IDs must appear once in February and once in March.
`trip_change = trips - previous_trips` uses signed arithmetic. The previous
count must match an independent zone/month dictionary, including zero-filled
combinations. Monthly count and amount sums over all zones must match the
corresponding known-pickup audit; ranked ties use `zone_id ASC`. In Polars,
sort by zone/month before `shift(1).over(zone_id)`, and cast counts to Int64
before subtraction so negative changes cannot underflow an unsigned dtype.

## Reference and manifest handoff

The machine-readable public contract is
[workflows.yml](../benchmarks/ride-hailing/workflows.yml). It records ordered
column types and nullability, output limits and ordering, semantic constants,
SQL file references relative to the repository root, and validation invariants.
Its SQL paths must be expanded into SQL text for the private runtime manifest.

Calculate direct ClickHouse native references outside measured runs. Validate
them using the independent aggregate/window invariants above and the same
normalized raw Parquet snapshot, with Python integer accumulators for sums.
Do not treat a copied Kelvo output as independent ground truth.

Reuse the existing canonical-value rules: ordered columns and rows, exact
integer values, UTF-8 string bytes and per-row null validity, independent of
Arrow batch boundaries and physical signed integer width. Every measured result
must match the reference row count, schema contract, full canonical hash and
invariants, and contain Arrow EOS. Physical field nullability may differ across
engines; actual unexpected null values fail the logical contract. Record IPC
byte hashes separately because differing batches or codecs can change them.
Failed or timed-out attempts must remain visible and must not publish a result
as successfully complete.

The integration owner will assemble a manifest with:

```text
dataset.trips  = {paths, rows, sha256}
dataset.zones  = {paths, rows, sha256}
versions      = pinned Kelvo binary, DuckDB, Polars and PyArrow versions
limits        = shared measured panel limits and exact output codec
workloads[ID] = {
  sql: {duckdb: rendered SQL, clickhouse: rendered SQL},
  inputs: [trips, zones as required],
  output: {columns: [{name, type, nullable:false}], order_by, max_rows},
  semantics: {timestamp_start_us, timestamp_end_us,
              bucket_basis: original_naive_source_wallclock},
  validation: {reference_id, canonical_value_sha256, invariant_results}
}
```

The direct-library runner may declare `implementation=polars_lazy` for its
native-expression version rather than forcing unsupported SQL syntax. Its
grouping, exclusions, zero filling, rank ordering, null behavior and output
casts must still match these contracts. Small day/month spines are legitimate
calendar dimensions, not fabricated trip rows, and must be constructed or joined
inside the measured work consistently.

## Measurement panels and README scope

Keep the live-source and same-file panels separate. A single ranking mixing
remote source queries, local file scans and different work placement would not
measure library overhead fairly.

| Panel | Execution | Data and interpretation |
| --- | --- | --- |
| Live native, Azure | Kelvo delegates SQL to Azure ClickHouse | Reports gateway/export overhead plus source SQL; database work is outside the Kelvo process/cgroup |
| Live native, Oracle | Kelvo delegates the same SQL to Azure over the recorded source path | Includes remote source transport; does not imply Oracle performed the analytical SQL |
| Live federated, Azure | Kelvo/DuckDB computes from registered ClickHouse scans | Reports projected columns, pushed integer filters and actual fetched rows/bytes; do not assume all 22.6M rows were fetched |
| Live federated, Oracle | Same logical query computed by Kelvo/DuckDB on the micro VM | Measures local computation plus source transport under the declared micro limits |
| Shared Parquet, Azure | Kelvo over the snapshot, direct DuckDB, native Polars | All read identical raw normalized trips/zones files, with identical input hashes, logical cohorts and final results; optimizer plans may differ |

For the same-file panel, snapshot creation, reference computation and validation
happen outside timing. Fresh process startup, imports, setup, scanning, grouping,
joins, windows, sorting, Arrow serialization and durable output completion are
inside the complete-export interval. Do not preload one library's full dataframe outside its measured
interval. If compute-only or already-warm phases are also useful, publish them
separately with their boundaries; do not mix them into the complete-export
ranking. Record native Polars expressions as such, and pin engine versions.

The live panel should use the same binary, source/result codec settings,
per-query thread counts and declared limits across its paired cases. The library
panel must use the same host, files, output codec, thread count and operating
system memory cap across all libraries. DuckDB memory settings are not a
whole-process limit, and Polars may not expose an equivalent internal budget;
report both the common external cap and each engine-specific setting. The root
operator must freeze those budgets before timing rather than silently raising
them after a failure.

Measure at least three sequential trials per workload/profile after a separately
identified warmup, interleaving or rotating profile order where practical.
Report every attempt, the successful count, median/range complete-export time,
whole-cgroup CPU, sampled process/RSS totals, charged memory, scratch, timeout/OOM
status, output rows/bytes, and canonical result validation. Account for source
database work and SSH separately. Resource sums must be sampled together;
charged page cache is not RSS and short samples can miss peaks.

For federation, retain scan-level row counts, logical Arrow bytes, encoded source
bytes, projections and the actual compiled source filters or a safe structural
description. Source rows fetched are not source storage rows examined. A native
aggregate's small result is not a measure of how little source data it scanned.
Where ClickHouse query-log read rows/bytes and elapsed/CPU can be matched safely
to a specific reference or trial, report them as a separate source metric.

The README matrix should link complete evidence and show workflow, dataset
scope, execution location/mode, validation count, output rows, median/range time,
CPU/memory and any failure. A valid native reference or a successful same-file
baseline does not turn a failed Oracle federated run into a success. Keep the
full-quarter scope; any later reduced-data diagnostic is a separate, clearly
named configuration. No throughput, scaling or production claim is authorized
by this workflow design alone.

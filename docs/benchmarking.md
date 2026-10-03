# ClickHouse benchmark helper

[benchmark_clickhouse.py](../scripts/benchmark_clickhouse.py) measures one native export through an already built Kelvo binary. Timing includes coordinator startup, execution and output fsync. Arrow validation runs afterward. This helper alone is not a reviewed speed claim.

## Run

Configure source credentials through catalog environment names, install PyArrow in the test environment, then run:

```sh
python3 scripts/benchmark_clickhouse.py \
  --binary /path/to/kelvo --catalog /path/to/kelvo.yml --connection clickhouse \
  --output /path/to/result.arrow --report /path/to/result.json
```

Defaults: `--memory-mb 1024 --max-rows 11000000 --max-bytes 1073741824 --timeout 120s --threads 2`. Native memory covers both source-query and Arrow-decoder budgets. An earlier 512 MiB source budget failed after approximately 69 MB/16 batches on this transfer; it is not a suitable default.

The report records binary SHA-256, schema/rows/NULLs, deterministic `id` sum, logical-buffer/file checksums, bytes and native stats. Coordinator/direct-child worker RSS is sampled separately every 20 ms; it is not total process-tree memory. Keep credentials, private endpoints and passwords out of commands/reports/commits.

## Exact fixture

Use `kelvo_bench.fact_events`: **100,000,000 contiguous IDs, 0–99,999,999**. The default `tenant_id < 100` query must return **10,000,000 rows**, `id_sum = 499995495000000`, and NULL counts from amount `% 23` / payload `% 17`. Custom `--query` does not inherit these assertions.

ArrowStream returns the six-column result with `event_time: uint32`, checked as `1704067200 + (id % 63072000)`, rather than an Arrow timestamp. Payload `hex(cityHash64(id))` suffixes have observed width 2–12 characters. Preserve the exact schema, formulas, ordering and null predicates:

```sql
CREATE DATABASE IF NOT EXISTS kelvo_bench;
CREATE TABLE IF NOT EXISTS kelvo_bench.fact_events (
  id Int64,
  tenant_id Int32,
  customer_id Int32,
  event_day Date,
  event_time DateTime,
  grp Int32,
  amount Nullable(Float64),
  payload Nullable(String)
) ENGINE = MergeTree
PARTITION BY toYYYYMM(event_day)
ORDER BY (tenant_id, event_day, grp, id);

CREATE TABLE IF NOT EXISTS kelvo_bench.tenant_dim (
  tenant_id Int32,
  tenant_name String,
  tier Int32
) ENGINE = MergeTree
ORDER BY tenant_id;

INSERT INTO kelvo_bench.tenant_dim
SELECT toInt32(number), concat('tenant-', toString(number)), toInt32(number % 4)
FROM numbers(1000);
```

Insert non-overlapping `(START, COUNT)` batches from START=0 until 100,000,000 rows exist. Verify IDs/counts first; never append to a different existing table.

```sql
INSERT INTO kelvo_bench.fact_events
SELECT
  toInt64(number) AS id,
  toInt32(number % 1000) AS tenant_id,
  toInt32((number * 17) % 10000000) AS customer_id,
  toDate('2024-01-01') + toIntervalDay(number % 730) AS event_day,
  toDateTime('2024-01-01 00:00:00') + toIntervalSecond(number % 63072000) AS event_time,
  toInt32((number * 13) % 4096) AS grp,
  if(number % 23 = 0, NULL, toFloat64((number * 37) % 100000) / 100.0) AS amount,
  if(number % 17 = 0, NULL,
     concat('event-', toString(number), '-c', toString((number * 17) % 10000000), '-',
            substring(hex(cityHash64(number)), 1, 12))) AS payload
FROM numbers(START, COUNT);
```

## Comparison boundaries

The reference uses PyArrow 25.0.1 and `clickhouse/clickhouse-server@sha256:77475011e36a9cbc6f107c59b7a925f0e810d9d7416df35083b5c9382466ff2d`, capped at 4 GiB/1.5 CPUs. Record deviations and build separately from timed work.

`warm-uncontrolled` is the default cache label. Use `cold-controlled` only with independently documented cache control. Review reports/environment before publishing performance claims; [federation](federation-capacity.md) and [analytical workflows](analytics-workflow-benchmarks.md) show measured examples.

# ClickHouse benchmark helper

`scripts/benchmark_clickhouse.py` measures one native ClickHouse export through the built Kelvo binary. It starts timing before the coordinator process starts and stops after the process exits and the output Arrow IPC file is fsynced. Arrow validation happens afterward, so its row counting and checksums do not inflate elapsed time.

The report contains the binary SHA-256, row count, deterministic `id` sum, Arrow schema, null counts, logical Arrow-buffer and output-file checksums, output byte count, native-query stats when available, and separate coordinator/worker Linux `/proc` RSS peaks sampled every 20 ms. The worker value is the maximum of direct children observed during the run. It is an observation, not a total process-tree allocation measure.

The helper accepts only paths and source IDs. Put source credentials in the environment variables named by the catalog before invoking it. Do not place credentials, URLs with credentials, VM addresses, or passwords in a command line, source catalog, result report, or committed file.

## Fixture

The prescribed fixture is the existing `kelvo_bench` ClickHouse fixture. It has 100,000,000 contiguous fact rows and the helper's default query selects the 10,000,000 rows where `tenant_id < 100`. For that exact default query, post-timer validation requires 10,000,000 rows, `id_sum = 499995495000000`, and null counts derived from the fixture's `% 23` amount and `% 17` payload predicates. A custom `--query` remains configurable but does not inherit those default-fixture assertions.

Use this exact schema and generator. It matches the public reference fixture; changing types, null predicates, partitioning, ordering, or formulas produces a different workload. ClickHouse ArrowStream maps this fixture's `DateTime` field to `uint32` epoch seconds rather than an Arrow timestamp. The helper therefore requires the six-column Arrow schema with `event_time: uint32` and checks `event_time = 1704067200 + (id % 63072000)` after timing. The payload suffix is `hex(cityHash64(id))`, whose observed width is 2 through 12 hexadecimal characters.

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

Insert `fact_events` in non-overlapping contiguous batches. For each batch `(START, COUNT)`, use this exact statement; begin with `START = 0` and continue until 100,000,000 rows have been inserted.

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

Before a run, verify the fixture has exactly 100,000,000 rows with IDs from 0 through 99,999,999. Do not append to a different existing table.

## Run

Build the binary separately, configure a catalog whose ClickHouse source uses environment-variable names, export those variables in the shell, then run:

```bash
python3 scripts/benchmark_clickhouse.py \
  --binary /path/to/kelvo \
  --catalog /path/to/catalog.json \
  --connection clickhouse \
  --output /path/to/result.arrow \
  --report /path/to/result.json
```

Install `pyarrow` in the benchmark environment for post-timer Arrow validation. The defaults are `--memory-mb 1024`, `--max-rows 11000000`, `--max-bytes 1073741824`, `--timeout 120s`, and `--threads 2`. The 1 GiB memory budget is deliberate: in native mode it covers both the ClickHouse query and Arrow decoder allocation budgets. A 512 MiB source budget previously failed after producing approximately 69 MB across 16 Arrow record batches for the 10M-row transfer fixture, so it is not an acceptable default for this workload.

The default cache label is `warm-uncontrolled`. It records that the source cache state was not controlled; it does not support a cold-cache or general performance claim. Use `--cache-state cold-controlled` only when the operator has independently controlled and documented cache state.

The reference runtime uses PyArrow 25.0.1 and the pinned `clickhouse/clickhouse-server@sha256:77475011e36a9cbc6f107c59b7a925f0e810d9d7416df35083b5c9382466ff2d` image with 4 GiB memory and 1.5 CPUs. Record any deviation in the report context.

No speed claim belongs in documentation or release material until runs from this helper, including their reports and environment details, have been reviewed.

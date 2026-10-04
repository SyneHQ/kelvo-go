# Measure telemetry cost

Compare telemetry settings on the same frozen Kelvo binary. This harness uses one million deterministic Parquet rows, one gateway, one worker and three TLS NATS peers. It measures a local fixture, not deployment capacity or WAN throughput.

## Workloads and comparisons

Each epoch warms and measures two CTE queries: a ten-group aggregate and a complete one-million-row transfer. Both validate every result, integer type, NULL and Arrow end marker.

| Comparison | Held constant |
| --- | --- |
| Metrics off versus on | Tracing absent |
| Tracing absent versus ratio 0 | Metrics enabled |
| Tracing absent versus ratio 0.1 | Metrics enabled |
| Tracing absent versus ratio 1 | Metrics enabled |

The collector runs in every epoch. Full mode uses six alternating AB/BA pairs per comparison: 48 epochs, 96 warmups and 96 measured queries. Startup, client validation and lease cooldown are outside request timings; inclusive epoch totals also cover shutdown.

## Run on Linux

1. Finish other capacity campaigns and independently verify their exact service/cgroup cleanup.
2. Freeze the source, binary, sandbox, collector, harness and input hashes. Use the passing [OTLP acceptance](tracing-acceptance.md) runtime.
3. Prepare private collector trust with [the certificate helper](../scripts/prepare_tracing_fixture.py); mount it read-only inside the service. Leave host trust unchanged.
4. Run the harness controls on the designated VM, then launch `--mode smoke` in a fresh bounded service.
5. Reconcile smoke results and cleanup before explicitly launching `--mode full` with its receipt and SHA-256.
6. Preserve successful and failed receipts, logs, collector ledgers and independent cleanup evidence.

[Harness](../scripts/telemetry_overhead/harness.py) · [Workloads](../scripts/telemetry_overhead/workloads.py) · [Receipt controls](../scripts/telemetry_overhead/test_harness.py)

Run the pure controls before launching a fixture:

```sh
python3 -m unittest discover -s scripts/telemetry_overhead -p 'test_*.py'
```

Both modes require 2 CPU, 6 GiB RAM, no swap and 512 tasks. Smoke runs two epochs with four warmups and four measured queries; its watchdog is 15 minutes. Full mode has a one-hour watchdog. No mode downloads packages, compiles Kelvo or launches another campaign.

Use `--help` for required paths and hashes. Full mode rejects a different source, binary, harness, workload or data identity from its passing smoke. Any incorrect answer, timeout, missing counter, forced cleanup or incomplete epoch fails the run; failed samples are never silently removed.

## Read the result

- Request duration spans submission through complete body receipt. Client Arrow validation follows it.
- Parent CPU excludes child CPU. Service CPU includes clients, brokers and the collector; asynchronous export may fall outside one request.
- Sampled process RSS and charged service memory are different observations; short peaks can be missed.
- Metrics comparison covers the whole metrics implementation, including child timings. It does not isolate one counter's cost.
- Fractional sampling is not exporter loss. Offered and lost spans stay unknown without a valid denominator.

DuckDB materializes before Arrow delivery. These comparisons cannot separate source, pure compute and network time. Smoke durations are diagnostic only; publish comparisons only after all 48 full-run epochs and outer checks pass.

## Format a reconciled full run

The [formatter](../scripts/telemetry_overhead/format_summary.py) checks all 48 epochs and recomputes the paired statistics before writing two tables. It rejects smoke, failed or inconsistent receipts; raw-evidence reconciliation remains a separate prerequisite.

```sh
python3 scripts/telemetry_overhead/format_summary.py \
  --input <reconciled-full-receipt.json> --output <new-summary.md>
```

[Six formatter controls](evidence/telemetry-formatter-controls.json) passed on the designated Linux VM. CI discovers these plus the nine harness controls.

## Recorded trials

The [first smoke](evidence/telemetry-overhead-smoke-93339e7-failed.json) stopped at the campaign guard before creating queries. Its guard searched every `systemctl` column and could match a path in DESCRIPTION; the original offending row was not retained. The corrected guard checks complete UNIT names.

The [corrected smoke](evidence/telemetry-overhead-smoke-93339e7.json) passed both epochs and all eight warmup/measured queries, with exact Arrow results, no tracing traffic and independent cleanup. All 13 VM controls passed (nine harness and four outer controls). Both attempts remain recorded.

## Recorded full comparison

All 48 epochs passed on runtime `93339e7`: 96 warmups and 96 measured queries, exact Arrow answers, reconciled collector ledgers and independent service/cgroup cleanup. Host trust and frozen inputs stayed unchanged. [Full receipt](evidence/telemetry-overhead-full-93339e7.json).

Local fixture: one worker, one million Parquet source rows, and a total service quota of 2 CPU / 6 GiB.

Warmups are excluded. Timings cover submission through complete body receipt; client Arrow validation is excluded. Tracing comparisons keep metrics enabled. Ratios are variant/baseline; above 1 means slower.

**Aggregate CTE: one million source rows, ten groups returned**

| Comparison | Baseline median (ms) | Variant median (ms) | Median paired ratio | Ratio IQR | Paired ratio min–max |
|---|---:|---:|---:|---:|---:|
| Metrics: off to on | 101.950 | 104.860 | 1.030x | 0.015 | 0.988x–1.037x |
| Tracing: off to 0% | 102.969 | 103.011 | 1.000x | 0.018 | 0.988x–1.014x |
| Tracing: off to 10% | 104.369 | 103.704 | 0.999x | 0.012 | 0.967x–1.052x |
| Tracing: off to 100% | 103.147 | 102.454 | 0.995x | 0.010 | 0.987x–1.014x |

**Transfer CTE: one million ordered rows returned**

| Comparison | Baseline median (ms) | Variant median (ms) | Median paired ratio | Ratio IQR | Paired ratio min–max |
|---|---:|---:|---:|---:|---:|
| Metrics: off to on | 301.878 | 309.207 | 1.030x | 0.080 | 0.903x–1.102x |
| Tracing: off to 0% | 303.313 | 306.794 | 1.008x | 0.040 | 0.938x–1.062x |
| Tracing: off to 10% | 301.551 | 307.980 | 1.031x | 0.087 | 0.933x–1.126x |
| Tracing: off to 100% | 308.620 | 321.455 | 1.073x | 0.102 | 0.972x–1.139x |

Six matched pairs per comparison; IQR is the 75th–25th percentile spread of paired ratios. Ratios are raw observations; IQR/min–max describe spread, not confidence intervals or statistical significance. 48 epochs, 96 measured queries and 96 warmups. These fixture measurements do not establish production capacity.

[Measured runtime 93339e79ab13](https://github.com/SYNEHQ/kelvo-go/commit/93339e79ab135b6d1c2c09da04a00f3e711fd24d).

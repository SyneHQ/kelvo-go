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
python3 -m unittest discover -s scripts/telemetry_overhead -p test_harness.py
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

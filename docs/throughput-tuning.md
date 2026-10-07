# Throughput tuning

Measure the source, row conversion, local execution and result delivery separately. More rows per second can simply mean narrower rows; report decoded bytes and encoded response bytes too.

## 1. Try larger row batches

Row-backed native connectors default to 1,024 rows per Arrow batch. Opt into a byte target for sustained exports:

```sh
kelvo query --config kelvo.yml --mode native --connection analytics \
  --sql 'SELECT * FROM events' --out events.arrow \
  --row-batch-target-bytes 1048576
```

For clusters, retain existing fields and add this under node `policy.limits` and gateway `tenants[].policy.limits`. Follow the [policy reprovisioning procedure](cluster.md#operations-and-security):

```yaml
policy:
  limits:
    row_batch_target_bytes: 1048576
```

For `accelerate refresh/watch`, use the same key under `acceleration.datasets[].limits`.

- Zero preserves the default. Valid targets are 1 KiB–64 MiB, capped by existing result/batch budgets and 65,536 rows.
- The target estimates decoded bytes; it is not an RSS cap. One larger row may exceed the target while still respecting hard limits.
- There is no timer flush. Larger batches can delay first results from slow sources. Sink calls remain synchronous and failures stop delivery.
- This changes row-to-Arrow builders, including native relational/warehouse paths. Direct Arrow, DuckDB and external adapters retain their own batch contracts.

[LZ4](usage.md#opt-in-result-compression) is a separate opt-in. Compression never raises decoded-byte or memory limits.

## 2. Read the right measurements

Authenticated worker `/metrics` includes fixed-size IPC diagnostics:

| Metric suffix (`kelvo_worker_ipc_…`) | Meaning |
| --- | --- |
| `read_validate_wait_seconds` | Parent read time excluding sinks; includes pipe waits, validation and decode |
| `sink_seconds` | Synchronous schema/batch callbacks; includes encoding and downstream waits |
| `input_bytes_total` | Child IPC bytes read, including framing and rejected trailing-byte probes |
| `decoded_bytes_total` | Validated buffers offered to sinks, including failed callbacks |
| `reads_total` | Complete, incomplete or malformed observations |

`complete` means valid child EOS/EOF and successful callbacks. Process outcome, final encoding, durable commit and client receipt remain separate checks. Unreached sink timing stays unknown.

These intervals overlap existing worker/child timings; do not add them together. They do not isolate source execution, pure CPU time or network transfer. Metrics reset on restart and store no SQL, tenant IDs or per-batch samples.

## 3. Keep workload budgets explicit

[Admission](workload-admission.md) preserves FIFO order within each workload class. A smaller new request cannot overtake an older request in that class. Up to three class heads are notified when capacity changes.

Interactive reserves remain available while background work waits. Class ceilings do not guarantee cross-class or tenant fairness; bound upstream queues and retain process containment.

## Reproduce the microbenchmarks on Linux

```sh
GOMAXPROCS=2 go test -tags duckdb_arrow -run '^$' \
  -bench 'Benchmark(WriterBatching|WorkerIPCTransit|WorkerIPCAttribution|ObserveIPCTransfer|AdmissionContended)$' \
  -benchmem -benchtime=500ms -count=5 \
  ./internal/sources/rowarrow ./internal/worker ./internal/telemetry ./internal/admission
```

These isolate conversion, IPC and admission. Inputs are already in memory; results use discard/counting sinks. Source databases, network, TLS and durable exports are outside these rates. [Capacity methodology](federation-capacity.md) and multi-host qualification are separate.

## Recorded result

On `124f0a9`, five alternating before/after pairs tested the schema-copy fix with **65,536 decoded 32-byte rows**, default batching and uncompressed IPC. Two Go threads, a two-CPU quota and 6 GiB cap on a shared Azure VM:

| Median per operation | Before | After |
| --- | ---: | ---: |
| Conversion + IPC time | 8.437 ms | 4.270 ms |
| Nominal input rate | 248.58 MB/s | 491.17 MB/s |
| Allocated bytes | 18.61 MB | 7.08 MB |
| Allocations | 69,794 | 4,257 |

That is **1.98× the conversion throughput** and 62% fewer allocated bytes for this fixture, with no batching opt-in. Allocated bytes are cumulative allocation volume, not peak RSS.

The separate 51-case matrix had five repeats per case. Larger batches were not consistently faster: 1 MiB slowed all three conversion/IPC widths; 4 MiB helped 256/1,024-byte rows but slowed 32-byte rows. Keep the default unless your measurements justify changing it.

IPC telemetry added roughly 4.4–5.6% elapsed time in the lightweight discard/plain-IPC cases. LZ4 reduced wide high-entropy output by less than 0.5% while making its memory-only path over 3× slower. Neither result predicts network break-even.

[Evidence, skipped gates and medians](evidence/throughput-tuning-124f0a9.json) · [All 255 benchmark measurements](evidence/throughput-microbenchmarks-124f0a9.txt). These are not GB/s database or client-delivery claims.

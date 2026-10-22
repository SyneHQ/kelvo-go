# Observability

A Kelvo node serves Prometheus text metrics at `/metrics` on its mTLS listener (gateway identity required). Metrics are process-local and bounded: no SQL, delegation, DSN, secret or source name is ever a label or log field.

## Metrics

Histograms use fixed buckets (seconds): .001 .005 .01 .05 .1 .5 1 5 10 30 60 300. `kind` is `query` or `refresh`.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `kelvo_reads_total` | counter | `tenant`, `outcome` (success, error, canceled), `class` | Reads completed on this node. `class` is `none` on success, else capacity, draining, deadline, cancelled, request, unavailable or execution (the same classes as the `kelvo read failed` log line). At most 256 distinct tenants; further tenants are counted as `other`. |
| `kelvo_read_duration_seconds` | histogram | `tenant` | Node read duration from execution start to completion, same tenant cap. |
| `kelvo_reads_in_flight` | gauge | none | Reads currently executing on this node. |
| `kelvo_slow_read_log_dropped_total` | counter | none | Slow-read log lines dropped by the rate limit (10 per second). |
| `kelvo_stall_dumps_total` | counter | none | Goroutine dumps written by the stall detector. Present only when `observability.stall_dump` is set. |
| `kelvo_jobs_completed_total` | counter | `kind`, `outcome` | Terminal job outcomes in this process. |
| `kelvo_jobs_rejected_total` | counter | `kind`, `reason` (capacity, draining) | Jobs refused before execution. |
| `kelvo_job_queue_wait_seconds` | histogram | `kind` | Worker admission wait of terminal jobs; excludes distributed dispatch wait. |
| `kelvo_job_duration_seconds` | histogram | `kind` | Worker call duration including setup and transfer, excluding admission wait. |
| `kelvo_worker_phase_seconds` | histogram | `kind`, `phase` | Disjoint local worker phases; `execution_delivery` includes overlapping computation and IPC delivery. |
| `kelvo_worker_resolve_seconds` | histogram | `kind` | Source credential and connection resolve; part of `node_admission`. |
| `kelvo_worker_first_batch_seconds` | histogram | `kind` | Executor entry to first decoded Arrow record. |
| `kelvo_worker_sink_seconds` | histogram | `kind` | Cumulative synchronous sink callbacks; a subset of `execution_delivery`. |
| `kelvo_child_timing_reports_total` | counter | `kind`, `status` | Child timing availability after process wait. |
| `kelvo_child_stage_unknown_total` | counter | `kind`, `lane`, `stage`, `reason` | Child stages without duration observations. |
| `kelvo_child_worker_stage_seconds` | histogram | `kind`, `stage` | Child worker stages (setup, executor_call, ipc_finalize, cleanup). |
| `kelvo_child_duckdb_stage_seconds` | histogram | `kind`, `stage` | Child DuckDB stages (engine_setup, materialization, arrow_drain). |
| `kelvo_worker_ipc_reads_total` | counter | `kind`, `status` | Parent IPC read attempts. |
| `kelvo_worker_ipc_read_validate_wait_seconds` | histogram | `kind` | Parent IPC wait, validation and decode time excluding sinks. |
| `kelvo_worker_ipc_sink_seconds` | histogram | `kind` | Synchronous IPC sink callbacks including downstream waits. |
| `kelvo_worker_ipc_sink_unknown_total` | counter | `kind` | IPC reads that never reached a sink callback. |
| `kelvo_worker_ipc_input_bytes_total` | counter | `kind`, `status` | Child pipe bytes read, including Arrow framing. |
| `kelvo_worker_ipc_decoded_bytes_total` | counter | `kind`, `status` | Validated Arrow buffer bytes offered to sinks. |
| `kelvo_worker_ipc_batches_total` | counter | `kind`, `status` | Validated Arrow batches offered to sinks. |
| `kelvo_read_adapter_slim_active` | gauge | none | 1 while read workers launch on the slim relational adapter, 0 after it failed its launch check. |
| `kelvo_read_adapter_slim_fallbacks_total` | counter | none | Read worker launches run on the full adapter because the slim one was unusable. |

There is no warm-pool metric yet. Warm-pool size and queue wait are not exported; this is a known gap.

## Configuration

Both blocks live in the node config and are off by default.

```yaml
observability:
  slow_read_threshold: 2s   # 0 or absent = off; maximum 10m
  stall_dump:
    after: 30s              # 1s to 1h
    dir: /var/lib/kelvo/stall   # absolute, clean path
```

### Slow-read log

A read that takes at least `slow_read_threshold` logs one line:

```
kelvo slow read: tenant="acme" job="..." mode="federated" sources="pg1,pg2" duration_ms=2310 rows=10 bytes=4096 outcome=success class=none
```

It never contains SQL, parameters, delegation, DSNs or error text. Source IDs are included only if they are well-formed catalog IDs (at most 16). At most 10 lines are written per second; the excess is counted in `kelvo_slow_read_log_dropped_total`.

### Stall dump

When the node has reads in flight and none has completed for `after`, it writes one goroutine dump (`runtime/pprof` goroutine profile, debug=2) to `dir/kelvo-stall-<timestamp>.txt`. The directory is created `0700` and files are `0600`. At most one dump is written per 10 minutes and only the newest 5 files are kept. There is no HTTP pprof endpoint. Dumps contain stack traces and may reveal internal function names and addresses; protect and rotate them like logs.

## Grafana

`deploy/grafana/kelvo.json` is a minimal dashboard (import it in Grafana, select your Prometheus data source) with read latency p50/p95 by tenant, error rate by class, in-flight reads and stall dumps. It has no warm-pool panel because that metric does not exist yet.

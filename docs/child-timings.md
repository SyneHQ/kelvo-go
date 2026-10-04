# Worker stage metrics

Worker metrics include subprocess stages. They follow `--metrics`; disabling metrics also disables child recording. Tracing alone does not enable it.

| Metric family | Stages |
| --- | --- |
| `kelvo_child_worker_stage_seconds` | `setup`, `executor_call`, `ipc_finalize`, `cleanup` |
| `kelvo_child_duckdb_stage_seconds` | `engine_setup`, `materialization`, `arrow_drain` |

Worker stages are disjoint. DuckDB stages sit **inside** `executor_call`; never add them to the worker durations.

- **Setup:** validated input through executor construction; excludes input decoding and process launch.
- **Executor call:** the complete executor invocation, including engine cleanup. Native drivers remain combined.
- **IPC finalization:** successful Arrow end-marker writing. Failed execution does not record this stage.
- **Cleanup:** IPC abort/release, native close and context cleanup. The report is emitted afterward.
- **Engine setup:** DuckDB preparation through entry to `QueryContext`. Failed preparation stays unknown.
- **Materialization:** the exact `QueryContext` call, including errors. DuckDB completes execution before Arrow delivery.
- **Arrow drain:** schema and record delivery through sink callbacks; excludes reader release. This includes consumer waiting, not just transport.

These are wall durations, not separate source, compute or network costs. Only completed intervals enter histograms; `kelvo_child_stage_unknown_total` counts unavailable stages. `kelvo_child_timing_reports_total` distinguishes observed, missing, malformed and terminated reports. Terminated means an unusable report after an OS-signaled exit; cancellation alone does not prove that.

Reports contain seven fixed intervals from one child-local monotonic clock. The parent checks them against its own process start-to-wait duration without aligning timestamps. Optional diagnostics are capped at 2 KiB and dropped before the existing 64 KiB outcome limit. Invalid diagnostics do not change the query result; an explicit `timing: null` is malformed, while an absent field is missing.

Parent and child run the same executable; this is not an independently versioned cross-service protocol. Timings remain outside query statistics, durable jobs and trace context. Metrics use fixed labels, with no SQL, source or tenant identifiers.

For parent phases and access control, see [operations](operations.md#diagnostics). Workload overhead and real multi-process trace acceptance remain tracked in [#24](https://github.com/SyneHQ/kelvo-go/issues/24).

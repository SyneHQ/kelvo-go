# Optional lifecycle tracing

Add `tracing` to a cluster node's YAML to export OpenTelemetry OTLP/HTTP spans. It is disabled by default; measure overhead before raising sampling.

```yaml
tracing:
  endpoint: https://collector.example.com/v1/traces
  token_env: KELVO_TRACE_TOKEN
  sample_ratio: 0.1
  queue_size: 64
  export_timeout: 5s
```

1. Use the collector's complete HTTPS trace path and system-trusted certificate. TLS 1.3 is required; redirects, ambient proxies and custom trust overrides are disabled.
2. Optionally name a parent-only `KELVO_` bearer-token variable. Restart the worker to rotate it.
3. Set a sample ratio from 0–1, queue size from 1–1,024 (default 64), and timeout up to 30s (default 5s).

Full queues drop spans; export does not retry. Responses are bounded (16 KiB headers, 64 KiB body); shutdown flushes for at most five seconds. Ambient `OTEL_EXPORTER_OTLP_*` settings cannot redirect export. This is best-effort telemetry, not durable audit.

## What is recorded

| Span | Scope |
| --- | --- |
| `kelvo.query` | Local worker call through cleanup, excluding gateway consumption confirmation |
| `kelvo.refresh` | Refresh through publication and cleanup; no duplicate extraction traces |
| `kelvo.phase.*` | Reached query phases: validation, node/source admission, prepare, execution/delivery, cleanup |
| `kelvo.admission` | Aggregate refresh node-admission interval, not a full refresh timeline |

Query phase children do not overlap. Execution/delivery combines source work, local computation, IPC and sink calls; those operations can overlap internally. Early failures retain deferred cleanup in the phase where they occur.

[Metrics](operations.md#diagnostics) also record first decoded record and cumulative sink-callback time. Neither proves client receipt. Sink time is already inside execution/delivery, excludes the outer HTTP end marker, and must not be added to phase totals or called pure network time.

Only fixed kind/outcome attributes and `service.name=kelvo` are exported. SQL, parameters, IDs, source/tenant names, secrets, raw errors, baggage and incoming trace headers are excluded. Environment resource attributes are removed before queueing.

There is no distributed trace continuity, JetStream queue timing or client-claim timing. Admission rejections remain metrics rather than execution spans. DuckDB still materializes before Arrow delivery; queue tests do not establish throughput or memory capacity.

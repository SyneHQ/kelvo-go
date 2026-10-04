# Optional lifecycle tracing

Add `tracing` to gateway and node YAML to export OpenTelemetry OTLP/HTTP spans. It is disabled by default; measure overhead before raising sampling.

```yaml
tracing:
  endpoint: https://collector.example.com/v1/traces
  token_env: KELVO_TRACE_TOKEN
  sample_ratio: 0.1
  queue_size: 64
  export_timeout: 5s
```

1. Use the collector's complete HTTPS trace path and system-trusted certificate. TLS 1.3 is required; redirects, ambient proxies and custom trust overrides are disabled.
2. Optionally name a parent-only `KELVO_` bearer-token variable. Restart that service to rotate it.
3. Set a sample ratio from 0–1, queue size from 1–1,024 (default 64), and timeout up to 30s (default 5s).

Full queues drop spans; export does not retry. Responses are bounded (16 KiB headers, 64 KiB body); shutdown flushes for at most five seconds. Ambient `OTEL_EXPORTER_OTLP_*` settings cannot redirect export. Use [audit](durable-audit.md) for durable records.

The gateway creates trace context after query authorization and stores it with the durable job. Workers import it after checking job authority. Client trace headers and ambient OpenTelemetry context are ignored. An unsampled parent stays unsampled; a sampled parent still obeys each service's local ratio.

Configure both services for continuity. Missing, malformed or older context falls back to an unrelated local trace. Context is optional and carries no query authority; it never appears in the result API or the ID-only dispatch message.

Older writers and jobs near the durable state size limit can drop optional correlation. Query state takes precedence.

## What is recorded

| Span | Scope |
| --- | --- |
| `kelvo.cluster.submit` | Authorized durable submission and enqueue attempt |
| `kelvo.cluster.dispatch` | Local assignment work after job authorization; excludes broker queue time |
| `kelvo.cluster.result_wait` | One authorized gateway result request waiting for assignment |
| `kelvo.cluster.relay` | Gateway result claim and relay through its completion checks |
| `kelvo.query` | Local worker call through cleanup, excluding gateway consumption confirmation |
| `kelvo.refresh` | Refresh through publication and cleanup; no duplicate extraction traces |
| `kelvo.phase.*` | Reached query phases: validation, node/source admission, prepare, execution/delivery, cleanup |
| `kelvo.admission` | Aggregate refresh node-admission interval, not a full refresh timeline |

Query phase children do not overlap. Execution/delivery combines source work, local computation, IPC and sink calls; those operations can overlap internally. Early failures retain deferred cleanup in the phase where they occur.

Cluster spans can overlap worker spans and each other. Do not add their durations. Correlation links activity across services; it does not correct clock differences between hosts.

[Metrics](operations.md#diagnostics) also record first decoded record and cumulative sink-callback time. Neither proves client receipt. Sink time is already inside execution/delivery, excludes the outer HTTP end marker, and must not be added to phase totals or called pure network time.

Only fixed kind/outcome attributes and `service.name=kelvo` are exported. SQL, parameters, query IDs, source/tenant names, secrets, raw errors, baggage and incoming trace headers are excluded. Environment resource attributes are removed before queueing.

## Still unmeasured

JetStream queue time, assignment-to-claim delay and separate source/compute costs remain unknown. [Child stage metrics](child-timings.md) separate worker setup, execution, IPC finalization and cleanup, with nested DuckDB intervals; these are not child spans. Admission rejections remain metrics rather than execution spans. DuckDB materializes before Arrow delivery. Distributed tracing overhead and deployment capacity need workload measurements; [#24](https://github.com/SyneHQ/kelvo-go/issues/24) tracks those gates.

## Validation

[Azure receipt](evidence/distributed-tracing-f2b9793.json), source `f2b9793`: 52 tests plus parser fuzz seeds, 106 test/subtest pass events, race checks, vet and build passed. No failures or skips. CI also runs the tracing package with the race detector.

Coverage includes principal/claim isolation, forged context, optional-state compatibility, local sampling limits, stalled exporter shutdown and durable Arrow completion. The private-network fixture used 1 CPU/3 GiB; its service and cgroup cleanup were independently verified. Its [recorded overlap](evidence/distributed-tracing-overlap-f2b9793.json) with the separate `0544d5f` sustained campaign lasted 53.926 seconds. This is component evidence. The later [real CLI OTLP gate](tracing-acceptance.md#recorded-result) verifies cross-gateway continuity, exact Arrow values, privacy and exporter outages on `93339e7`. Workload overhead remains unmeasured.

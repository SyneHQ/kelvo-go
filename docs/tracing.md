# Optional lifecycle tracing

Tracing is disabled unless a cluster worker's YAML includes `tracing`. It uses
standard OpenTelemetry OTLP/HTTP protobuf export. Start with a modest sampling
ratio and measure overhead on the actual deployment:

```yaml
tracing:
  endpoint: https://collector.example.com/v1/traces
  token_env: KELVO_TRACE_TOKEN
  sample_ratio: 0.1
  queue_size: 64
  export_timeout: 5s
```

`token_env` is optional. When supplied, it names a `KELVO_` environment variable
in the trusted worker parent containing the collector's bearer token. Kelvo
neither logs the token nor forwards it into query subprocesses. Tokens are read
when the tracing exporter starts; restart the worker to rotate this token.

The endpoint must use HTTPS with a certificate trusted by the system roots.
User information, query strings and fragments are rejected. Kelvo requires
TLS 1.3, disables HTTP redirects and ambient HTTP proxies, and explicitly sets
the endpoint, headers, timeouts and TLS client. Ambient `OTEL_EXPORTER_OTLP_*`
settings cannot redirect export, add headers, enable plaintext or replace the
trusted certificate roots. No insecure TLS or custom-CA option is exposed.
Use a certificate installed in the system trust store when operating a private
collector. Specify the collector's complete trace path, usually `/v1/traces`.

The queue defaults to 64 spans and accepts 1–1,024. Export timeout defaults to
five seconds and must be positive and at most 30 seconds. Sampling accepts
0–1; zero records no spans. A full queue drops spans instead of waiting for
collector capacity. Export retries are disabled. Collector response headers are capped at 16 KiB
and response bodies at 64 KiB. Worker shutdown allows at
most five additional seconds to flush traces, then reports a sanitized failure.
This is best-effort observability, not a durable audit or billing ledger.

## What is recorded

- `kelvo.query`: a worker executor call, including setup, source admission,
  computation, transfer and cleanup. It excludes subsequent gateway consumption
  confirmation; success is not proof that a client received the entire result.
- `kelvo.refresh`: one refresh attempt through snapshot publication and resource
  cleanup. Nested extraction workers do not create duplicate refresh traces.
- `kelvo.phase.validation`, `kelvo.phase.node_admission`,
  `kelvo.phase.source_admission`, `kelvo.phase.prepare`,
  `kelvo.phase.execution_delivery` and `kelvo.phase.cleanup`: reached query worker
  phases, using ordered offsets from the same monotonic clock. Missing phases
  produce no span. Node and source admission are separate; preparation covers
  snapshot resolution, credentials and subprocess configuration. Execution and
  delivery covers process launch, source/local computation, Arrow decoding and
  synchronous sink calls. Cleanup starts after the IPC read returns and includes
  child reaping, lease release and temporary-file removal. An early failure
  before IPC teardown remains in the phase where it occurred, including its
  deferred cleanup.
- `kelvo.admission`: refreshes retain this aggregate outer node-resource
  acquisition interval. Source quota acquisition remains inside refresh duration.
  It is positioned at the start of the refresh call and is not an exact refresh
  stage timeline. Nested extraction workers do not emit separate stage spans.

The parent span covers the complete local call, including admission time. Query
phase children partition that local call without overlapping one another; they
replace the former aggregate query admission child. The duration passed to the
recorder excludes measured admission time, which is added back to obtain the
parent end timestamp. Execution/delivery is deliberately one combined phase:
source fetching, local computation and IPC output can overlap and are not
independently measurable through the worker interface. The pinned DuckDB path
still materializes execution before Arrow delivery. None of these spans measures
JetStream dispatch wait, time spent waiting for a client to claim results,
gateway delivery, or distributed trace continuity.

[Worker diagnostics](operations.md#diagnostics) also expose fixed phase
histograms, time from executor entry to the first decoded record, and cumulative
schema/record sink-callback time. First-record availability precedes sink delivery;
it is not proof of client receipt. Results that produce no record have no
first-record observation. Sink time is a subset of execution/delivery and can
include encoding, downstream backpressure or a snapshot writer; never add it to
phase totals or label it pure network time. Sink timing excludes the outer HTTP Arrow end-of-stream write. Metrics record reached phases on failures
and admission rejections; rejection events still do not create execution spans.

Spans contain only fixed `kelvo.kind` and `kelvo.outcome` attributes and a fixed
`service.name=kelvo` resource. Outcomes are `success`, `error` and `canceled`.
They do not contain SQL, parameters, query IDs, source names, tenant identifiers,
credentials, result previews, raw errors, request baggage or incoming trace
headers. Kelvo starts independent local traces rather than claiming distributed
trace continuity. Resource attributes supplied through the OpenTelemetry
environment are removed into detached snapshots before entering the export
queue; the SDK still reads its normal process environment during initialization.

Rejected node resource reservations remain rejection metrics and do not become
execution spans. Tracing is independent of metrics and optional execution
history. Measurements must establish any claimed overhead or micro-VM capacity;
passing the bounded-queue tests is not a throughput benchmark.

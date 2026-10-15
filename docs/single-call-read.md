# Single-call transient read

`POST /v1/queries/stream` is submit, wait and fetch in one request. The body is
the same JSON as `POST /v1/queries`, including the optional `bypass_cache`.

- Same audit, authorization, tenant query slots and limits as the three calls.
  The gateway composes the existing submit and relay code; it adds no new
  execution path. The three-call path is unchanged.
- Success: `200`, `Content-Type: application/vnd.apache.arrow.stream`,
  `Kelvo-Result-Completion: durable-eos-v1`, `Kelvo-Query-Id: <id>`. The final
  Arrow EOS is released only after the durable `Succeeded` commit. The body is
  followed by the `Kelvo-Query-Receipt` HTTP trailer:
  `{"id":..., "state":"succeeded", "stats":{...}}`. A client must treat a missing
  or inconsistent receipt as a failed read. `client.QueryStream` does.
- Above capacity the request waits up to 5 s at each admission point (request
  slot, tenant query slot, result slot), then gets `429` with `Retry-After: 1`
  and code `RESOURCE_EXHAUSTED`. The wait is a constant until phase 4 moves
  capacity settings.
- A stream abandoned before it finishes cancels its job; it does not hold its
  tenant slot until expiry.
- A node without the endpoint answers `404` or `405`. `client.QueryStream`
  returns `client.ErrStreamUnsupported` before anything was submitted, so the
  caller can fall back to `Query`.
- `bypass_cache` asks for a fresh execution on an explicit refresh. It is part
  of the request, excluded from the delegation digest, and ignored until a
  result cache exists. An older node rejects the unknown field with `400`; a
  caller falling back to an older node must clear it.

Not covered: the standalone `httpapi` server and application mode do not serve
this endpoint; they answer `404`.

## Node failure log and stage metrics

A failed read logs one line: `kelvo read failed: code=… class=… stage=…
duration_ms=…`. It carries no SQL, parameters, credentials or error text.
`stage` is `validation`, `admission`, `resolve`, `prepare`, `execute` or
`drain`. Connect and execute both run in the one-shot child, so they report as
`execute` until the first batch and `drain` after it.

`kelvo_worker_phase_seconds` already exports validation, node and source
admission, prepare, execution_delivery and cleanup. The credential and
connection resolve ran unmeasured inside `node_admission`; it is now exported as
`kelvo_worker_resolve_seconds{kind}`.

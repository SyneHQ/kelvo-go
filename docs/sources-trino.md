# Trino and Presto native queries

Query an operator-managed Trino or Presto coordinator over HTTPS. Kelvo does not start or manage those clusters; federation runs inside the selected remote engine.

1. Provision a bearer credential and an authorized read-only session user.
2. Register `type: trino` or `type: presto`:

```yaml
sources:
  - id: warehouse
    type: trino
    url_env: KELVO_SOURCE_TRINO_URL
    token_env: KELVO_SOURCE_TRINO_TOKEN
    username_env: KELVO_SOURCE_TRINO_USER
    options:
      catalog: lake
      schema: analytics
```

3. Set an HTTPS origin such as `https://coordinator.example.com:8443` and submit a native request:

```json
{
  "mode": "native",
  "connection_id": "warehouse",
  "sql": "SELECT region, SUM(amount) AS revenue FROM sales GROUP BY region"
}
```

Origins reject paths, userinfo, queries and fragments. Certificates use system roots. `catalog` and `schema` are optional simple names. Password/browser OAuth flows, custom certificates, role/session mutation and impersonation are unsupported. The coordinator must authenticate the token and authorize the user; a username header is insufficient.

Only one SELECT/WITH is accepted. Parameters, MongoDB payloads and a `sources` list are rejected. Kelvo does not append LIMIT, copy data automatically or expose this source as a DuckDB federation table. Database grants, including function permissions, enforce read access.

## Protocol and bounds

Kelvo sends `POST /v1/statement`, then follows `nextUri` until absent. Pagination must stay on the configured HTTPS origin, protocol-specific statement path and query ID. Schema changes, repeated URLs, malformed rows, update results and server errors fail; human-readable status text does not determine completion.

Supported paths follow Trino 483 and Presto 0.295, including Presto's `slug`. Other proxy rewrites/older shapes, redirects, external spooling, `binaryData`, binary modes and arbitrary response headers are rejected. Credentials stay on the original coordinator. Failed submissions are never replayed.

| Limit | Value |
| --- | --- |
| Pages / pagination URL | 8192 / 2048 bytes |
| Response, including decompression | Smaller of 32 MiB and one quarter of client memory |
| Results | Query deadline and Arrow row/byte limits; overruns fail |

Pages are buffered with exact `json.Number` decoding, then delivered in synchronous Arrow batches. Slow consumers stop further fetching after bounded staging fills; this does not cap remote execution memory.

After a trusted next URI is known, errors trigger best-effort DELETE with an independent two-second deadline. A lost/malformed/oversized initial reply may leave no cancel handle. The [worker cancellation grace](usage.md#native-cancellation-and-remote-cleanup) may interrupt that two-second cleanup attempt. Configure coordinator timeouts, resource groups and memory/concurrency limits. Partial output is not a completed analysis.

## Result types

Supported: signed integer widths, `real`/Float32, `double`, Boolean, varchar/char, base64 varbinary, date, decimal(p,s) up to 38 digits, naive timestamp(p) up to precision 9, and all-NULL `unknown`.

Exact widths, decimals and NULLs are retained. Rounding, overflow, nonfinite floats, malformed dates and mismatched JSON types fail. Timestamps use ms/µs/ns according to precision; Trino receives `PARAMETRIC_DATETIME`, and the session timezone is UTC.

Zoned timestamps, times, arrays, maps, rows, JSON, UUID, intervals and other unlisted types require an explicit supported cast.

## Validation

TLS protocol fixtures cover both engines, exact types, pagination, cancellation, limits and credential boundaries, including race tests. Live cluster acceptance and throughput remain unverified.

References: [Trino protocol](https://trino.io/docs/current/develop/client-protocol.html), [Trino queued](https://github.com/trinodb/trino/blob/483/core/trino-main/src/main/java/io/trino/dispatcher/QueuedStatementResource.java), [Trino executing](https://github.com/trinodb/trino/blob/483/core/trino-main/src/main/java/io/trino/server/protocol/ExecutingStatementResource.java), [Presto headers](https://github.com/prestodb/presto/blob/0.295/presto-client/src/main/java/com/facebook/presto/client/PrestoHeaders.java), [Presto queued](https://github.com/prestodb/presto/blob/0.295/presto-main/src/main/java/com/facebook/presto/server/protocol/QueuedStatementResource.java), [Presto executing](https://github.com/prestodb/presto/blob/0.295/presto-main/src/main/java/com/facebook/presto/server/protocol/ExecutingStatementResource.java), [Presto results](https://github.com/prestodb/presto/blob/0.295/presto-client/src/main/java/com/facebook/presto/client/QueryResults.java).

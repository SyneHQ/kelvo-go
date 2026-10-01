# Trino and Presto native queries

Kelvo connects to an operator-managed Trino or Presto coordinator using its HTTP
statement protocol. The engines stay separate services; Kelvo does not start,
embed, or manage their clusters. Set `type: trino` for Trino or `type: presto`
for Presto. The adapter uses the corresponding `X-Trino-*` or `X-Presto-*`
headers and validates each protocol's pagination path.

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

The URL must be an HTTPS origin, such as `https://coordinator.example.com:8443`,
without a path, embedded credentials, query string, or fragment. The token is a
Bearer credential accepted by the coordinator; the username is the session user.
The coordinator must authenticate the token, authorize that user, and grant only
the intended read access. The username header alone is not authentication.
Catalog and schema options are optional simple names. Password, OAuth browser
flows, custom certificate files, role/session mutation, and impersonation flows
are not implemented. TLS certificates must verify against the system roots.

Submit a native query using the normal authenticated Kelvo endpoint:

```json
{
  "mode": "native",
  "connection_id": "warehouse",
  "sql": "SELECT region, SUM(amount) AS revenue FROM sales GROUP BY region"
}
```

Queries use the shared conservative single-SELECT/WITH syntax gate. SQL
parameters, MongoDB payloads, and a `sources` list are rejected. Database grants
remain the read-only enforcement boundary, including permissions on functions
that a SELECT might invoke. Kelvo does not rewrite queries to append a LIMIT or
automatically copy source data. Federation happens inside the user's Trino or
Presto cluster; this native adapter does not expose that cluster as a DuckDB
federation table.

## Protocol and bounds

Kelvo sends SQL text in `POST /v1/statement`, then fetches each JSON page using
the returned `nextUri`. It only follows HTTPS URLs on the configured origin
whose statement path contains the same query ID. Repeated page URLs, changed
query IDs, changed schemas, malformed rows, update results, and server errors
fail the query. A final response without `nextUri` ends retrieval; the human
readable server status is not used to infer completion.

Pagination accepts the documented queued/executing paths from Trino 483 and
Presto 0.295, including Presto's `slug` parameter. Alternate proxy rewriting or
older endpoint shapes fail explicitly. HTTP redirects, other origins, external
spooling segments, Presto `binaryData`, binary result modes, and arbitrary
response headers are not followed. Tokens and configured headers are sent only
to the original coordinator. SQL is not automatically resubmitted after an HTTP
failure; transient coordinator/load-balancer errors return an error to the
caller rather than potentially starting duplicate work.

Retrieval is bounded by the request deadline, 8192 pages, 2048-byte pagination
URLs, and the shared HTTP response cap: the smaller of 32 MiB and one quarter of
the configured client memory budget. The cap applies after decompression as well.
Each JSON page is materialized before conversion, with exact JSON numbers retained
as `json.Number`. Arrow batches are delivered synchronously; a blocked result
consumer prevents additional page retrieval after the bounded staging buffer
fills. The source engine can execute and produce results incrementally, but
each HTTP page is buffered locally.

Client row and byte limits fail explicitly. If an error, cancellation, sink
failure, or output limit occurs after a trusted next URI is available, Kelvo
issues a best-effort DELETE to that URI with an independent two-second deadline.
If the initial response is lost, malformed, or exceeds its byte cap, no trusted
handle may be available to cancel. Configure coordinator-side query timeouts,
resource groups, concurrency limits, and memory policies separately. The Kelvo
client budget does not limit Trino/Presto server memory. Partial output followed
by an error must not be treated as a completed analysis.

## Result types

Supported types are `tinyint`, `smallint`, `integer`, `bigint`, `real`, `double`,
`boolean`, `varchar`, `char`, `varbinary`, `date`, `decimal(p,s)` with precision up
to 38, timezone-free `timestamp(p)` with precision up to 9, and `unknown` when
every value is NULL. Integer widths and exact decimal values are retained;
overflow, decimal rounding, nonfinite floats, invalid dates, excess timestamp
precision, and mismatched JSON value types fail explicitly. `real` uses Arrow
Float32. Binary values decode from the protocol's base64 representation.

Timezone-free timestamps retain their lack of timezone and use millisecond,
microsecond, or nanosecond Arrow units according to declared precision. Trino
receives the `PARAMETRIC_DATETIME` capability so its timestamp precision is not
silently reduced by compatibility formatting. The session timezone is UTC.
Timezone-bearing timestamps, times, arrays, maps, rows, JSON, UUID, intervals,
and other engine-specific types are currently rejected. A user can explicitly
cast unsupported values to a supported textual type in SQL when that is the
desired representation.

## Validation

VM tests use local HTTPS protocol fixtures for both engines. They verify exact
large integers, decimals, narrow integer widths, dates, timestamps, binary,
Boolean, Float32 and NULL values; empty results; pagination; schema/ID changes;
source errors; partial bodies; forbidden redirects/spooling; cancellation;
output limits; and the source/credential boundary. The shared transport's JSON
and raw-SQL paths are tested together with race detection.

These are protocol fixtures, not live Trino/Presto cluster acceptance or
throughput benchmarks. No production capacity or engine-version coverage beyond
the inspected protocol is claimed.

References: [Trino client protocol](https://trino.io/docs/current/develop/client-protocol.html),
[Trino queued protocol](https://github.com/trinodb/trino/blob/483/core/trino-main/src/main/java/io/trino/dispatcher/QueuedStatementResource.java),
[Trino executing protocol](https://github.com/trinodb/trino/blob/483/core/trino-main/src/main/java/io/trino/server/protocol/ExecutingStatementResource.java),
[Presto headers](https://github.com/prestodb/presto/blob/0.295/presto-client/src/main/java/com/facebook/presto/client/PrestoHeaders.java),
[Presto queued protocol](https://github.com/prestodb/presto/blob/0.295/presto-main/src/main/java/com/facebook/presto/server/protocol/QueuedStatementResource.java),
[Presto executing protocol](https://github.com/prestodb/presto/blob/0.295/presto-main/src/main/java/com/facebook/presto/server/protocol/ExecutingStatementResource.java),
and [Presto result representation](https://github.com/prestodb/presto/blob/0.295/presto-client/src/main/java/com/facebook/presto/client/QueryResults.java).

# Exasol native SQL

Kelvo executes Exasol SQL using the documented native WebSocket protocol version
2 over verified TLS. It uses the official `exasol-driver-go` v1.1.1 package for
DSN parsing and public metadata structs, but does not register or execute through
its `database/sql` driver. That driver's numeric decoding passes JSON numbers
through `float64`, which can round DECIMAL integers above 2^53; its fetch and
rollback operations also use background contexts.

```yaml
sources:
  - id: warehouse
    type: exasol
    dsn_env: KELVO_SOURCE_EXASOL_DSN
```

The environment variable contains an official-format DSN, for example:

```text
exa:exasol.example.com:8563;user=reader;password=YOUR_PASSWORD;encryption=1;validateservercertificate=1;autocommit=0;schema=ANALYTICS
```

Use the DSN builder when credentials contain semicolons; the official escaping
is `\;`. Credentials must be explicit, nonempty username/password values. The
constructor immediately parses and validates the DSN. It rejects disabled TLS or
certificate verification, certificate fingerprint overrides, token authentication,
compression, alternate URL paths, host lists/ranges, unknown parameters, and
`resultsetmaxrows` overrides. Supported endpoints are one DNS name or IPv4
address and a port. Options and alternate source credential fields are rejected.
The operating system certificate trust store must trust the server certificate.
No HTTP proxy environment variables, cookies, redirects, operating-system user
identity, or other ambient credentials participate in authentication.

Requests select the registered source explicitly:

```json
{"mode":"native","connection_id":"warehouse","sql":"SELECT id, amount FROM ANALYTICS.INVOICES"}
```

The connector permits one SELECT or WITH statement through the shared read-query
guard. Prepared parameters currently return `UNSUPPORTED`; values are never
interpolated into SQL. Use a database account granted only the intended read
permissions. The guard is a conservative syntax restriction, not a full SQL
parser or a replacement for database authorization. A default schema does not
restrict explicitly qualified table access.

Each execution establishes a separate WSS session. Login uses the documented RSA
PKCS#1 v1.5 password challenge inside TLS and disables compression and autocommit.
The connector requests `timestampUtcEnabled=true` and a server query timeout
rounded up from the client deadline (or a shorter explicit DSN `querytimeout`).
It verifies the autocommit and timestamp settings using `getAttributes` before
sending SQL. A successful result requires cursor close, `ROLLBACK`, and disconnect
acknowledgements. Autocommit is forced off regardless of its DSN default; the
connector never sends `COMMIT` or enables autocommit. This rollback-only session
is not a server-enforced read-only transaction mode.

Results use immutable Arrow schemas from the server's column metadata:

| Exasol type | Arrow representation |
| --- | --- |
| DECIMAL, including integer aliases | Decimal128 with the server's exact precision and scale |
| BOOLEAN | Boolean |
| DOUBLE | Finite Float64 |
| CHAR, VARCHAR | UTF-8 |
| DATE | Date32 |
| TIMESTAMP | Nanosecond timestamp without a timezone |
| TIMESTAMP WITH LOCAL TIME ZONE | Nanosecond UTC timestamp, after verifying UTC transport |

JSON decoding uses `UseNumber`, preserving all integer and decimal digits. DECIMAL
precision must be 1–36 and scale 0–precision. Plain decimal values and bounded
scientific notation are converted only when the value fits exactly; scale
rounding and overflow fail explicitly. NULLs remain NULL. Timestamps require ISO
date/time text with at most nine fractional digits and must fit Arrow's
nanosecond range. GEOMETRY, HASHTYPE, intervals, unknown types, and unsupported
metadata fail with `UNSUPPORTED` instead of being guessed from sample values.

An execute response supplies inline data or a cursor. Fetches advance an exact
integer offset and validate the column-major data lengths and declared row counts.
Repeated metadata must match. Fetch size defaults to the upstream 2,000 KiB and
is capped at half the response budget; explicit `fetchsize` values must be
1–65,536 KiB. Each response is capped at the smaller of 32 MiB and one quarter of
the configured memory budget, and total normal response payload bytes at four
times `max_bytes` plus that response budget. There are at most 100,000 pages.
The query requests `max_rows + 1` so a provider limit cannot silently turn an
oversized result into success. Arrow row and byte limits still apply, and batches
are delivered synchronously. Fetch pagination does not establish bounded server
execution memory or an engine streaming guarantee.

Cancellation sends `abortQuery` and allows up to two seconds to drain the pending
response. When the session remains usable, an independent two-second cleanup
budget closes the cursor and rolls back. A stalled, oversized, malformed, or
unrecoverable transport is closed; in that case the operation fails and does not
claim an acknowledged rollback. Success and failure both release the socket.

TLS protocol fixtures verify the RSA login and session attributes, exact decimal
and integer values, timestamps, NULLs, inline/empty results, fetch pagination,
Arrow batching, result limits, source binding, SQL restrictions, provider errors,
sink errors, abort and rollback behavior, bounded stalls, certificate validation,
and redirect rejection. These are protocol fixtures; no live Exasol database
acceptance or throughput measurement is claimed.

References: [WebSocket API version 2](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV2.md),
[login](https://github.com/exasol/websocket-api/blob/master/docs/commands/loginV1.md),
[execute](https://github.com/exasol/websocket-api/blob/master/docs/commands/executeV1.md),
[fetch](https://github.com/exasol/websocket-api/blob/master/docs/commands/fetchV1.md),
[abort](https://github.com/exasol/websocket-api/blob/master/docs/commands/abortQueryV1.md),
[session attributes](https://github.com/exasol/websocket-api/blob/master/docs/commands/getAttributesV1.md),
[official Go driver](https://github.com/exasol/exasol-driver-go).

Worker boundary: the cleanup described here requires the connector process to
remain alive. CLI/HTTP/cluster cancellation or an outer deadline can kill that
process before remote cleanup runs. See [native cancellation and remote cleanup](usage.md#native-cancellation-and-remote-cleanup).

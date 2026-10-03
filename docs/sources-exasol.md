# Exasol native SQL

Query Exasol through native WebSocket protocol v2 over verified TLS. Kelvo uses `exasol-driver-go` v1.1.1 for DSN/metadata only; its own transport preserves exact numbers and bounded cancellation.

1. Create a narrowly granted read account and register its DSN reference:

```yaml
sources:
  - id: warehouse
    type: exasol
    dsn_env: KELVO_SOURCE_EXASOL_DSN
```

2. Store an official-format DSN in the environment:

```text
exa:exasol.example.com:8563;user=reader;password=YOUR_PASSWORD;encryption=1;validateservercertificate=1;autocommit=0;schema=ANALYTICS
```

3. Submit a native SELECT/WITH:

```json
{"mode":"native","connection_id":"warehouse","sql":"SELECT id, amount FROM ANALYTICS.INVOICES"}
```

Use the DSN builder for credentials containing semicolons (escaped as `\;`). Explicit nonempty user/password and one DNS/IPv4 host plus port are required. System roots must trust the certificate. Disabled TLS/verification, fingerprint overrides, tokens, compression, URL paths, host lists/ranges, unknown parameters, `resultsetmaxrows`, other credential fields and options are rejected. Proxies, cookies, redirects and ambient credentials are unused.

Parameters are unsupported and never interpolated. A default schema does not restrict qualified table access; database grants enforce reads alongside the SQL guard.

Each query gets a separate WSS session with RSA PKCS#1 v1.5 password login inside TLS, compression/autocommit disabled, and verified `timestampUtcEnabled=true`. The server timeout rounds up from the client deadline or uses a shorter DSN `querytimeout`. Success requires cursor close, ROLLBACK and disconnect acknowledgements. Rollback-only operation is not a server read-only transaction mode.

| Exasol type | Arrow representation |
| --- | --- |
| DECIMAL, including integer aliases | Exact Decimal128; precision 1–36, scale 0–precision |
| BOOLEAN; DOUBLE | Boolean; finite Float64 |
| CHAR, VARCHAR | UTF-8 |
| DATE | Date32 |
| TIMESTAMP | Naive nanoseconds |
| TIMESTAMP WITH LOCAL TIME ZONE | UTC nanoseconds after verified UTC transport |

`UseNumber` retains digits, bounded scientific notation and NULLs. Rounding, overflow, unsupported metadata, GEOMETRY, HASHTYPE and intervals fail. Timestamps require ISO text, at most nine fractional digits and Arrow's nanosecond range.

Execute returns inline data or a cursor. Fetches validate exact offsets, column lengths, counts and repeated metadata. Limits:

| Setting | Bound |
| --- | --- |
| Fetch size | Default 2,000 KiB; explicit 1–65,536 KiB; capped at half the response budget |
| One response | Smaller of 32 MiB and one quarter of memory |
| Total normal payload | Four times `max_bytes` plus one response budget |
| Pages | 100,000 |
| Results | Request `max_rows + 1`; enforce Arrow row/byte limits without successful truncation |

Batches are synchronous; these limits do not bound source execution memory. Cancellation sends `abortQuery`, allows two seconds to drain, then uses a separate two-second cleanup budget where possible. Unusable transports close without claiming an acknowledged rollback. See [worker cleanup limits](usage.md#native-cancellation-and-remote-cleanup).

TLS fixtures cover login, exact values, pagination, limits, abort/rollback and transport security. Live Exasol acceptance and throughput remain unverified.

References: [protocol v2](https://github.com/exasol/websocket-api/blob/master/docs/WebsocketAPIV2.md), [login](https://github.com/exasol/websocket-api/blob/master/docs/commands/loginV1.md), [execute](https://github.com/exasol/websocket-api/blob/master/docs/commands/executeV1.md), [fetch](https://github.com/exasol/websocket-api/blob/master/docs/commands/fetchV1.md), [abort](https://github.com/exasol/websocket-api/blob/master/docs/commands/abortQueryV1.md), [attributes](https://github.com/exasol/websocket-api/blob/master/docs/commands/getAttributesV1.md), [Go driver](https://github.com/exasol/exasol-driver-go).

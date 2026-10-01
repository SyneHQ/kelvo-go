# Apache Ignite 2 REST SQL

Kelvo's native `ignite` connector executes read queries through the Apache
Ignite **2** REST SQL fields API. It sends `qryfldexe`, fetches subsequent pages
with `qryfetch`, and closes unfinished cursors with `qrycls`. This connector does
not implement Ignite 3's different protocol.

```yaml
sources:
  - id: grid
    type: ignite
    url_env: KELVO_SOURCE_IGNITE_URL
    username_env: KELVO_SOURCE_IGNITE_USER
    password_env: KELVO_SOURCE_IGNITE_PASSWORD
    options:
      cache_name: analytics
```

Set the URL variable to a verified HTTPS origin, such as
`https://ignite.example.com:8443`. The endpoint path is fixed at `/ignite`.
Enable Ignite 2's REST modules and TLS, or operate a trusted TLS termination
proxy. The operating system trust store must trust its certificate. URL paths,
embedded credentials, query strings, redirects, and TLS verification overrides
are rejected. HTTP proxy environment variables and ambient authentication are
not used.

Both credential environment references and nonempty values are required.
Kelvo sends the documented `ignite.login` and `ignite.password` parameters in
each `application/x-www-form-urlencoded` POST body, including fetch and cleanup.
Credentials and SQL never enter the URL. The connector does not reuse session
tokens returned by the server. `cache_name` is required and is the only option,
mapped to the protocol's mandatory `cacheName` parameter.

A native query selects this registered source:

```json
{"mode":"native","connection_id":"grid","sql":"SELECT id, amount FROM Invoice"}
```

Only one SELECT or WITH statement passes the conservative read-query guard.
Use server-enforced read permissions for the intended tables; the syntax guard
is not a SQL authorization engine, and `cache_name` is not an isolation boundary
against SQL that names other schemas. The connector rejects typed parameters
with `UNSUPPORTED`: Ignite 2 REST binds `arg1` through `argN` as strings, without
enough type information to preserve Kelvo's NULL and numeric parameter contract.
SQL is never interpolated.

The REST query API has no read-only transaction flag. Enforce read-only access
through the deployment's authorization layer; successful authentication alone
does not establish read-only permissions.

The first page establishes an immutable Arrow schema from `fieldsMetadata`.
Later pages may omit metadata; repeated metadata must match. Supported values:

| Ignite Java type | Arrow representation |
| --- | --- |
| Boolean; Byte, Short, Integer, Long | Boolean; signed 8-, 16-, 32-, 64-bit integer |
| Float, Double | Finite 32-, 64-bit floating point |
| String | UTF-8 |
| `byte[]` (`[B`) | Binary, decoded from base64 |
| `java.sql.Date` | Date32 |
| `java.sql.Timestamp` | Nanosecond timestamp without an invented timezone |
| `java.math.BigDecimal` | Exact numeric UTF-8 text, with `logical_type=decimal` and `encoding=exact_numeric_text` |
| `java.sql.Time` | Validated `HH:mm:ss` UTF-8 text with `logical_type=time` |
| UUID | Validated UUID UTF-8 text with `logical_type=uuid` |
| Void | Null |

NULL validity is preserved for every type. JSON decoding uses `UseNumber`, so
signed long values and cursor IDs above 2^53 remain exact. BigDecimal results
retain all digits, exponent notation, and trailing zeros. Ignite 2 REST metadata
does not expose decimal precision or scale, so the connector uses annotated text
instead of guessing an Arrow decimal scale from the first page. This also gives
empty and all-NULL decimal results a stable schema. Decimal text is limited to
4,096 characters. Unknown Java types, malformed values, numeric overflow,
subnanosecond timestamp precision, and timestamps outside Arrow's nanosecond
range fail explicitly. Ignite 2.17 can label a SQL UUID as binary (`[B`) while
returning UUID text. Kelvo rejects that inconsistent representation; select
`CAST(uuid_column AS VARCHAR)` to obtain an explicit text schema.

Pages request at most 1,000 rows. Result rows and Arrow bytes use the configured
query limits. Each response is bounded to the smaller of 32 MiB and one quarter
of the configured memory budget; total response bytes are bounded to four times
`max_bytes` plus that response budget. A query permits at most 100,000 pages.
An oversized page, changed cursor, changed schema, empty nonfinal page, provider
error, unsupported compressed response, or malformed JSON fails explicitly.
Arrow batches are delivered synchronously; this does not claim bounded server
execution memory or an Ignite execution streaming guarantee.

Cancellation and timeout abort the HTTP operation. Once a cursor ID is known,
failure and cancellation trigger best-effort cleanup on an independent two-second
deadline. The server closes exhausted cursors. Before the initial response gives
Kelvo a cursor ID, there is no cancellable server handle; HTTP cancellation alone
does not prove the server stopped execution. Configure Ignite's idle query cursor
timeout and route execute/fetch/close to the same node (or use sticky routing):
Ignite 2 REST cursor IDs are held by the node that executed the query.

TLS protocol fixtures cover pagination, exact values, stable schemas, NULLs,
resource limits, errors, cancellation, cleanup, source binding, read-query
restrictions, form authentication, redirects and certificate verification.
[Live Ignite 2.17 acceptance](evidence/ignite-native.json) also passed with native
authentication behind a verified-TLS fixture: 1,205-row pagination, exact decimal
and timestamp values, NULLs, empty results, UUID ambiguity rejection and explicit
text casts, and failure cleanup. All six observed cursor handles were released.
Run `scripts/ignite_acceptance.py` on a disposable Linux VM to reproduce it.
This validates the connector against one node, not authorization/RBAC, cluster
routing, throughput, or HA.

References: [Ignite 2 REST API](https://ignite.apache.org/docs/latest/restapi),
[Ignite 2.17 REST request parsing](https://github.com/apache/ignite/blob/ignite-2.17/modules/rest-http/src/main/java/org/apache/ignite/internal/processors/rest/protocols/http/jetty/GridJettyRestHandler.java),
[REST field metadata](https://github.com/apache/ignite/blob/ignite-2.17/modules/core/src/main/java/org/apache/ignite/internal/processors/rest/handlers/query/CacheQueryFieldsMetaResult.java),
[query cursor lifecycle](https://github.com/apache/ignite/blob/ignite-2.17/modules/core/src/main/java/org/apache/ignite/internal/processors/rest/handlers/query/QueryCommandHandler.java),
[date and timestamp serialization](https://github.com/apache/ignite/blob/ignite-2.17/modules/json/src/main/java/org/apache/ignite/internal/jackson/IgniteObjectMapper.java).

Worker boundary: the cleanup described here requires the connector process to
remain alive. CLI/HTTP/cluster cancellation or an outer deadline can kill that
process before remote cleanup runs. See [native cancellation and remote cleanup](usage.md#native-cancellation-and-remote-cleanup).

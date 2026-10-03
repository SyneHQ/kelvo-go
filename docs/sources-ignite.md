# Apache Ignite 2 REST SQL

Query Ignite 2 through `qryfldexe`, `qryfetch` and `qrycls`. Ignite 3 uses a different protocol and is unsupported by this connector.

1. Enable Ignite 2 REST with verified TLS or a trusted TLS proxy; route execute/fetch/close to the same node.
2. Configure credentials and the required `cache_name`:

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

3. Set an HTTPS origin such as `https://ignite.example.com:8443`, then submit:

```json
{"mode":"native","connection_id":"grid","sql":"SELECT id, amount FROM Invoice"}
```

The path is fixed at `/ignite`. Paths in the origin, embedded credentials, queries, redirects, TLS bypass and environment proxies are rejected. Both credential references and values are required; `ignite.login`/`ignite.password` travel in form POST bodies, never URLs. Returned session tokens are unused.

Only one SELECT/WITH is accepted. Typed parameters are unsupported because REST `argN` values lack the required NULL/numeric type contract. `cache_name` does not restrict cross-schema SQL, and REST has no read-only transaction flag: enforce permissions in the deployment's authorization layer.

The first page's `fieldsMetadata` fixes the schema; repeated metadata must match:

| Ignite Java type | Arrow representation |
| --- | --- |
| Boolean; Byte, Short, Integer, Long | Boolean; signed 8/16/32/64-bit integers |
| Float, Double | Finite Float32, Float64 |
| String; `byte[]` (`[B`) | UTF-8; base64-decoded Binary |
| `java.sql.Date`; `java.sql.Timestamp` | Date32; naive nanoseconds |
| `java.math.BigDecimal` | Exact numeric text; `logical_type=decimal`, `encoding=exact_numeric_text` |
| `java.sql.Time`; UUID | Validated text; `logical_type=time` or `uuid` |
| Void | Null |

NULLs, integers and cursor IDs remain exact through `UseNumber`. BigDecimal metadata lacks precision/scale, so annotated text preserves digits, exponents and trailing zeros without schema inference; text is limited to 4,096 characters. Unknown types, malformed values, overflow and subnanosecond/out-of-range timestamps fail. Ignite 2.17 may label UUID text as binary; use `CAST(uuid_column AS VARCHAR)` to avoid that rejected mismatch.

| Limit | Bound |
| --- | --- |
| Requested page size / pages | 1,000 rows / 100,000 pages |
| One response | Smaller of 32 MiB and one quarter of memory |
| Total response bytes | Four times `max_bytes` plus one response budget |
| Results | Query row/Arrow-byte limits; synchronous batches |

Changed cursors/schemas, empty nonfinal pages, provider errors, compression and malformed/oversized responses fail. These limits do not cap server execution memory.

Failures with a known cursor attempt cleanup under an independent two-second deadline. Before the first response, HTTP cancellation has no confirmed remote handle. Exhausted cursors close server-side; configure idle cursor timeouts and sticky/same-node routing. See [worker cleanup limits](usage.md#native-cancellation-and-remote-cleanup).

[Live Ignite 2.17 evidence](evidence/ignite-native.json) covers verified TLS, authentication, pagination, exact values, UUID rejection/casts and released cursors. Reproduce with `scripts/ignite_acceptance.py` on a disposable Linux VM. This is single-node acceptance; RBAC, cluster routing, HA and throughput remain unverified.

References: [REST API](https://ignite.apache.org/docs/latest/restapi), [request parsing](https://github.com/apache/ignite/blob/ignite-2.17/modules/rest-http/src/main/java/org/apache/ignite/internal/processors/rest/protocols/http/jetty/GridJettyRestHandler.java), [metadata](https://github.com/apache/ignite/blob/ignite-2.17/modules/core/src/main/java/org/apache/ignite/internal/processors/rest/handlers/query/CacheQueryFieldsMetaResult.java), [cursor lifecycle](https://github.com/apache/ignite/blob/ignite-2.17/modules/core/src/main/java/org/apache/ignite/internal/processors/rest/handlers/query/QueryCommandHandler.java), [date serialization](https://github.com/apache/ignite/blob/ignite-2.17/modules/json/src/main/java/org/apache/ignite/internal/jackson/IgniteObjectMapper.java).

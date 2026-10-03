# Native ClickHouse adapter

Execute a configured ClickHouse query and deliver its ArrowStream batches directly. `New(catalog.Config, query.Limits) (*Engine, error)` implements `query.Executor`.

1. Configure an HTTP(S) URL through `url_env`, with optional username/password environment references. Use HTTPS across untrusted networks.
2. Submit native mode with the configured `connection_id`. For selected-table joins, use the [federation bridge](../../../docs/federation.md).
3. Optionally set `options.arrow_compression: lz4_frame`; accepted values are `none` and `lz4_frame`, with no compression by default.

URLs permit only the `database` query parameter. Embedded credentials, redirects, request-selected URLs and native parameter binding are rejected. Compression affects source Arrow buffers only; it does not enable HTTP, worker-pipe or client-result compression. Measure its encoding/decoding tradeoff against the workload.

Batches are borrowed synchronously with original Arrow types and no row-map conversion. Counters have distinct scopes:

| Counter | Meaning |
| --- | --- |
| `arrow_bytes` | Delivered Arrow buffers |
| `source_wire_bytes` | Consumed HTTP 200 Arrow body, including IPC framing/partial failures; excludes discovery and HTTP/TLS/SSH overhead |
| `wire_bytes` | Kelvo's output transport |

Federation reports source wire bytes per supported scan and in total; PostgreSQL/MySQL do not currently report them. Row/byte limits apply before delivery. ClickHouse receives execution, memory, thread and result limits; the decoder separately bounds Arrow bodies/metadata. Compression never relaxes decoded limits. Codec scratch and source blocking operators still consume memory; these budgets are not an RSS guarantee.

ClickHouse 26.9.7.9 emits native DateTime as Arrow `uint32` epoch seconds without timezone metadata. DateTime64 emits an Arrow timestamp with scale-derived unit and configured timezone. Kelvo preserves these mappings. For timestamp-aware consumers, cast explicitly:

```sql
SELECT toDateTime64(event_time, 0, 'UTC') AS event_time
FROM events
```

This returns `timestamp[s, tz=UTC]`. Without the cast, Arrow cannot distinguish DateTime from UInt32; automatic temporal-field discovery cannot infer that source type.

Use SELECT-only grants and restrict table functions, networks and settings. `readonly=1` supplements source authorization; the settings profile must allow trusted limits while preventing SQL callers from relaxing them. The connector alone is not a tenant SQL sandbox. ClickHouse rejects multiple statements; a FORMAT override producing non-Arrow output fails. Native queries stay single-source.

Each request has a unique `query_id`, context deadline and `cancel_http_readonly_queries_on_client_close=1`. Closing the response allows cooperative cancellation through ClickHouse progress callbacks; source execution timeout remains the backstop. Unsupported server settings fail. No privileged KILL QUERY connection is used. [Worker termination](../../../docs/usage.md#native-cancellation-and-remote-cleanup) does not confirm that remote work stopped.

HTTP 200 alone is insufficient: Kelvo requires the Arrow end marker, EOF and no trailing exception. Partial batches before a late error are not successful completion. Public exceptions expose only bounded numeric codes; code 241 identifies source memory limits separately from decoder allocations. SQL, diagnostics and credentials stay private.

[Validation evidence](../../../docs/validation.md) records live results, failed attempts and measurement scope. HTTP fixtures cover types, limits, cancellation, malformed streams and credential boundaries.

References: [HTTP interface](https://clickhouse.com/docs/concepts/features/interfaces/http), [ArrowStream](https://clickhouse.com/docs/interfaces/formats/ArrowStream), [type mappings](https://clickhouse.com/docs/interfaces/formats/Arrow#data-types-matching), [26.9.7.9 conversion](https://github.com/ClickHouse/ClickHouse/blob/v26.9.7.9-stable/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L137), [DateTime](https://clickhouse.com/docs/sql-reference/data-types/datetime#usage-remarks), [cancellation](https://github.com/ClickHouse/ClickHouse/blob/master/src/Server/HTTPHandler.cpp), [Arrow Go IPC reader](https://github.com/apache/arrow-go/blob/v18.5.1/arrow/ipc/message.go).

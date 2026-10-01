# Native ClickHouse adapter

`New(catalog.Config, query.Limits) (*Engine, error)` implements `query.Executor`.
Use `mode: "native"` and a configured ClickHouse `connection_id`. The administrator
supplies an HTTP(S) source URL via `url_env`, and optional `username_env` and
`password_env` references. The URL may include only the `database` query parameter;
embedded credentials, redirects, request-selected URLs, and native query parameters
are rejected. HTTP is useful for private/local test networks; use HTTPS across
untrusted networks.

The source returns ArrowStream batches directly. Kelvo decodes and delivers each
borrowed batch synchronously, preserving the emitted Arrow types and avoiding a row-map intermediate.
`arrow_bytes` counts delivered Arrow buffers, not network framing. Row and byte
limits are checked before delivery; the source also receives execution, memory,
thread, and result limits. A separate bounded decoder protects Arrow body and
metadata allocations. These are component budgets, not an RSS guarantee. Blocking
source operations such as sorting can still consume source memory before output.

ClickHouse's Arrow schema is not a complete description of its original database
types. In ClickHouse 26.9.7.9, a native `DateTime` column is emitted as Arrow
`uint32`: Unix epoch seconds, without Arrow timestamp/timezone metadata.
`DateTime64` is emitted as an Arrow timestamp with a time unit derived from its
scale and its configured timezone. Kelvo preserves these source mappings and does
not reinterpret arbitrary integers or silently rewrite SQL. For timestamp-aware
consumers, explicitly request a timestamp in the query:

```sql
SELECT toDateTime64(event_time, 0, 'UTC') AS event_time
FROM events
```

This produces Arrow `timestamp[s, tz=UTC]`; it preserves the original `DateTime`
instant and second precision. Without that cast, the Arrow schema alone cannot
distinguish a source `DateTime` from a source `UInt32`. This is a documented
preview limitation for automatic temporal-field discovery, not a guarantee of
database-type normalization.

Configure a source account with SELECT-only grants and appropriate database,
table-function, network, and settings restrictions. `readonly=1` is an additional
request restriction, not a replacement for source authorization. The account's
settings profile must permit the trusted request limits while preventing the SQL
caller from relaxing them. All gateway clients currently share one trust domain;
this adapter is not a multi-tenant SQL sandbox. ClickHouse enforces read-only SQL
and its HTTP endpoint rejects multiple statements; Kelvo does not classify SQL
with regular expressions. A SQL `FORMAT` override producing non-Arrow output
fails explicitly. This initial adapter does not rewrite arbitrary SQL or support
native parameter binding or cross-source joins.

Each request has a unique `query_id`, a Go context deadline, and
`cancel_http_readonly_queries_on_client_close=1`. Cancelling or abandoning delivery
closes the response, allowing ClickHouse's progress callback to cancel its query.
This is cooperative cancellation, not an immediate server-side kill guarantee;
the source execution deadline remains a backstop. No separate privileged KILL
QUERY connection is required. Servers lacking these settings fail the request.

HTTP 200 is not sufficient to establish query success. The adapter requires an
explicit Arrow end marker, verifies EOF, and rejects trailing errors or truncated
responses. Batches delivered before a late source failure are partial results;
the outer query status remains authoritative. A bounded late ClickHouse exception
is reported using only its numeric code; a source memory limit (code 241) is
identified separately from a Kelvo Arrow decoder allocation limit. Source SQL,
diagnostic text, and credentials are never copied into the public error.

Primary references inspected during implementation:

- [ClickHouse HTTP interface](https://clickhouse.com/docs/concepts/features/interfaces/http)
- [ClickHouse ArrowStream format](https://clickhouse.com/docs/interfaces/formats/ArrowStream)
- [ClickHouse Arrow type mappings](https://clickhouse.com/docs/interfaces/formats/Arrow#data-types-matching)
- [ClickHouse 26.9.7.9 Arrow conversion implementation](https://github.com/ClickHouse/ClickHouse/blob/v26.9.7.9-stable/src/Processors/Formats/Impl/CHColumnToArrowColumn.cpp#L137)
- [ClickHouse DateTime Unix timestamp semantics](https://clickhouse.com/docs/sql-reference/data-types/datetime#usage-remarks)
- [ClickHouse cancellation implementation](https://github.com/ClickHouse/ClickHouse/blob/master/src/Server/HTTPHandler.cpp)
- [Arrow Go v18.5.1 IPC message reader](https://github.com/apache/arrow-go/blob/v18.5.1/arrow/ipc/message.go)

The tests use a local HTTP fixture to check Arrow values/types, source settings,
cancellation, explicit limits, invalid streams, credential redaction, and redirect
blocking. Real ClickHouse aggregate and large-export results, failed attempts,
tested versions, and measurement scope are recorded in the
[validation evidence](../../../docs/validation.md).

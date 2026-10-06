# D1 and Databricks parameters

Bind values separately from SQL for both reads and autocommit statements. Values never become SQL text. Unsupported values fail before the first statement in a batch is sent.

| Source | Markers | Values |
| --- | --- | --- |
| Cloudflare D1 | SQLite positional markers: `?`, `?1` | Text, null, booleans as 0/1, finite numbers, byte arrays, date/timestamp text |
| Databricks | `?` in order, or named `:p1`, `:p2` | Typed scalar values, exact integers, decimal128, dates and timestamps |

For Databricks, Kelvo rewrites only `?` markers to provider parameter names. Quoted text and comments stay unchanged. Do not mix positional and named markers.

```sql
SELECT customer_id, amount
FROM orders
WHERE amount >= ? AND created_at >= ?
```

Pass the first value as `decimal128` and the second as `timestamp` for Databricks. For D1, use a supported numeric type or an explicit text binding and SQL cast.

- D1 rejects integers outside ±9,007,199,254,740,991 to avoid JSON precision loss. Decimal bindings are unavailable.
- Databricks preserves `uint64` as `DECIMAL(20,0)`. Timestamps must fit microsecond precision; binary, JSON and decimal256 bindings are unavailable.
- Null and an empty string remain distinct. A failed or lost write response is never automatically replayed.

TLS protocol fixtures cover these paths. Dedicated vendor accounts remain required for live-provider acceptance.

See [database operations](database-operations.md) for authorization and outcome handling.

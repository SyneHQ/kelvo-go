# MongoDB SQL and native aggregation

Run read-only aggregation pipelines or restricted SQL through the official MongoDB Go driver v2.9.1. MongoDB stays separately operated; its server license is distinct from the Apache-2.0 driver license.

1. Provision a database read account and TLS connection. Custom client certificate/key files are not yet supported by sandbox provisioning.
2. Store a `mongodb://` or `mongodb+srv://` URI in the referenced environment variable:

```yaml
sources:
  - id: documents
    type: mongodb
    dsn_env: KELVO_SOURCE_MONGODB_DSN
    options:
      database: analytics
```

3. Submit an authenticated native request to `POST /v1/queries`, then retrieve its result handle:

```json
{
  "mode": "native",
  "connection_id": "documents",
  "mongo": {
    "collection": "orders",
    "pipeline": [
      {"$match": {"status": "paid"}},
      {"$group": {"_id": "$region", "revenue": {"$sum": "$amount"}}},
      {"$sort": {"_id": 1}}
    ]
  }
}
```

Pipeline requests exclude SQL, parameters and `sources`. Use canonical Extended JSON (`$numberLong`, `$numberDecimal`, `$date`, `$oid`) when exact BSON types matter.

## SQL SELECT requests

SYNEHQ's Apache-2.0 [zero-sql](https://github.com/SyneHQ/zero-sql) compiler accepts:

```json
{
  "mode": "native",
  "connection_id": "documents",
  "sql": "SELECT region, COUNT(*) AS row_count, SUM(amount) AS revenue FROM orders WHERE status = 'paid' GROUP BY region ORDER BY region"
}
```

SQL and `mongo` are mutually exclusive; parameters and `sources` are unsupported. Kelvo uses `ConvertReadOnlySQLToMongoWithCollection`, then validates the pipeline under the native limits. It never runs the legacy preprocessor or strips CAST.

| Supported | Boundary |
| --- | --- |
| One collection/alias, `*`, columns/aliases, aliased literals | Simple ASCII names |
| Comparisons, Boolean logic, IS NULL, IN/NOT IN, BETWEEN/NOT BETWEEN, LIKE/NOT LIKE | SQL NULL/type checks; at most 128 IN values |
| Simple-column GROUP BY; COUNT(*), COUNT(column), numeric SUM/AVG/MIN/MAX | At most 16 grouping keys; aggregate ordering names a selected alias |
| ORDER BY; literal LIMIT/OFFSET | One sort key; nonnegative limits/offsets |

Missing fields become NULL in explicit projections and predicates. Comparisons, negation and NULL-containing IN lists follow SQL three-valued logic. Exact int64/Decimal128 literals avoid float64. Numeric aggregates use Decimal128; all-NULL SUM is NULL, and an empty global aggregate returns COUNT zero with other aggregates NULL. Incompatible types fail. COUNT retains MongoDB's BSON integer width; Decimal128 arithmetic has finite precision/range.

LIKE is case-sensitive full-string matching; `%` and `_` include newlines and regex punctuation is escaped. Non-NULL operands must be strings. Other string comparisons, grouping and ordering inherit collection collation. Keep sorting/grouping fields type-consistent.

Unsupported: multiple sort keys, ESCAPE/ILIKE, joins, cross-database references, CTEs, subqueries, UNION, DISTINCT, HAVING, CAST, arithmetic, arbitrary functions, writes/DDL, hints, locks and statement terminators.

SQL limits are 64 KiB, 4096 tokens/expression nodes, 64 nesting levels and 128 projections; the resulting pipeline must also fit 128 KiB. Guarded expressions can reduce index use. Inspect explain plans and use native `$match` for workloads that need a particular indexed plan.

## Result delivery and limits

All results, including SQL, use one non-null Arrow Binary `document_bson` field with `kelvo.logical_type=bson` and `content_type=application/bson`. Original BSON preserves heterogeneous fields, ObjectIDs, decimals, int64, binary subtypes, timestamps, nested documents and missing versus NULL. Consumers decode it explicitly:

```python
import bson
import pyarrow.ipc as ipc

with ipc.open_stream("mongo-result.arrow") as result:
    for batch in result:
        for value in batch.column("document_bson"):
            document = bson.BSON(value.as_py()).decode()
            print(document)
```

Pipelines allow at most 128 stages, 128 KiB and 64 nesting levels. Nested `$out`, `$merge`, `$where`, `$function`, `$accumulator`, `$changeStream` and JavaScript BSON are rejected, as are `system.*` and command collections. Database grants still control collection access.

Each query owns a pool with at most one application connection per server. Deadlines cover selection, aggregation and cursor reads; a separate bounded cleanup context attempts `killCursors`, then disconnects. Pools are not retained between disposable workers. After worker cancellation, the [bounded grace](usage.md#native-cancellation-and-remote-cleanup) can end before cleanup finishes; local termination does not confirm remote work stopped.

Arrow staging targets 128 documents or 1 MiB. An oversized document gets a singleton batch and must fit one quarter of client memory, the output budget and MongoDB's 16 MiB limit. Driver wire batches may allocate earlier; enforce process and database memory policies separately. Aggregation spill is disabled with `allowDiskUse: false`.

Kelvo adds a final `max_rows + 1` limit and fails on the extra row or byte overflow. Slow sinks stop cursor reads. Partial output is never successful completion. BSON results are not DuckDB federation tables, and this adapter has no CDC.

## Validation

Run the disposable official-server fixture on the Linux test VM:

```sh
docker pull mongo:8.0.32
python3 scripts/mongodb_acceptance.py --go /path/to/go --output artifacts/mongodb-acceptance.json
```

The fixture checks exact BSON/SQL semantics, NULLs, limits and write denial using separate read-only credentials and a private network. Its 1 GiB container/256 MiB WiredTiger settings are test bounds, not production sizing. See [validation](validation.md) and [MongoDB acceleration evidence](evidence/mongodb-acceleration.json).

References: [driver](https://github.com/mongodb/mongo-go-driver/tree/v2.9.1), [driver license](https://github.com/mongodb/mongo-go-driver/blob/v2.9.1/LICENSE), [aggregation](https://www.mongodb.com/docs/drivers/go/current/aggregation/), [Extended JSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/), [context behavior](https://www.mongodb.com/docs/drivers/go/current/fundamentals/context/), [server licensing](https://www.mongodb.com/legal/licensing/server-side-public-license).

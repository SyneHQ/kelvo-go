# MongoDB SQL and native aggregation

Kelvo uses the official MongoDB Go driver v2.9.1 (Apache-2.0) to execute read-only aggregation pipelines on a registered MongoDB database. The MongoDB server remains separately operated; its license is distinct from the Go driver's license. No MongoDB server is shipped inside Kelvo.

```yaml
sources:
  - id: documents
    type: mongodb
    dsn_env: KELVO_SOURCE_MONGODB_DSN
    options:
      database: analytics
```

Set the referenced environment variable to the operator-provided `mongodb://` or `mongodb+srv://` URI. Provision an account with only the required database read grants. Use TLS for remote connections. System CA roots are available in the sandbox; arbitrary client certificate/key files require explicit source-file provisioning, which is not implemented yet.

Submit a native request to `POST /v1/queries` with the usual authentication, then retrieve the result handle:

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

Native aggregation requests do not contain SQL, SQL parameters, or a `sources` list. Pipeline values accept MongoDB Extended JSON, including canonical `$numberLong`, `$numberDecimal`, `$date`, and `$oid` representations. Use canonical Extended JSON when exact BSON types matter.

## SQL SELECT requests

A MongoDB source also accepts a restricted SQL SELECT through SYNEHQ's
[zero-sql](https://github.com/SyneHQ/zero-sql) library (Apache-2.0):

```json
{
  "mode": "native",
  "connection_id": "documents",
  "sql": "SELECT region, COUNT(*) AS row_count, SUM(amount) AS revenue FROM orders WHERE status = 'paid' GROUP BY region ORDER BY region"
}
```

SQL and `mongo` are mutually exclusive. SQL parameters and `sources` are not
accepted. Kelvo calls only zero-sql's independent
`ConvertReadOnlySQLToMongoWithCollection` API; it does not use the legacy SQL
preprocessor or silently strip CAST expressions. The converted pipeline passes
through the same read-only, byte, stage, and nesting validation as native input.
Native aggregation remains available when the SQL subset cannot express a query.

Supported SQL includes one collection and optional alias, `*` or named columns,
column aliases, explicitly aliased literals, comparisons, Boolean operators,
`IS NULL`, `IN`/`NOT IN`, `BETWEEN`/`NOT BETWEEN`, `LIKE`/`NOT LIKE`, simple-column
`GROUP BY`, `COUNT(*)`, `COUNT(column)`, numeric `SUM`/`AVG`/`MIN`/`MAX`, one
`ORDER BY` key, and nonnegative literal `LIMIT`/`OFFSET`. Names use simple ASCII
identifiers. Aggregate ordering must name a selected output alias. Multiple sort
keys fail explicitly because zero-sql's map-based pipeline API cannot preserve
MongoDB sort-key precedence.

Missing fields become NULL in explicit projections and SQL predicates. NULL
comparisons, Boolean negation, and NULL-containing IN lists follow SQL's
three-valued logic. Numeric aggregates use Decimal128; an all-null SUM is NULL,
and an empty global aggregate yields one row with COUNT zero and the other
aggregates NULL. Non-null nonnumeric aggregate inputs and incompatible comparison
types fail explicitly. Exact int64 and Decimal128 literals never pass through
float64; MongoDB Decimal128 arithmetic still has finite precision/range. COUNT
preserves the integer BSON width produced by MongoDB. These outputs remain BSON
in Arrow Binary, including SQL results.

LIKE performs case-sensitive full-string matching, escapes regex punctuation,
and lets `%` and `_` match newlines. Explicit ESCAPE and ILIKE are unsupported.
Non-null LIKE operands must be strings. Other string comparisons, grouping, and
ordering inherit the collection's MongoDB collation; LIKE does not. Sorting and
grouping columns should contain consistent BSON types. Joins, cross-database
references, CTEs, subqueries, UNION, DISTINCT, HAVING, CAST, arithmetic, arbitrary
functions, writes/DDL, hints, locks, and statement terminators are rejected.
The SQL input is capped at 64 KiB, 4096 tokens/expression nodes, 64 nesting
levels, 128 projections, 16 grouping keys, and 128 IN-list values; the pipeline
must also fit the shared 128 KiB and depth limits.

The strict compiler uses guarded MongoDB aggregation expressions to enforce
NULL and type semantics. These expressions can limit index use compared with
a hand-written native `$match`. Inspect MongoDB explain plans for important
workloads and use the native pipeline when an indexed source query is required.
The SQL fixtures establish result correctness; they do not establish throughput.

## Result delivery and limits

The result schema contains one non-null Arrow Binary column, `document_bson`, with `kelvo.logical_type=bson` and `content_type=application/bson` field metadata. Each value contains the source-produced BSON document bytes. This preserves heterogeneous fields, ObjectIDs, Decimal128, int64, binary subtypes, timestamps, nested documents, and missing versus null values without inferring a common schema or coercing values through JSON floating point. Consumers must decode BSON explicitly; these are not automatically flattened Arrow columns. For example, with PyArrow and PyMongo installed:

```python
import bson
import pyarrow.ipc as ipc

with ipc.open_stream("mongo-result.arrow") as result:
    for batch in result:
        for value in batch.column("document_bson"):
            document = bson.BSON(value.as_py()).decode()
            print(document)
```

The adapter rejects `$out`, `$merge`, `$where`, `$function`, `$accumulator`, JavaScript BSON values, and `$changeStream`, including nested occurrences. It rejects `system.*` and command collection names. Pipeline input is bounded to 128 stages, 128 KiB, and 64 levels of nesting. These checks complement the database account's grants; they do not grant access to another database or provide per-collection authorization.

The client applies the request deadline to connection selection, aggregation, and cursor reads. Cursor cleanup uses a separate bounded context so cancellation can still issue `killCursors`; the client is then disconnected. Each query owns one connection pool with at most one application connection per server. The disposable worker model does not retain MongoDB pools between queries.

Results are fetched in cursor batches and delivered synchronously, so a slow sink stops further cursor reads. Arrow staging targets at most 128 documents or 1 MiB; an oversized document uses a singleton batch and must fit within one quarter of the configured client memory budget, the result-byte budget, and MongoDB's 16 MiB document limit. The driver can allocate a wire batch before Kelvo inspects its documents. The client budget is not a process RSS limit or a MongoDB server query-memory limit: enforce container memory and database-side policies separately. Server-side aggregation spill is explicitly disabled with `allowDiskUse: false`.

Kelvo appends a final limit of `max_rows + 1` and reports an error if an extra row is observed. Result byte limits also fail explicitly; partial streamed data must never be treated as a successful completed analysis. MongoDB results are not yet exposed as DuckDB federation tables, and this adapter does not implement CDC.

## Validation

The connector unit suite covers nested write/JavaScript rejection, canonical numbers, nesting limits, exact BSON preservation, batch boundaries, byte/row overflow, cancellation, empty results, restricted SQL conversion, and sanitized errors. Live SQL fixtures assert returned BSON values for filtering, ordering, aliases, exact numbers, LIKE, NULL logic, grouped/all-null/empty aggregates, sums exceeding int64, and incompatible-type rejection. Run the disposable official-server acceptance on the Linux test VM:

```sh
docker pull mongo:8.0.32
python3 scripts/mongodb_acceptance.py --go /path/to/go --output artifacts/mongodb-acceptance.json
```

The fixture creates its own internal Docker network and publishes no host port. It creates separate administrator and read-only users with random credentials, tests exact BSON output, write denial, and SQL result semantics, and removes only the resources it created. The fixture uses a 1 GiB container ceiling and 256 MiB WiredTiger cache; these are fixture settings, not production sizing measurements.

References: [official Go driver](https://github.com/mongodb/mongo-go-driver/tree/v2.9.1), [driver license](https://github.com/mongodb/mongo-go-driver/blob/v2.9.1/LICENSE), [aggregation](https://www.mongodb.com/docs/drivers/go/current/aggregation/), [Extended JSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/), [Go driver context behavior](https://www.mongodb.com/docs/drivers/go/current/fundamentals/context/), and [MongoDB licensing](https://www.mongodb.com/legal/licensing/server-side-public-license).

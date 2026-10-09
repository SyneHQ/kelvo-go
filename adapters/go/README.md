# Optional Go adapters

This separate module holds compatibility connectors. Kelvo's native Arrow reads
remain the analytical path; importing `adapter` does not import these drivers.

Available source components:

| Component | Scope |
| --- | --- |
| Cassandra / ScyllaDB | TLS CQL reads, parameters, scoped metadata, autocommit DML and DDL |
| PostgreSQL / MySQL / MariaDB | Reads, catalog metadata, bounded statement batches, transactions |
| SQL Server | Reads, catalog metadata, statement batches; permissions enforced by the source principal |
| ClickHouse | Native Arrow reads, scoped catalog, acknowledged autocommit writes |
| [ClickHouse Lambda](../../docs/sources-clickhouse-lambda.md) | Signed synchronous invokes, exact TSV values, acknowledged writes; no catalog discovery |
| Cloudflare D1 / Databricks | Bounded SQL API reads, connection tests, basic catalog metadata, autocommit statements |
| MongoDB | SQL SELECT subset, explicit native reads/writes, scoped collection catalog |
| Oracle | Verified TCPS, read-only queries, scoped metadata, DML transactions at default isolation |
| Trino / Presto / Flight SQL / Exasol / Ignite | Native reads, metadata and acknowledged autocommit statements |
| BigQuery / Snowflake / Spanner / Athena / DynamoDB | Explicit credentials, native reads, scoped metadata and autocommit statements |
| Cosmos DB | Container reads, discovery and scoped REST mutations from a bounded SQL grammar |
| Redis | Verified TLS, bounded native commands and selected-database metadata |
| CSV / Parquet / JSON / JSONL / DuckDB / SQLite | Verified file snapshots; DuckDB and SQLite changes produce a separate publication candidate |
| Google Sheets | Scoped snapshots queried through DuckDB |
| Stripe / GA4 / Google Ads / Facebook Ads / Salesforce | Bounded read-only provider queries with explicit saved credentials |
| PostHog / MotherDuck / Ramp / Daloopa / Salesforce Data 360 / Tableau Next | Validated native provider commands; capabilities vary by provider |
| SQL change sessions | Dedicated connections, role isolation, uncertain write outcomes |
| Private HTTP boundary | Verified worker identity, bounded admission, redacted errors, receipt binding |

Workers require current operation authority and a durable dispatch record.
Live provider coverage varies; protocol fixtures do not certify vendor accounts.
See [validation coverage](../../docs/database-operations-validation.md).

## Development

From the repository root, use a temporary Go workspace to test both modules.
The optional worker needs CGO and the Arrow build tag. Run on the validation VM:

```sh
go work init . ./adapters/go
sdk_version=$(awk '$1 == "github.com/SYNEHQ/kelvo-go" { print $2 }' adapters/go/go.mod)
go work edit "-replace=github.com/SYNEHQ/kelvo-go@${sdk_version}=."
go work edit -replace=github.com/sijms/go-ora/v2@v2.9.0=./third_party/go-ora-v2.9.0
go test -mod=readonly -race ./adapter ./operations
cd adapters/go
CGO_ENABLED=1 go test -mod=readonly -race -tags duckdb_arrow ./...
CGO_ENABLED=1 go vet -mod=readonly -tags duckdb_arrow ./...
CGO_ENABLED=1 go build -mod=readonly -tags duckdb_arrow -o kelvo-adapter-go ./cmd/kelvo-adapter-go
```

CQL reads emit borrowed Arrow batches with row and byte limits. Variable-scale
decimals and arbitrary-size integers use exact text with source-type metadata;
unsupported collection types fail explicitly. These query sessions do not yet
implement federation discovery/pushdown. A failed limit or iterator means the
result is incomplete. Source frames are decoded before cell limits apply.
Connection setup uses the driver's bounded timeout; query cancellation is best
effort. Sessions are never shared across tenants.

SQL writes are never retried. A lost commit or autocommit response is `unknown`
until reconciled. Role changes require current operation authority; the SDK
mapping does not grant that authority. Customer credentials must never enter
operation records, response bodies or logs.

Required MySQL/MariaDB transactions accept parsed DML with unqualified tables
and functions, and require all selected-database base tables to use InnoDB.
They use native server guarantees: failures after dispatch remain unknown
because triggers, views or routines can have nontransactional effects.

D1 and Databricks reuse the native bounded readers and current request token.
Metadata covers catalogs, schemas, tables and columns. Bound parameters follow each provider's [type rules](../../docs/cloud-sql-bindings.md). Atomic batches, schema plans
and migrations are not supported.
Each autocommit statement reports completion separately; a lost response stops
the batch without replay. Live cloud-account acceptance is a separate gate.

Optional Java compatibility runs through the same contained Go worker. See
[pinned JDBC runtimes](../../docs/jdbc-runtime.md).

## MongoDB commands

Use `native.read` for `aggregate`, `find`, `find_one`, `count` or `list_indexes`.
Use `native.execute` for inserts, updates, deletes, collection creation/removal
and named index creation/removal. Mutations require an idempotency key; the
adapter sends each command once and reports uncertain outcomes without replay.

Each command takes one `json` parameter, at most 16 KiB:

```yaml
provider: mongodb
command: find
parameters:
  - type: json
    value:
      collection: orders
      filter: {customer_id: 42}
      projection: {total: 1}
```

Use canonical Extended JSON for decimals, doubles, dates, ObjectIDs and binary
values. The connector emits BSON; the operation process converts it to canonical
Extended JSON in Arrow `document` cells. The API returns these documents without
a BSON dependency. Bare fractional numbers are rejected to prevent rounding.

The gateway also accepts JSON-only forms such as
`db.orders.find({"id":1})` and `db.orders.insertOne({"id":1})`. Find supports
ordered `.sort()`, `.skip()` and `.limit()` chains. Use quoted JSON keys and
Extended JSON values; constructors such as `ObjectId(...)` are rejected.

Mutation results preserve inserted IDs and affected counts. A confirmed write
stays committed even if delivery of its result fails.

The adapter rejects executable shell code, arbitrary administrative commands, JavaScript,
cross-database pipelines and unknown options. SQL writes, inferred column
schemas and MongoDB transactions are not implemented. SQL reads use the existing
bounded SELECT compiler; native aggregation supports a broader read vocabulary.

Oracle preserves exact `NUMBER` values when precision and scale are known, and
normalizes timestamp-with-time-zone instants to Arrow UTC. Local-time-zone
results, untyped `NUMBER` expressions and nondefault transaction isolation fail
explicitly. Oracle TCPS still requires live-provider acceptance.

Native cloud and federation statement wrappers accept autocommit SQL strings;
they reject parameter arrays, transactions and result-producing mutations.
All batch slots are validated before the first write. Cancellation or a lost
acknowledgement never triggers replay; confirmed earlier statements remain in
the receipt. See [Cosmos mutation syntax](../../docs/sources-cosmosdb.md#authorized-writes).

## MongoDB change streams

Native watches require MongoDB 6+ with a replica set or sharded cluster. Install
creates a scoped cursor record in `_kelvo_watch_state`; reads never install or
advance it. Acknowledgements advance the exact previous token after durable
capture. Invalid or expired tokens fail without restarting from now.

Event IDs come from immutable resume tokens. Inserts and replacements carry
documents; updates carry `documentKey` plus `updateDescription`; deletes carry
the document key. `data_kind` and `old_data_kind` state which shape was returned.
Values use canonical Extended JSON. There is no mutable full-document lookup.

Retired generations stay recorded so delayed requests cannot revive them. A
watcher key supports 1,000 retired generations; rotate the key after that limit.
Existing API-owned resume tokens require a controlled import before cutover.

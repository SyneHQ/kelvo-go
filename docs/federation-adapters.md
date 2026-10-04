# Federation adapters: operator and contributor guide

Configure the SQL Server, Snowflake, BigQuery, Databricks and Oracle adapters, or add a compiled-in Go adapter. All use the optional [bridge build](federation.md#build-and-update); native query support alone does not provide federation.

## Why these five

These tabular engines complement ClickHouse/PostgreSQL/MySQL and reuse existing typed connectors. The [2025 Stack Overflow survey](https://survey.stackoverflow.co/2025/technology#1-databases) informed coverage priorities; it is neither market share nor a performance ranking.

MongoDB federation is deferred until typed fields, missing/NULL values, mixed types, arrays and collection authorization have a verified contract. MongoDB [null](https://www.mongodb.com/docs/manual/tutorial/query-for-null-fields/) and [equality](https://www.mongodb.com/docs/manual/reference/operator/query/eq/) semantics cannot silently replace SQL semantics. Use its [native document connector](sources-mongodb.md) meanwhile.

## Configure exact remote names

1. Choose local `id` and table `name`; together they form aliases such as `warehouse.orders`.
2. Supply exact remote identifiers below. Kelvo quotes them by dialect; they are never SQL fragments.

| Engine | Required table fields | Remote reference and case rules |
| --- | --- | --- |
| SQL Server | `database`, `schema`, `table` | `[Sales].[dbo].[Orders]`; identifier case behavior follows the database collation. Brackets avoid dependence on `QUOTED_IDENTIFIER`. |
| Snowflake | `database`, `schema`, `table` | `"ANALYTICS"."PUBLIC"."ORDERS"`; unquoted source DDL normally produces uppercase names. Quoted mixed-case objects require their exact spelling. |
| BigQuery | `database`, `schema`, `table` | `` `example-project.analytics.orders` ``; `database` is the project ID and `schema` the dataset. Project IDs may contain hyphens. Dataset/table case is significant unless the dataset enables case-insensitive names. |
| Databricks | `database`, `schema`, `table` | `` `main`.`analytics`.`orders` ``; `database` names the catalog. Backticks delimit identifiers; identifier references are case-insensitive. |
| Oracle | `schema`, `table`; omit `database` | `"ANALYTICS"."ORDERS"`; the DSN selects the service/database. Unquoted source DDL normally produces uppercase names; quoted objects retain their spelling. |

3. Register authorized tables and per-scan budgets. Keep token/DSN values in the environment:

```yaml
sources:
  - id: reporting
    type: sqlserver
    dsn_env: KELVO_SOURCE_SQLSERVER_DSN
    federation:
      max_scan_rows: 100000
      max_scan_bytes: 16777216
      tables:
        - name: orders
          database: Sales
          schema: dbo
          table: Orders

  - id: snow
    type: snowflake
    url_env: KELVO_SOURCE_SNOWFLAKE_URL
    token_env: KELVO_SOURCE_SNOWFLAKE_TOKEN
    options:
      warehouse: ANALYTICS_WH
      role: ANALYTICS_READER
      token_type: OAUTH
    federation:
      max_scan_rows: 100000
      max_scan_bytes: 16777216
      tables:
        - name: orders
          database: ANALYTICS
          schema: PUBLIC
          table: ORDERS

  - id: bigquery
    type: bigquery
    url_env: KELVO_SOURCE_BIGQUERY_URL
    token_env: KELVO_SOURCE_BIGQUERY_TOKEN
    options:
      project: example-project
      location: US
      maximum_bytes_billed: "1000000000"
    federation:
      max_scan_rows: 100000
      max_scan_bytes: 16777216
      tables:
        - name: orders
          database: example-project
          schema: analytics
          table: orders

  - id: lakehouse
    type: databricks
    url_env: KELVO_SOURCE_DATABRICKS_URL
    token_env: KELVO_SOURCE_DATABRICKS_TOKEN
    options:
      warehouse_id: example-warehouse-id
    federation:
      max_scan_rows: 100000
      max_scan_bytes: 8388608
      tables:
        - name: orders
          database: main
          schema: analytics
          table: orders

  - id: ledger
    type: oracle
    dsn_env: KELVO_SOURCE_ORACLE_DSN
    federation:
      max_scan_rows: 100000
      max_scan_bytes: 16777216
      tables:
        - name: orders
          schema: ANALYTICS
          table: ORDERS
```

4. Select the source IDs in a federated request, then query aliases such as `SELECT * FROM reporting.orders`.

BigQuery `options.project` controls job/billing identity; table `database` selects the data project. Cross-project grants and consistent dataset locations are required. The example billed-byte ceiling is not a cost estimate.

Use narrowly granted accounts, SQL Server verified TLS 1.2+, Oracle verified TCPS, and verified HTTPS cloud origins. Operators acquire/refresh tokens. `ApplicationIntent=ReadOnly` only routes SQL Server reads; grants control authorization, including privileged functions. See [SQL Server/Oracle](sources-sql.md), [cloud SQL](sources-cloud.md) and [BigQuery](sources-bigquery.md).

Naming references: [SQL Server](https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers), [Snowflake](https://docs.snowflake.com/en/sql-reference/identifiers-syntax), [BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#case_sensitivity), [Databricks](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-identifiers), [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Database-Object-Names-and-Qualifiers.html).

## Pushdown, types and source costs

DuckDB owns joins, aggregates, ordering, final LIMIT and residual expressions. Adapters receive ordered columns and typed filters: **apply every required filter exactly or fail**. DuckDB may already have removed it from local evaluation.

Built-in pushdown is bound per table to exact integer/Boolean types, plus Arrow Date32 on unguarded native ClickHouse tables. Other fields stay local, including their NULL checks. SQL Server BIT requires numeric or typed bit constants. Unsupported required filters fail explicitly; see [the full pushdown contract](federation.md#what-runs-where).

[Snowflake integer names](https://docs.snowflake.com/en/sql-reference/data-types-numeric) normally yield Decimal128 NUMBER(38,0); Oracle NUMBER can also have negative scale. Exact transport does not enable decimal pushdown. Never narrow these domains to int64 just to push a filter.

| Source | Type boundary |
| --- | --- |
| SQL Server / Oracle | Reject unusable decimal metadata, rounded decimal floats, per-value timezone offsets and standalone TIME; Oracle DATE includes time |
| Snowflake | Supported exact decimals/binary/dates/NTZ/LTZ; TIME, TIMESTAMP_TZ, VARIANT, ARRAY and OBJECT need a supported source view |
| BigQuery | Scalar types and decimals within Decimal256's 76 digits; reject extreme 77-digit values, repeated/STRUCT fields; TIMESTAMP is microseconds |
| Databricks | Declared scalar schema and exact decimals; nested types need an intentional supported projection |

Discovery requires a complete schema even for empty tables. Unsupported fields may prevent registration before projection; use a narrow source view. Preserve NULLs, widths and exact values throughout.

Native caller parameters remain unsupported on Snowflake/BigQuery/Databricks despite their provider binding APIs ([Snowflake](https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests), [BigQuery](https://cloud.google.com/bigquery/docs/parameterized-queries), [Databricks](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial)). The bridge generates validated constants. SQL Server/Oracle use `@pN`/`:N`; identifiers still require separate validation/quoting.

Scan limits are independent of final output. Overruns fail; rescans may repeat source jobs and do not share a snapshot.

- Databricks JSON_ARRAY/INLINE has a [25 MiB ceiling](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial), plus smaller Kelvo budgets. External Arrow links are not followed.
- Snowflake chooses [partition sizes](https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses); each must fit the response/decompression budget.
- BigQuery uses paged JSON, not Storage Read. Set `maximum_bytes_billed`; [LIMIT usually does not reduce reads](https://cloud.google.com/bigquery/docs/best-practices-costs).

Warehouses may charge for execution and transfer. Arrow/wire bytes do not measure billed reads or server CPU. Configure provider deadlines/resource policies. Cancellation is best effort; lost Snowflake/Databricks submission replies may leave unknown handles. BigQuery preassigns its job ID but cancellation can still fail. Provider cleanup may also outlast the [worker cancellation grace](usage.md#native-cancellation-and-remote-cleanup).

## Add a compiled-in Go adapter

1. Implement `Driver` and `Relation` from `github.com/SYNEHQ/kelvo-go/federation`:

```go
type Driver interface {
    Validate(Source, Table) error
    Open(context.Context, Source, Table, Limits) (Relation, error)
}

type Relation interface {
    Schema() *arrow.Schema
    Scan(context.Context, ScanPlan, Sink) (ScanStats, error)
    Close() error
}
```

2. Validate source/table configuration before credentials or I/O. Never accept request-selected endpoints, DSNs, credentials or tables through names/filters.
3. Register during package initialization and compile the same registrations into gateways and workers. No dynamic Go plugin or C-pointer access is required.

`Source` carries identity, environment references and options; `Table` carries alias/namespace. `Limits` must bound discovery as well as scans. `ScanPlan.Columns` is ordered, and every `Filters` predicate is mandatory. Core normalizes zero-column counts to a real source column.

Call `Sink.Schema` before synchronous `Sink.Write`. Batches are borrowed during Write; retaining them requires Arrow Retain/Release. Core owns admission, bounded handoff and callback lifetime. Adapters own source reads, exact conversion, credentials and cleanup.

Report consumed encoded response bytes in `ScanStats.SourceWireBytes`, including rejected/incomplete data but excluding discovery/headers; use zero if unavailable. Core counts rows, batches and Arrow bytes.

Discovery and each scan receive independent relations; never share a mutable cursor across self-joins/rescans. Honor context during connect/discovery/read, close safely after partial failure, and return wrapped `federation.ErrUnsupported` for required unsupported behavior. Diagnostics must exclude credentials.

### Minimal executable example

Save this one-row teaching adapter as `example/staticadapter/adapter.go`. It demonstrates projection and ownership, rejects all pushed filters, and does not connect to a database:

```go
package staticadapter

import (
    "context"

    "github.com/SYNEHQ/kelvo-go/federation"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func init() { federation.MustRegister("example_static", driver{}) }

type driver struct{}

func (driver) Validate(_ federation.Source, table federation.Table) error {
    if table.Database != "" || table.Schema != "" || table.Table != "one_row" {
        return federation.ErrUnsupported
    }
    return nil
}

func (d driver) Open(ctx context.Context, source federation.Source,
    table federation.Table, _ federation.Limits) (federation.Relation, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    if err := d.Validate(source, table); err != nil {
        return nil, err
    }
    schema := arrow.NewSchema([]arrow.Field{
        {Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
    }, nil)
    return &relation{schema: schema}, nil
}

type relation struct{ schema *arrow.Schema }

func (r *relation) Schema() *arrow.Schema { return r.schema }
func (r *relation) Close() error          { return nil }

func (r *relation) Scan(ctx context.Context, plan federation.ScanPlan,
    sink federation.Sink) (federation.ScanStats, error) {
    stats := federation.ScanStats{}
    if err := ctx.Err(); err != nil {
        return stats, err
    }
    if len(plan.Filters) != 0 || len(plan.Columns) != 1 || plan.Columns[0] != "id" {
        return stats, federation.ErrUnsupported
    }
    if err := sink.Schema(r.schema); err != nil {
        return stats, err
    }
    builder := array.NewInt64Builder(memory.DefaultAllocator)
    defer builder.Release()
    builder.Append(1)
    values := builder.NewArray()
    defer values.Release()
    record := array.NewRecordBatch(r.schema, []arrow.Array{values}, 1)
    defer record.Release()
    return stats, sink.Write(record)
}
```

Add `cmd/kelvo/adapters_custom.go`:

```go
package main

import _ "github.com/SYNEHQ/kelvo-go/example/staticadapter"
```

Register the table:

```yaml
sources:
  - id: demo
    type: example_static
    federation:
      tables:
        - name: sample
          table: one_row
```

Build with [bridge support](federation.md#build-and-update), select `demo` and query `SELECT id FROM demo.sample`. Registration must finish before the first lookup, cannot replace built-ins and cannot be installed by configuration alone.

## Contributor acceptance and live validation

1. Test namespace validation/quoting, table authorization and rejection before credentials, including adversarial names and cross-namespace attempts.
2. Assert empty/all-NULL schemas, integer/decimal limits, timestamp precision, unsupported values and schema drift; never use float64 for exact values.
3. Compare projections, predicates/NULL truth tables, empty projections, self-joins and rescans against direct source and DuckDB results.
4. Test borrowed ownership, multiple batches, slow consumers, partial-delivery errors and failed Schema/Write calls.
5. Test connect/discovery/fetch cancellation, cleanup, row/byte/concurrency limits, pagination identity/counts and truncation rejection.
6. Run a real verified-TLS source with narrow grants and cross-source joins. Record versions, settings, expected/observed values and cleanup without credentials.

Use the designated Linux VM under [AGENTS.md](../AGENTS.md); no local builds/downloads. Fixtures do not prove live permissions or provider semantics.

[Validation evidence](validation.md#expanded-federation-and-public-adapter-sdk) covers live SQL Server, warehouse protocol fixtures and the public SDK. Live Oracle/Snowflake/BigQuery/Databricks federation acceptance remains pending. SQL Server's [container setup](https://learn.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker) requires suitable Linux x86-64, at least 2 GB RAM and no emulation. Other providers require provisioned instances/accounts, verified transport, read/execution grants and explicit cost budgets.

## Optional capability declarations

Implement `federation.CapabilityProvider` with a v1 `FederationCapabilities()` declaration when useful. `InspectCapabilities` validates and detaches it; absent, invalid, panicking or future-version providers remain unknown. Wrappers must forward it explicitly.

Declarations are advisory. Custom adapters receive projections while DuckDB keeps query predicates local, including when a declaration advertises filters. Under a tenant policy, Kelvo's guard may evaluate supported query predicates alongside the policy; the underlying adapter receives neither. Every filter in a directly supplied `ScanPlan.Filters` remains mandatory: apply it exactly or return `ErrUnsupported`.

# Federation adapters: operator and contributor guide

Kelvo is an independent open-source analytics gateway by SYNEHQ. Its optional
Go/C++ Arrow bridge lets DuckDB plan cross-source queries while compiled-in Go
adapters read operator-selected source tables. This guide covers five additions
to the existing ClickHouse, PostgreSQL and MySQL adapters: SQL Server,
Snowflake, BigQuery, Databricks and Oracle.

These adapters reuse the existing native connectors. They do not turn every
native connector into a federation source. The [bridge build and deployment
requirements](federation.md#build-and-update) still apply. An ordinary build
without the bridge fails explicitly when custom federation is requested.

## Why these five

The [2025 Stack Overflow Developer Survey](https://survey.stackoverflow.co/2025/technology#1-databases)
reports the following usage among professional developers. This is a survey of
respondents, not database market share or a performance comparison.

| Priority | Engine | Survey usage | Reason to add federation |
| --- | --- | ---: | --- |
| 1 | SQL Server | 30.9% | Broad relational adoption; an existing typed Go driver can feed the bridge. |
| 2 | BigQuery | 6.5% | Common analytics warehouse; joins with operational sources are useful, with explicit query-cost limits. |
| 3 | Snowflake | 4.2% | Warehouse data fits the tabular adapter model; the existing SQL API preserves exact scalar values. |
| 4 | Databricks SQL | 3.2% | Adds lakehouse query results through the existing Statement Execution API, within its inline-result ceiling. |
| 5 | Oracle | 10.4% | Important enterprise relational coverage; precise NUMBER metadata and verified TCPS need particular care. |

MongoDB usage is higher than most of these, at 24.3%, but popularity alone does
not establish a safe relational mapping. Its native connector preserves BSON
documents in Arrow Binary. A useful collection adapter needs an explicit typed
field policy, missing-versus-NULL behavior, mixed-type rejection, nested/array
semantics, and collection-level authorization. MongoDB equality can also match
array elements, and null queries can include missing fields; those semantics
must not be substituted for SQL predicates. See the [MongoDB source
guide](sources-mongodb.md), [null-field reference](https://www.mongodb.com/docs/manual/tutorial/query-for-null-fields/)
and [equality reference](https://www.mongodb.com/docs/manual/reference/operator/query/eq/).
Typed MongoDB federation is deferred until that contract is implemented and
verified.

## Configure exact remote names

The source `id` and table `name` form the local DuckDB reference, such as
`warehouse.orders`. The `database`, `schema` and `table` fields identify the
remote object. They are identifiers, never SQL snippets. Kelvo quotes them for
the selected dialect; supply the exact remote names instead of relying on a
search path or converting every name to lowercase.

| Engine | Required table fields | Remote reference and case rules |
| --- | --- | --- |
| SQL Server | `database`, `schema`, `table` | `[Sales].[dbo].[Orders]`; identifier case behavior follows the database collation. Brackets avoid dependence on `QUOTED_IDENTIFIER`. |
| Snowflake | `database`, `schema`, `table` | `"ANALYTICS"."PUBLIC"."ORDERS"`; unquoted source DDL normally produces uppercase names. Quoted mixed-case objects require their exact spelling. |
| BigQuery | `database`, `schema`, `table` | `` `example-project.analytics.orders` ``; `database` is the project ID and `schema` the dataset. Project IDs may contain hyphens. Dataset/table case is significant unless the dataset enables case-insensitive names. |
| Databricks | `database`, `schema`, `table` | `` `main`.`analytics`.`orders` ``; `database` names the catalog. Backticks delimit identifiers; identifier references are case-insensitive. |
| Oracle | `schema`, `table`; omit `database` | `"ANALYTICS"."ORDERS"`; the DSN selects the service/database. Unquoted source DDL normally produces uppercase names; quoted objects retain their spelling. |

Official naming references: [SQL Server](https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers),
[Snowflake](https://docs.snowflake.com/en/sql-reference/identifiers-syntax),
[BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#case_sensitivity),
[Databricks](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-identifiers),
and [Oracle](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Database-Object-Names-and-Qualifiers.html).

This example exposes one table per source and bounds each scan. Environment
variable values are supplied separately by the operator; no token or DSN belongs
in the catalog. Replace the example remote names with real authorized objects.

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

BigQuery's `options.project` is the job/billing project; the table's `database`
is its data project. Cross-project access still requires appropriate grants.
Use a consistent BigQuery location for all referenced datasets. The billed-byte
value above is an example ceiling, not an estimate of this query's cost.

Submit a federated query with the corresponding source IDs selected. For example,
`SELECT * FROM reporting.orders` exposes only the configured table alias. A
query may join these aliases with other selected federation sources. Snapshot
aliases remain a separate, materialized acceleration feature.

SQL Server requires verified TLS 1.2 or newer with encryption enabled; an
Availability Group's `ApplicationIntent=ReadOnly` is a routing hint rather than
an authorization boundary. Oracle requires verified TCPS and uses a read-only
transaction. Use narrowly granted database users for both. Cloud connectors
use verified HTTPS origins and source-scoped credentials; token acquisition and
refresh remain operator responsibilities. Follow [SQL Server/Oracle
configuration](sources-sql.md), [cloud SQL configuration](sources-cloud.md) and
[BigQuery configuration](sources-bigquery.md). Read-only syntax checking cannot
make a privileged source function safe.

## Pushdown, types and source costs

DuckDB owns joins, aggregation, ordering, final LIMIT, and expressions it keeps
above the source scan. The adapter receives ordered column names and a typed
filter tree. It must implement every required supplied filter exactly or return
an unsupported error. It must never drop a required filter merely because
DuckDB can evaluate similar expressions locally: pushed filters may already
have been removed from the DuckDB plan.

The initial predicate contract is deliberately narrow: exact supported integer
and Boolean comparisons, NULL checks, and supported AND/OR combinations.
Support depends on the source type as represented in Arrow. SQL Server `BIT`
needs numeric or explicitly typed bit constants; bare Boolean keywords are not
portable T-SQL. String collations, floating-point comparisons, timestamp zones,
decimal comparisons and document comparisons need their own conformance before
being added to source pushdown. The bridge advertises filter support only on
integer/Boolean columns: other column predicates, including NULL checks, stay
in DuckDB. Those queries remain available but can fetch more rows before
filtering. Unsupported predicates that are nevertheless supplied as required
filters fail explicitly; adapters must never silently drop them.

In particular, [Snowflake integer names are aliases for
NUMBER(38,0)](https://docs.snowflake.com/en/sql-reference/data-types-numeric).
Their native result schema is normally Decimal128, not Arrow Int64. Oracle
NUMBER is also a decimal domain and can have negative scale. Transporting these
values exactly does not imply support for pushing decimal predicates. Do not
narrow them to int64 to make a filter pass.

Each source retains its native type limitations:

- SQL Server/Oracle reject unverified decimal metadata, already-rounded
  floating-point decimal values, unsupported per-value timezone offsets and
  standalone TIME. Oracle DATE includes a time component. NUMBER without usable
  precision/scale may require a typed source view.
- Snowflake preserves supported exact decimals, binary, dates and NTZ/LTZ
  timestamps. TIME, TIMESTAMP_TZ, VARIANT, ARRAY and OBJECT require a supported
  representation in a source view; they are not silently flattened.
- BigQuery preserves supported scalar types, NUMERIC and BIGNUMERIC within
  Arrow Decimal256's 76-digit bound. Extreme 77-digit values, repeated/STRUCT
  fields and other unsupported types fail. TIMESTAMP uses microsecond precision.
- Databricks uses its declared scalar schema and exact decimals. Unsupported
  nested types must be projected into an intentional supported representation
  in a source view before registration.

Discovery must preserve a complete typed schema even when the table is empty.
NULL is never an empty string, zero, or a dropped row. A schema containing an
unsupported field can prevent registration even if a later query selects fewer
columns. Expose a narrow source view when a table has incompatible fields.

The public native SQL APIs currently reject caller parameters on Snowflake,
BigQuery and Databricks. Their providers support [Snowflake `?`
bindings](https://docs.snowflake.com/en/developer-guide/sql-api/submitting-requests),
[BigQuery named or positional
parameters](https://cloud.google.com/bigquery/docs/parameterized-queries) and
[Databricks named parameters](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial),
but that provider capability is distinct from Kelvo's implemented contract.
The bridge generates validated typed constants for its supported predicates.
SQL Server and Oracle native drivers use `@pN` and `:N` markers respectively;
identifier names must always be validated and quoted separately from values.

Scan row and Arrow-byte limits are independent of final output limits. An
overflow must fail the whole query, never turn a complete relation into its
first N rows. Each self-join/rescan can read the source again, and independently
opened scans do not share a database snapshot.

- Databricks currently uses JSON_ARRAY/INLINE results, with a provider ceiling
  of [25 MiB](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial).
  Kelvo also applies its smaller response/byte budgets and rejects truncation.
  External Arrow-result links are not followed by this connector.
- Snowflake chooses the size of its [result
  partitions](https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses).
  A partition must fit the bounded HTTP/decompression budget; the adapter cannot
  assume that a small Arrow batch implies a small source response.
- BigQuery's `maximum_bytes_billed` constrains job cost. A returned row count or
  SQL LIMIT is not a reliable storage-read cost bound; [LIMIT generally does not
  reduce costs on non-clustered tables](https://cloud.google.com/bigquery/docs/best-practices-costs).
  Repeated scans can submit repeated jobs. The connector uses paged JSON results,
  not the BigQuery Storage Read API.

All three warehouses can charge for source execution and result transfer.
Arrow bytes and source wire bytes are different measurements; neither reports
warehouse CPU time or billed storage reads. Source deadlines, provider resource
policies and narrow projection/filtering remain necessary. Cancellation is
cooperative and cloud cancellation is best effort. A lost Snowflake or
Databricks submission response can leave a remote statement whose handle is
unknown; BigQuery assigns its job ID before submission, but cancellation can
still fail.

## Add a compiled-in Go adapter

Use the public package `github.com/SYNEHQ/kelvo-go/federation`. A contributor
implements `Driver` and `Relation`, then registers the driver during package
initialization. It does not import `internal/federation`, access C pointers, or
load a Go dynamic plugin.

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

`Source` carries the registered source identity, environment-reference names and
provider options. `Table` carries the local alias and remote namespace. Never
accept a request-selected endpoint, DSN, credential, or extra table through a
filter or column name. Validate configuration before reading credentials or
opening the source. Use `Limits` to constrain work and allocation at the source,
including metadata reads.

`ScanPlan.Columns` is ordered. Its `Filters` are the required typed predicates.
The core normalizes a zero-column count scan to a real source column so the
adapter can preserve its known schema. A `Sink` receives `Schema(*arrow.Schema)`
then synchronous `Write(arrow.RecordBatch)` calls. A batch is borrowed for the
duration of `Write`; retaining data beyond that call requires Arrow
Retain/Release ownership. The core provides scan admission, bounded batch
handoff, resource checks, and the DuckDB callback lifetime. The adapter owns
source reads, credentials, exact conversion, predicate semantics and cleanup.
Report encoded source response bytes consumed in `ScanStats.SourceWireBytes`,
including rejected/incomplete results but excluding discovery and transport
headers. Leave it zero when unavailable; the core counts rows, batches and
Arrow bytes.

The core opens an independent relation for discovery and for each scan. Do not
share a mutable cursor between self-joins or rescans. Honor the supplied context
during connect, discovery, execution and reading; make `Close` safe after partial
failure. Return `federation.ErrUnsupported`, optionally wrapped, for a required
feature you cannot implement. Custom diagnostic text is sanitized at the public
query boundary and must never include credentials.

### Minimal executable example

The following teaching adapter exposes one in-memory `id` value. It demonstrates
registration, projection, schema and borrowed-batch ownership. It deliberately
rejects every pushed filter; replace that behavior with verified source
semantics when building a real connector. It does not claim to connect to a
database.

Save this as `example/staticadapter/adapter.go` in a Kelvo checkout:

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

Add a compiled-in registration import at `cmd/kelvo/adapters_custom.go`:

```go
package main

import _ "github.com/SYNEHQ/kelvo-go/example/staticadapter"
```

Register its single table in the catalog:

```yaml
sources:
  - id: demo
    type: example_static
    federation:
      tables:
        - name: sample
          table: one_row
```

Build using the [bridge build instructions](federation.md#build-and-update), then
select source `demo` and query `SELECT id FROM demo.sample`. Use the same binary
and registrations on gateways and query workers. Registration is process-local,
must complete before the first adapter lookup, and cannot replace a reserved
built-in source name. A configuration entry alone cannot install code.

## Contributor acceptance and live validation

Before proposing another adapter, provide focused tests for these independent
requirements:

1. Namespace validation, quoting/case, selected-table authorization and
   rejection before credential lookup. Include adversarial column names,
   reserved words and cross-namespace attempts.
2. Empty/all-NULL relations, exact integer limits and decimals, timestamp
   precision, unsupported values and schema drift. Assert schema as well as
   values; never use float64 as an exact-integer or decimal intermediate.
3. Projection reordering, each supported predicate and NULL truth table,
   unsupported required filters, empty projections, self-joins and rescans.
   Compare pushed results with a direct source query and DuckDB evaluation.
4. Borrowed Arrow lifetime, multiple batches, slow consumers, source errors
   after partial delivery, and errors returned by `Sink.Schema`/`Sink.Write`.
5. Cancellation during connect, discovery and fetch; cursor/job cleanup;
   row/byte/concurrency overflow; pagination identity/count checks; and
   truncation rejection. A partial stream must not become a successful query.
6. A real source instance using narrowly granted credentials and verified TLS,
   plus joins against another source. HTTP fixtures or a driver constructor do
   not prove live permissions, query semantics or cancellation.

Do not run builds or download packages on the local workstation for this
repository's current workflow. Use the designated Linux test VM and bounded,
isolated fixture resources as required by [AGENTS.md](../AGENTS.md). Preserve
unrelated workloads. Record the engine version, relevant configuration, query,
expected/observed values and cleanup outcome without including credentials.

The [validation record](validation.md#expanded-federation-and-public-adapter-sdk)
links live SQL Server acceptance, warehouse protocol fixtures and external
public-SDK acceptance. Live Oracle, Snowflake, BigQuery and Databricks federation
acceptance remains pending. SQL Server Developer can be tested on a suitable
Linux x86-64 VM; its official
[container quickstart](https://learn.microsoft.com/en-us/sql/linux/quickstart-install-connect-docker)
requires at least 2 GB RAM and does not support emulated hosts. Oracle Free
requires a suitable instance and verified TCPS setup. Snowflake, BigQuery and
Databricks require operator-provisioned accounts, source read grants, execution
permissions and explicit cost budgets. No live cloud credentials or acceptance
are implied by the included YAML or protocol tests.


## Optional capability declarations

An adapter or wrapper may implement `federation.CapabilityProvider` with a v1
`FederationCapabilities()` declaration. `InspectCapabilities` validates bounds
and returns a detached declaration; absent, invalid, panicking or future-version
providers are unknown. Wrappers must explicitly forward the method.

These are advisory declarations, not conformance certification or planner
negotiation. Every predicate passed in `ScanPlan.Filters` remains mandatory:
apply it exactly or return `ErrUnsupported`. Never drop an unsupported filter on
the assumption DuckDB will reapply it. The current declaration vocabulary is
limited to the bridge's integer/Boolean comparisons, null predicates and logical
combinations; it does not advertise aggregate/join pushdown.

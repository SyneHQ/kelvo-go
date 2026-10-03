# Native DuckDB federation

Kelvo keeps DuckDB as its embedded federation engine. The optional native bridge
connects DuckDB's Arrow scanner to the existing Go ClickHouse, PostgreSQL,
MySQL, SQL Server, Oracle, Snowflake, BigQuery and Databricks connectors. DuckDB
plans joins and local computation; the connector fetches projected columns and
applies supported filters at the source. There is no additional server process,
Rust runtime or unsigned DuckDB extension. Source decoding follows each native
connector's protocol; cloud SQL APIs currently decode their JSON result pages
into typed Arrow batches.

This is an opt-in Linux amd64 implementation, with explicit conformance and
deployment limits. It does not make every native connector a federation adapter.
PostgreSQL/MySQL configurations without a `federation` section retain their
existing signed DuckDB extension path. File/object snapshots retain Parquet.

## Configure selected tables

```yaml
sources:
  - id: warehouse
    type: clickhouse
    url_env: KELVO_SOURCE_WAREHOUSE_URL
    username_env: KELVO_SOURCE_WAREHOUSE_USER
    password_env: KELVO_SOURCE_WAREHOUSE_PASSWORD
    federation:
      max_scan_rows: 10000000
      max_scan_bytes: 536870912
      tables:
        - name: events
          database: analytics
          table: events
```

Query `warehouse.events` using `mode: federated` and select `warehouse` in the
request's source list. Only the operator-listed tables become views. There are
at most 32 custom tables per source and across a selected query; names are plain
SQL identifiers. Keep the catalog narrow: initial registration fetches each configured table's schema using a
zero-row native query. Source grants must independently restrict access to the
permitted database tables and functions.

```sh
bin/kelvo query --config examples/federation.yml --sources warehouse \
  --sql 'SELECT account_id, count(*) FROM warehouse.events WHERE event_id >= 1000 GROUP BY account_id' \
  --out accounts.arrow
```

For PostgreSQL, the DSN chooses the database and each table requires an explicit
`schema`. For MySQL and ClickHouse, each table requires `database`. The other
namespace field must be absent; Kelvo does not infer `search_path`. See
[the three-source example](../examples/federation-relational.yml).

For SQL Server, Snowflake and Databricks, specify both `database` and `schema`.
Oracle requires `schema` and omits `database`. BigQuery uses `database` for the
data project and `schema` for the dataset. Use exact remote case and names.
See [the five additional adapter configurations](federation-adapters.md#configure-exact-remote-names),
including separate BigQuery job/billing project settings and cloud API limits.

PostgreSQL requires its native verified-TLS URL and MySQL its native verified-TLS
DSN, both using system CA roots. The signed-extension connection-string format
is not the native Go adapter format. Follow the [relational source guide](sources-relational.md).
Use database-enforced read-only accounts. PostgreSQL, MySQL and Oracle scans
also use rollback-only read-only transactions. SQL Server uses rollback-only
transactions and relies on database grants for read-only enforcement. Scans on
different sources do not share a transaction snapshot.

`max_scan_rows` and `max_scan_bytes` bound each source scan independently of the
final result. If omitted, the query's corresponding limits apply. Exceeding a
scan budget fails the query; Kelvo never silently truncates a relation before a
join or aggregate. The query shares an active-scan admission limit of at most
four, further bounded by its configured thread count. Saturation fails explicitly
rather than waiting on a slot that another join input might hold.

## What runs where

| Operation | Current execution |
| --- | --- |
| Requested columns | Selected in source SQL; Arrow batches keep the required order |
| Integer/boolean comparisons | Exact typed constants applied using the selected source's dialect |
| NULL checks and supported AND/OR combinations on integer/boolean columns | Applied at the source when passed down by the optimizer; otherwise evaluated by DuckDB |
| String, decimal, floating-point, temporal and other column predicates | Retained in DuckDB, including NULL checks; these columns do not advertise source filter pushdown |
| Required pushed predicates outside that subset | Explicit unsupported error |
| Residual expressions retained by DuckDB | Evaluated by DuckDB |
| Joins, aggregates, ordering and LIMIT | DuckDB; no general source pushdown for these operators |

Sparse integer `IN (...)` lists can also reach adapters as an OR of exact
equalities when the pinned optimizer supplies an optional IN filter. DuckDB
retains its original predicate. Lists must contain at most 256 non-NULL constants
of one supported type. Additional hints are omitted when the complete plan would
exceed 32 KiB, 256 top-level filters or 1,024 predicate nodes. Required predicates
keep their existing validation and cannot be dropped. Hints are never extracted
from inside an OR arm.

Unsupported, large, NULL-containing and negative IN predicates retain local
SQL evaluation or another supported optimizer plan; source filtering is not
guaranteed for every SQL spelling. This adds no string/collation, floating-point,
decimal or temporal pushdown. Once an equality tree reaches an adapter, the
existing exact-application contract still applies. Tests cover the pinned planner,
SQL NULL behavior, global budgets and eight dialect compilers; the HTTP/Arrow
fixture's reduced row transfer is not a live-database throughput benchmark.

The bridge receives typed optimizer predicates rather than rewriting the user's
SQL. It validates every requested column against the acquired schema. Integer
constants retain their width and signedness; no floating-point conversion is
used. A private copy of the pinned Arrow scanner advertises integer/boolean
filter support only. String, decimal, floating-point and temporal predicates stay
in DuckDB so source collation, rounding and timezone rules cannot silently change
them. This may fetch more rows and reach a scan limit sooner. A required
predicate cannot be discarded: DuckDB assumes the producer has applied it.

ClickHouse supplies ArrowStream directly. PostgreSQL, MySQL, SQL Server and Oracle
use their Go driver's row protocol and exact row-to-Arrow conversion. Snowflake,
BigQuery and Databricks use their existing paginated JSON SQL APIs. These paths are
not columnar source wire protocols. The source connector owns decoding, source limits
and cancellation.
Each scan hands off one retained batch and waits for the consumer to advance.
The C interface pins exported Go buffers until DuckDB releases them. Different
scans own independent readers, including when a query references the same table
twice. The query connection and database close before callback factories are
released. Type-specific conversion inside DuckDB can still copy data; this is
not a claim that every type or complete query is zero-copy.

Successful statistics include `federation` entries with source/table identity,
scan count, fetched rows, batches and logical Arrow bytes. These measure data
received by Kelvo, not rows examined inside the source. Provider query profiling
is needed to establish source CPU, disk reads and index effectiveness.

## Add an adapter

The public [`github.com/SYNEHQ/kelvo-go/federation`](../federation/federation.go)
package defines `Driver`, `Relation`, typed scan plans and synchronous Arrow
sinks. Trusted adapters register an exact custom source type during initialization;
they cannot replace built-ins or load code from a query. Core code keeps table
allowlists, schema checks, scan admission, delivery limits and batch ownership.
The [contributor guide](federation-adapters.md) includes a complete example and
the required correctness, cancellation and security checks.

## Validation

The [expanded federation record](validation.md#expanded-federation-and-public-adapter-sdk)
covers live SQL Server joins, warehouse protocol fixtures, both build
configurations and the public adapter SDK. It distinguishes those results from
the four new adapters still awaiting live provider acceptance.

[Live validation](validation.md#duckdb-custom-federation-adapter) demonstrates
10-row source filtering from a million-row table, exact values, independent
self-join scans and CSV joins. Three narrow million-row exports measured
3.28–3.44 million rows/s with 151–158 MiB sampled worker RSS on the shared test
VM. These are warm-cache, instrumented fixture results; they do not establish
general production capacity. The later [NYC Taxi capacity tests](federation-capacity.md)
cover substantial join inputs, complete sorting, constrained memory, remote
delivery and sustained tenant workloads.

## Build and update

The ordinary build remains available without the bridge. A configured custom
federation source fails explicitly when the binary lacks bridge support.

On the Linux amd64 build host:

```sh
python3 scripts/provision_duckbridge.py artifacts/duckbridge --go go
export CGO_CXXFLAGS="-I${PWD}/artifacts/duckbridge/headers"
go build -p 2 -modfile artifacts/duckbridge/duckbridge.mod \
  -tags duckdb_arrow,duckbridge -o bin/kelvo ./cmd/kelvo
```

For the container build, pass `--build-arg DUCKBRIDGE=1` to the existing
Dockerfile. Headers, Python and patched driver sources stay in the build stage.
The runtime contains the compiled Go/C++ binary and normal runtime libraries.

Provisioning verifies a pinned DuckDB source archive SHA-256 and Go module sum.
It copies the official driver into build artifacts, applies the checked-in
23-line accessor patch and writes a separate module file. The baseline `go.mod`
is unchanged. The patch exposes a borrowed native connection only inside
`sql.Conn.Raw`; it does not guess private Go layouts or fork driver execution.

The C++ scanner interface is version-specific. Header version, static DuckDB
library, driver patch and tests must advance together. Runtime rejects a
different DuckDB version. [Upgrade checks](upgrading-duckdb.md) include exact
types, pushed predicates, Arrow ownership under GC, rescan, cancellation, query
limits and both ordinary/tagged builds. Never replace just the runtime library
or headers independently. Original MIT notices are in `licenses/duckdb.txt`.

## Deployment boundaries

The existing tenant-bound subprocess, Landlock launcher, network policy and
resource controls remain required. A thin C++ shim is native code with native
failure modes. Direct user calls to pointer-bearing Arrow table functions are
rejected. The shim loads no arbitrary library and receives no request-selected
database endpoint. The registered source's credentials stay in its tenant's
query process, as for the existing native connector.

DuckDB's pinned Go query path still materializes execution before Arrow result
delivery. Bounded source handoff does not bound hash joins, sorting, native
allocations or total process RSS. Source scan budgets apply per scan, not to the
sum of all tenants. Enforce container CPU/memory/PID limits and account for
DuckDB's spill files. Linux worker cancellation allows up to 750 ms for source
cleanup before forcibly killing the process group. PostgreSQL sends a bounded
connection-keyed cancellation request. MySQL also applies a server-side SELECT
timeout because socket closure need not stop upstream work immediately. Network
failure and forced process death can still prevent cooperative cleanup;
source-side resource policies and read-only grants remain necessary. See
[relational cancellation limits](sources-relational.md#resource-and-validation-boundaries).

There is no transaction shared across independently opened scans or databases.
Concurrent writes can change results between inputs, even within one query.
Use a source-consistent export/snapshot when the analysis requires that guarantee.

## Why this approach

The pinned [Go table-function API](https://github.com/duckdb/duckdb-go/blob/v2.10506.0/table_udf.go)
does not expose the full filter-planning hooks. DuckDB's
[Arrow stream factory](https://github.com/duckdb/duckdb/blob/v1.5.6/src/function/table/arrow.cpp)
already receives projections and filters. Reusing it lets Kelvo retain the Go
connectors and DuckDB execution path with a small C++ boundary.

A Go-to-Rust DataFusion bridge is possible through a custom C ABI and Arrow C
Data/Stream, or a separately isolated Flight SQL service. However,
[DataFusion FFI](https://github.com/apache/datafusion/tree/main/datafusion/ffi)
primarily connects Rust libraries; it is not a supported drop-in Go binding.
[DataFusion federation](https://github.com/datafusion-contrib/datafusion-federation)
is an alpha framework, while database coverage comes from
[table providers](https://github.com/datafusion-contrib/datafusion-table-providers).
Adding that stack would introduce another planner, connector layer and upgrade
path. It remains a future option if matched workload measurements justify it.
Neither implementation language nor a bridge alone establishes higher throughput.


## Actual scan diagnostics

Set `scan_diagnostics: true` on a federated query request, or pass
`--scan-diagnostics` to `kelvo query`, to include bounded diagnostics in query
statistics. Collection is off by default. It records the projected columns,
required predicate operators/types (without their literal values), scan outcome
and observed row/byte/batch counts at the native adapter boundary.

Reports are capped at 8 KiB and explicitly identify truncation. Identifiers and
nested filter shapes have additional bounds. `local_residuals: not_observed`
means the Go scan boundary cannot report DuckDB's local predicates, joins or
aggregates. These records are not a global execution plan or cost estimate, and
diagnostics do not change pushdown or query results.

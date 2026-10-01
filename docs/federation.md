# Native DuckDB federation

Kelvo keeps DuckDB as its embedded federation engine. The optional native bridge
connects DuckDB's Arrow scanner to the existing Go ClickHouse connector. DuckDB
plans joins and local computation; the connector fetches projected columns and
applies supported filters at ClickHouse. There is no additional server process,
Rust runtime, row-by-row JSON conversion or unsigned DuckDB extension.

This is an opt-in Linux amd64 implementation, with explicit conformance and
deployment limits. It does not make every native connector a federation adapter.
PostgreSQL/MySQL continue to use their existing signed DuckDB extensions, and
file/object snapshots retain their existing Parquet path.

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
at most 32 custom tables per source and across a selected query; names are plain SQL identifiers. Keep the catalog
narrow: initial registration fetches each configured table's schema using a
zero-row native query. Source grants must independently restrict access to the
permitted database tables and functions.

```sh
bin/kelvo query --config examples/federation.yml --sources warehouse \
  --sql 'SELECT account_id, count(*) FROM warehouse.events WHERE event_id >= 1000 GROUP BY account_id' \
  --out accounts.arrow
```

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
| Integer/boolean comparisons | Exact typed constants applied at ClickHouse |
| NULL checks and supported AND/OR combinations | Applied at ClickHouse when passed down by the optimizer; otherwise evaluated by DuckDB |
| Required pushed predicates outside that subset | Explicit unsupported error |
| Residual expressions retained by DuckDB | Evaluated by DuckDB |
| Joins, aggregates, ordering and LIMIT | DuckDB; no general source pushdown for these operators |

The bridge receives typed optimizer predicates rather than rewriting the user's
SQL. It validates every requested column against the acquired schema. Integer
constants retain their width and signedness; no floating-point conversion is
used. String, floating-point and temporal comparison pushdown needs additional
semantic conformance, including collation and timezone behavior. A required
predicate cannot be discarded: DuckDB assumes the producer has applied it.

The source connector owns Arrow decoding, source limits and HTTP cancellation.
Each scan hands off one retained batch and waits for the consumer to advance.
The C interface pins exported Go buffers until DuckDB releases them. Different
scans own independent readers, including when a query references the same table
twice. The query connection and database close before callback factories are
released. Type-specific conversion inside DuckDB can still copy data; this is
not a claim that every type or complete query is zero-copy.

Successful statistics include `federation` entries with source/table identity,
scan count, fetched rows, batches and logical Arrow bytes. These measure data
received by Kelvo, not rows examined inside ClickHouse. Provider query profiling
is needed to establish source CPU, disk reads and index effectiveness.

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
DuckDB's spill files. Cancellation at the outer worker boundary can kill the
process before a connector finishes its remote cleanup; source-side timeouts
and read-only grants remain necessary.

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

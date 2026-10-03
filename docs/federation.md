# Native DuckDB federation

Join selected source tables in DuckDB through Kelvo's optional Go/C++ Arrow bridge. Built-in adapters cover ClickHouse, PostgreSQL, MySQL, SQL Server, Oracle, Snowflake, BigQuery and Databricks on Linux amd64.

## Configure selected tables

1. Build with [bridge support](#build-and-update) and provision narrowly granted source accounts.
2. Register only the tables the query should access:

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

3. Select the source in a federated request and query its `source.table` alias:

```sh
bin/kelvo query --config examples/federation.yml --sources warehouse \
  --sql 'SELECT account_id, count(*) FROM warehouse.events WHERE event_id >= 1000 GROUP BY account_id' \
  --out accounts.arrow
```

Only listed tables become views. Names are plain SQL identifiers, with at most 32 custom tables per source and across a selected query. Registration fetches every configured schema using a zero-row native query; keep catalogs narrow and enforce source table/function grants independently.

| Source | Required remote namespace |
| --- | --- |
| PostgreSQL | `schema`; DSN chooses database |
| MySQL, ClickHouse | `database`; omit `schema` |
| SQL Server, Snowflake, Databricks | `database` and `schema` |
| Oracle | `schema`; omit `database` |
| BigQuery | `database` is data project; `schema` is dataset |

Use exact remote names, without search-path inference. See the [three-source example](../examples/federation-relational.yml) and [additional configurations](federation-adapters.md#configure-exact-remote-names).

PostgreSQL/MySQL require their [native verified-TLS connection formats](sources-relational.md), which differ from signed-extension strings. Without a `federation` section, their existing signed-extension route remains available. File/object snapshots use Parquet.

`max_scan_rows`/`max_scan_bytes` bound each scan; omitted values inherit query limits. Overruns fail the query rather than truncate a join input. At most four scans run concurrently, further bounded by query threads; saturation fails rather than waiting for another join input's slot.

## What runs where

| Operation | Execution |
| --- | --- |
| Requested columns | Source SQL; required Arrow order retained |
| Integer/Boolean comparisons | Exact typed constants in source dialect |
| Integer/Boolean NULL checks and supported AND/OR | Source when pushed by the optimizer; otherwise DuckDB |
| String, decimal, float, temporal and other predicates, including NULL checks | DuckDB |
| Unsupported required pushed predicate | Explicit error |
| Residual expressions, joins, aggregates, ordering and LIMIT | DuckDB; no general source pushdown |

The bridge receives typed optimizer predicates, validates columns against the schema and preserves integer width/signedness. Required predicates cannot be dropped: DuckDB assumes the source applied them. Restricting pushdown avoids collation, rounding or timezone changes, but may fetch more rows and reach scan limits sooner.

Sparse integer IN lists may become optional OR-of-equality hints while DuckDB keeps the original predicate. Hints permit at most 256 non-NULL constants of one supported type. They are omitted if the plan exceeds 32 KiB, 256 top-level filters or 1,024 predicate nodes, and are never extracted from an OR arm. Unsupported, negative, NULL-containing or larger lists keep local evaluation or another supported optimizer plan; every SQL spelling is not guaranteed to push down.

ClickHouse supplies ArrowStream. PostgreSQL/MySQL/SQL Server/Oracle convert native driver rows to Arrow; Snowflake/BigQuery/Databricks convert paged JSON. These latter routes do not provide columnar source wire transport.

Each scan owns an independent reader, retains one batch for handoff and waits for consumption. C exports pin Go buffers until DuckDB releases them. Connections/databases close before callback factories; DuckDB may copy during type conversion, so complete queries are not guaranteed zero-copy.

`federation` statistics report source/table, scans, fetched rows, batches and logical Arrow bytes. These count data received by Kelvo. Use provider profiling for source CPU, disk reads and index effectiveness.

## Add an adapter

Implement `Driver` and `Relation` from the public [`federation` package](../federation/federation.go), then register a trusted compiled-in source type. Registrations cannot replace built-ins or load query-supplied code. The [contributor guide](federation-adapters.md) provides a working example and acceptance checklist.

## Validation

[Expanded acceptance](validation.md#expanded-federation-and-public-adapter-sdk) separates live SQL Server, protocol fixtures and public SDK checks from pending live Oracle/Snowflake/BigQuery/Databricks acceptance.

[Initial live checks](validation.md#duckdb-custom-federation-adapter) cover exact values, filtering, self-joins and CSV joins. [NYC Taxi capacity tests](federation-capacity.md) cover larger joins, sorting, constrained memory, remote delivery and tenant workloads. Use their recorded conditions when citing throughput; fixture measurements are not general production capacity.

## Build and update

Build on the Linux amd64 host:

```sh
python3 scripts/provision_duckbridge.py artifacts/duckbridge --go go
export CGO_CXXFLAGS="-I${PWD}/artifacts/duckbridge/headers"
go build -p 2 -modfile artifacts/duckbridge/duckbridge.mod \
  -tags duckdb_arrow,duckbridge -o bin/kelvo ./cmd/kelvo
```

For containers, pass `--build-arg DUCKBRIDGE=1`. Headers, Python and patched sources stay in the build stage; runtime uses the compiled binary and normal libraries. Ordinary builds remain available and reject custom federation explicitly.

Provisioning verifies the pinned source archive SHA-256 and Go module sum, copies the official driver into artifacts, applies the scoped 23-line accessor patch and writes a separate module file. Baseline `go.mod` is unchanged. The accessor borrows a connection only inside `sql.Conn.Raw` without relying on private Go layouts.

Headers, static library, driver patch, runtime version check and tests must advance together. Follow [upgrade checks](upgrading-duckdb.md); never replace headers/library independently. [Original MIT notices](../licenses/duckdb.txt) are retained.

## Deployment boundaries

Tenant-bound subprocesses, Landlock, network policy and resource controls remain required. The shim is native code; direct user calls to pointer-bearing Arrow functions are denied. It loads no arbitrary library or request-selected endpoint. Registered credentials stay in the tenant query process.

Pinned DuckDB execution materializes before Arrow delivery. Bounded source handoff does not bound joins, sorts, native allocations or total RSS. Scan limits are per scan, not shared across tenants. Enforce process/container CPU, memory and PID limits and budget spill storage.

On Linux, cancellation, outer deadlines and IPC/sink failures send SIGTERM, allow up to 750 ms for cleanup, then SIGKILL remaining group members before reaping the leader. PostgreSQL sends bounded connection-keyed cancellation; MySQL adds a SELECT timeout because socket closure may not stop remote work. Network failure or process death can prevent cleanup. See [relational cancellation](sources-relational.md#resource-and-validation-boundaries).

Use database-enforced read-only accounts. PostgreSQL/MySQL/Oracle use rollback-only read-only transactions; SQL Server uses rollback-only transactions with grants enforcing reads. Independent scans have no shared snapshot, including repeated reads of one source. Use source-consistent exports when required.

## Why this approach

The pinned [Go table-function API](https://github.com/duckdb/duckdb-go/blob/v2.10506.0/table_udf.go) lacks the full filter-planning hooks. DuckDB's [Arrow stream factory](https://github.com/duckdb/duckdb/blob/v1.5.6/src/function/table/arrow.cpp) supplies them, allowing a small C++ boundary around existing Go connectors.

A DataFusion bridge would add another planner, connector layer and upgrade path. [DataFusion FFI](https://github.com/apache/datafusion/tree/main/datafusion/ffi) primarily connects Rust libraries; its [federation framework](https://github.com/datafusion-contrib/datafusion-federation) is alpha, with database coverage from [table providers](https://github.com/datafusion-contrib/datafusion-table-providers). Reconsider that path if matched workload measurements justify it.

## Actual scan diagnostics

Set `scan_diagnostics: true` or CLI `--scan-diagnostics` to record projected columns, required predicate operators/types without literals, outcomes and observed row/byte/batch counts. Collection is off by default and does not change query behavior.

Reports are capped at 8 KiB, identify truncation and bound identifiers/filter shapes. `local_residuals: not_observed` means this boundary cannot report DuckDB's local predicates, joins or aggregates; diagnostics are not a global plan or cost estimate.

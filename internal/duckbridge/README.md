# Pinned native Arrow scan bridge

Connect DuckDB's Arrow scan planning callbacks to Go `array.RecordReader` producers. The optional C++17 shim uses a private copy of DuckDB 1.5.6 `arrow_scan`; baseline builds select an unavailable stub.

1. Create a fresh in-memory DuckDB database and trusted schema, then call `New(ctx, fullSchema, producer)`.
2. Call `factory.Register(rawConnection, schemaName, viewName)` only inside `sql.Conn.Raw`.
3. Close results, connection and database before `factory.Close()`. Check `factory.Err()` for the first callback failure.

Views contain process-local pointers: never persist or reuse them beyond the factory lifetime. The scoped driver patch lends an opaque native handle only inside `sql.Conn.Raw`, behind `duckbridge`; it does not assume Go struct layouts or expose pointer SQL/UDF arguments.

Provision and test on the designated VM:

```sh
python scripts/provision_duckbridge.py /absolute/artifacts/duckbridge --go /absolute/go
CGO_CXXFLAGS=-I/absolute/artifacts/duckbridge/headers \
  /absolute/go test -p 1 -modfile=/absolute/artifacts/duckbridge/duckbridge.mod \
  -tags duckdb_arrow,duckbridge ./internal/duckbridge
```

The helper pins the source archive SHA-256, module sum and patch checksum, writing a separate module file without changing baseline `go.mod`. Support is Linux amd64 with matching static driver libraries. `Available`/`New` verify runtime version. Upgrade C++ headers, library and tests together; Arrow C Data stability does not stabilize DuckDB's planner ABI.

Producers receive ordered columns and typed filters. Every required filter must execute exactly or fail: DuckDB may already have removed it from local evaluation. Integer/Boolean comparisons, NULL checks and logical combinations are supported; other types remain local. Only explicitly optional wrappers may be omitted. For a zero-column count scan, supply a private constant column to carry row counts.

Each scan/rescan/self-join owns its reader. Go state crosses C as `cgo.Handle` integers. Exported batches retain Arrow data and pin all Go buffers, including nested/dictionary buffers, until C release. DuckDB may still copy during conversion. Errors, cancellation and callback panics fail the query; panic details stay private. Adapters enforce source row/byte/memory/concurrency budgets.

Reject direct user calls to `arrow_scan`, `arrow_scan_dumb` and private `kelvo_arrow_scan_` functions before source setup. Only trusted setup registers these views; the executor/adapter controls source identity, credentials and read-only SQL. No unsigned extension or extra service is loaded.

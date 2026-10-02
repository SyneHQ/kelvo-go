# Pinned native Arrow scan bridge

This optional C++ shim copies DuckDB 1.5.6's built-in `arrow_scan` into a private,
per-factory table function with Kelvo's type-pushdown capabilities. Its planning
callbacks connect to a Go `array.RecordReader` producer. It does not install or load an
unsigned DuckDB extension and does not create a separate execution service.
Baseline builds select the unavailable stub.

`New(ctx, fullSchema, producer)` owns a factory. Call
`factory.Register(rawConnection, schemaName, viewName)` inside `sql.Conn.Raw`,
after creating the trusted schema. Use only a fresh in-memory DuckDB database.
Close every result, the connection, and the database before `factory.Close()`;
stored view definitions contain process-local callback pointers and must never
be persisted or reused outside that lifetime. `factory.Err()` preserves the
first callback failure for the outer executor's sanitized error mapping.

The Go driver does not expose a pointer type through SQL, UDF arguments, or its
public connection API. The build helper copies the official pinned module to a
private artifact directory and applies a small, reviewed connection accessor
patch. That method is only compiled with `duckbridge` and only lends the opaque
C API handle during `sql.Conn.Raw`. It makes no assumptions about Go struct
layout. The helper emits a separate module file; the repository's baseline
`go.mod` is unchanged.

All downloads, compilation, and tests run on the designated VM:

```sh
python scripts/provision_duckbridge.py /absolute/artifacts/duckbridge --go /absolute/go
CGO_CXXFLAGS=-I/absolute/artifacts/duckbridge/headers \
  /absolute/go test -p 1 -modfile=/absolute/artifacts/duckbridge/duckbridge.mod \
  -tags duckdb_arrow,duckbridge ./internal/duckbridge
```

The helper pins the DuckDB source archive SHA256, Go module checksum, and local
patch checksum. Initial support is Linux amd64 with the pinned static Go driver
libraries and C++17. `Available` and `New` verify the runtime library version
before C++ bridge operations. A DuckDB upgrade requires reviewing the internal
C++ ABI and rerunning these tests; Arrow's stable C Data ABI does not make the
DuckDB planning callback ABI stable.

The producer receives ordered columns and a typed predicate tree. DuckDB removes
pushed filters from its own scan, so every required filter must be executed
exactly or the query fails. Predicates on integer/boolean columns support
comparisons, null checks, and conjunctions. Other column types do not advertise
filter pushdown: DuckDB retains their expressions and NULL checks locally.
Unsupported required filters still fail before producing a source stream.
Only explicitly optional filter wrappers may
be omitted. Ordinary residual expressions left above the Arrow scan remain
DuckDB's responsibility. The pinned optimizer normally requests one physical
column for `count(*)`; if it requests none, the producer supplies a private
constant column solely to carry row counts.

Each scan, including rescans and self-joins, receives a distinct owned reader.
Go state crosses C only through `cgo.Handle` integers. Each exported batch
retains its Arrow data and pins all Go buffers, including nested and dictionary
buffers, until the C ArrowArray release callback runs. This avoids an additional
row conversion in the bridge; DuckDB may copy during its own Arrow conversion.
Reader errors, cancellation, and callback panics are reported as failures;
diagnostic panic values never enter public errors. The adapter must still enforce
source scan, byte, row, memory, and concurrency budgets.

Direct user calls to `arrow_scan`, `arrow_scan_dumb` and every private
`kelvo_arrow_scan_` function must remain denied before source setup. Only
trusted setup registers pointer-bearing views. Source identity, credential
selection, and read-only SQL generation belong to the enclosing executor and
native adapter, not to arbitrary table-function arguments.

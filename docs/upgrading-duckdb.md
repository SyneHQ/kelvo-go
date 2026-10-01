# Updating DuckDB

1. Select a reviewed `duckdb-go` release and its matching DuckDB/Arrow/bindings versions.
2. Update `go.mod` and `go.sum` together; inspect release notes and dependency/license changes.
3. Provision matching signed PostgreSQL/MySQL extensions for each target engine/platform. Do not reuse old extension binaries.
4. Run local-file and real-database conformance: types, NULLs, parameters, joins, permissions, cancellation, limits and schema/error behavior.
5. Re-run the same first-batch, completion, native memory, spill and throughput measurements against the previous version. Check whether the adapter's non-streaming behavior has changed.
6. Build a versioned image/binary, validate staging, canary the update and retain rollback artifacts. Persistent DuckDB-file upgrades require backups and storage-format compatibility checks before rollback is promised.

Keep any required downstream driver patch small, justified and regression-tested.
Kelvo should normally need changes only at its DuckDB adapter boundary. A new upstream version is not automatically safe or faster.

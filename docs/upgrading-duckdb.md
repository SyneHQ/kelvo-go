# Updating DuckDB

1. Select a reviewed `duckdb-go` release and its matching DuckDB/Arrow/bindings versions.
2. Update `go.mod` and `go.sum` together; inspect release notes and dependency/license changes.
3. Provision matching signed PostgreSQL/MySQL extensions for each target engine/platform. Do not reuse old extension binaries.
4. Run local-file and real-database conformance: types, NULLs, parameters, joins, permissions, cancellation, limits and schema/error behavior.
5. Re-run the same first-batch, completion, native memory, spill and throughput measurements against the previous version. Check whether the adapter's non-streaming behavior has changed.
6. Build a versioned image/binary, validate staging, canary the update and retain rollback artifacts. Persistent DuckDB-file upgrades require backups and storage-format compatibility checks before rollback is promised.

Keep any required downstream driver patch small, justified and regression-tested.
Kelvo should normally need changes only at its DuckDB adapter boundary. A new upstream version is not automatically safe or faster.

For native federation builds, also update the version and archive/module checksums
in `scripts/provision_duckbridge.py`, review `scripts/duckbridge-driver.patch`,
and match the runtime check in `internal/duckbridge`. Build artifacts use a
separate module file with a local replacement. Regenerate it from the new pins;
do not reuse a patched module or C++ headers from an older engine. Run both the
ordinary and `duckbridge` test/build variants, strict cgo pointer checks with GC,
source-predicate conformance and the real ClickHouse/PostgreSQL/MySQL acceptance
scripts. Re-run mixed-source joins, substantial hash-build inputs, low-memory
spill, complete exports and cluster admission/cancellation against the same
versioned dataset and source settings. A stable
Arrow C Data ABI does not make DuckDB's C++ planner interfaces version-independent.

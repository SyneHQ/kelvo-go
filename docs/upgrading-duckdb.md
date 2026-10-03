# Updating DuckDB

Upgrade the Go driver, engine, bindings and extensions as a matched set. Run downloads, builds and acceptance on the designated VM under [AGENTS.md](../AGENTS.md).

1. Select a reviewed `duckdb-go` release and matching DuckDB/Arrow/bindings versions. Check release notes, licenses and dependency changes.
2. Update `go.mod`/`go.sum` together and provision matching signed PostgreSQL/MySQL extensions for every target platform.
3. Run file and live-database conformance: exact types, NULLs, parameters, joins, permissions, cancellation, limits and schema/errors.
4. Compare first-batch/completion latency, native memory, spill and throughput against the prior version. Verify whether execution still materializes before delivery.
5. Build versioned artifacts, validate staging and canary the update. Keep rollback binaries; persistent DuckDB files also need backups and storage-format checks.

For native federation builds:

1. Update archive/module checksums in `scripts/provision_duckbridge.py`, review `scripts/duckbridge-driver.patch`, and match `internal/duckbridge` runtime checks.
2. Regenerate the separate module file and patched driver from the new pins. Never mix old headers, libraries or patched artifacts.
3. Run ordinary and `duckbridge` builds/tests, strict cgo pointer checks under GC, predicate conformance and real ClickHouse/PostgreSQL/MySQL acceptance.
4. Repeat mixed-source joins, large hash builds, low-memory spill, full exports and cluster admission/cancellation on identical versioned datasets/settings.

Keep downstream patches small and tested. Arrow's stable C Data ABI does not stabilize DuckDB's C++ planning interfaces or guarantee a faster release.

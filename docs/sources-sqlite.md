# SQLite sources

Query selected SQLite files through DuckDB's signed `sqlite` extension. Kelvo attaches canonical paths with `TYPE SQLITE, READ_ONLY` and rejects arbitrary paths, extensions and write SQL.

1. Use the DuckDB Arrow build pinned to DuckDB 1.5.6.
2. Provision the matching `sqlite_scanner.duckdb_extension` in the approved extension directory.
3. Select the configured source in a federated query.

[Linux amd64 acceptance](evidence/sqlite-acceptance.json) records the artifact URL/checksum and checks exact integers, text, binary, NULLs, write denial and unselected-file rejection. Concurrent access and throughput were not benchmarked.

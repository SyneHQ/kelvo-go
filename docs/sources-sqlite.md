# SQLite sources

SQLite federation uses DuckDB's signed `sqlite` extension from the configured,
version-matched extension directory. Kelvo attaches only the selected canonical
source path with `TYPE SQLITE, READ_ONLY`; arbitrary extension names, paths,
and write SQL remain unavailable.

This requires the DuckDB Arrow build and a provisioned extension matching
pinned DuckDB 1.5.6. Its official artifact is named
`sqlite_scanner.duckdb_extension`; load it from the approved extension directory.
The [Linux amd64 acceptance fixture](evidence/sqlite-acceptance.json) verified
the exact integer `9007199254740993`, text, binary bytes, NULLs, write denial,
and rejection of an unselected file. The fixture also records the official
download URL and artifact checksum. This is a functional check, not a throughput
or concurrent-access benchmark.

# Dependency rationale

| Dependency | Purpose | License |
| --- | --- | --- |
| duckdb-go v2.10506.0 / DuckDB 1.5.6 | Embedded SQL execution, file analysis and supported federation | MIT |
| Arrow Go v18.5.1 | Typed record batches and IPC encoding/decoding | Apache-2.0 |
| Go standard library | HTTP, subprocess lifecycle, configuration and CLI | Go BSD-style license |

Transitive dependencies are pinned by `go.sum`; review their licenses when distributing binaries. DuckDB's driver includes platform-specific native libraries. Matching signed extensions are external provisioned artifacts, with their own dependency notices.

The ClickHouse adapter uses standard HTTP and source-produced ArrowStream rather than introducing another native driver into this first slice. Native protocols, Flight SQL, additional connectors and CDC require separate justifications and acceptance evidence.

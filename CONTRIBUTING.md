# Contributing

Kelvo uses `cargo` as its default branch. Choose an issue from the [delivery board](https://github.com/orgs/SyneHQ/projects/3), or open one before a substantial API, connector or storage change.

1. Read [AGENTS.md](AGENTS.md) and the [delivery process](docs/delivery-process.md).
2. Use a purpose-based branch, such as `fix/query-cancellation`. Keep each commit to **ten files or fewer**.
3. Preserve exact types, permissions and cancellation. Return an explicit error for unsupported behavior.
4. Run the relevant tests, vet and build on a Linux development host or CI:

```sh
go test -tags duckdb_arrow ./...
go vet -tags duckdb_arrow ./...
go build -tags duckdb_arrow -o bin/kelvo ./cmd/kelvo
```

5. Open a focused PR describing the problem, resulting behavior and validation. Link the issue and include failed or unrun required checks.

For application SDK changes, run `CGO_ENABLED=0 go test ./client ./query ./delegation ./resolver ./operations`
and the [independent application check](docs/application-sdk.md#verify-an-independent-build).
Runtime isolation tests need the [Linux containment setup](docs/process-containment.md).

Connector changes need real-database checks for types/NULLs, cancellation, limits and permissions. Protocol fixtures alone do not prove compatibility.

Performance changes need reproducible before/after results: versions, data, plans, cache state, limits, exact answers, first-batch time, completion time and memory scope. Keep failed trials; never publish credentials or private data.

Justify new dependencies and check their licenses. Contributions use Apache-2.0; preserve third-party notices. Maintainers review merges and releases.

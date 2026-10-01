# Contributing

Contributions are welcome through GitHub issues and pull requests. The default branch is `cargo`.

Discuss substantial API, connector or storage changes before implementation. Focus a pull request on one behavior and describe the problem, resulting behavior and validation. Preserve source-native types and permissions; unsupported behavior should return an explicit error.

Run the tagged Go tests, vet and build from the README. Connector changes need tests against a real database, including NULL/type fidelity, cancellation, limits and source permissions. A fake HTTP server is useful for error cases but is not database compatibility evidence.

Performance changes need reproducible before/after workloads with versions, schemas, query plans, cache state, CPU/memory limits, returned-result validation, first-batch latency, completion time and memory scopes. Keep failed trials. Do not submit production/customer data or credentials.

Use descriptive branches such as `fix/query-cancellation` or `feature/sqlserver`. Dependencies need a written justification and compatible licensing. Do not add an engine, deployment platform or model runtime merely for optional future use.

Code contributions are accepted under Apache License 2.0. Preserve applicable notices for adapted third-party code. Maintainers review and merge contributions; opening a pull request does not publish a release.

# Export lifecycle acceptance

[Cargo CI run 37179473684](https://github.com/SYNEHQ/kelvo-go/actions/runs/37179473684) passed on merged `b1a0ea5`. Both jobs checked out that exact commit. The [public receipt](evidence/cargo-b1a0ea5-acceptance.json) records revisions, binary hashes, job IDs and artifact checksums.

| Current gate | Result |
| --- | --- |
| Actual child cancellation, source quotas and commit-boundary crashes | All five required tests passed; no missing, failed or skipped gate |
| NATS 2.14.7 and 2.15.0 | ACL, dispatch and actual-worker lifecycle gates passed on each |
| Lifecycle on each broker | Five scenarios plus their parent passed: submit disconnect, LZ4/NULLs, worker restart, lost completion and replacement-key access |
| Lost-completion checkpoint on each broker | `cancelled`, durable receipt present, no reached deadline, three total executions across the campaign |

The lost-completion case does not replay its SQL. Ready exports support repeat downloads with current authorization. Ordinary and race suites also cover storage limits, expiry, framing, TLS slow readers and HTTP/2 stream isolation. [Storage and API](exports.md) · [TLS evidence](tls-stream-cancellation.md)

## Retained historical failure

The [earlier hosted failure](evidence/export-ci-37144625480-failure.json) remains recorded with an **unknown cause**. The separately reproduced [dispatch race](export-dispatch-reliability.md) was fixed; it is not a proven explanation for that run. The current direct passes establish the merged source's behavior without changing the old result.

An [earlier isolated reproduction](evidence/export-diagnostics-reproduction-1513901.json) passed six parser controls and five lifecycle scenarios plus their parent. It used test source `1513901`, a worker built from `0da7523`, and NATS 2.14.7. Its receipt retains exact hashes, resource limits, cleanup and the preceding archive-extraction failure; that failed attempt ran no tests or broker.

## Diagnostic contract and limits

Public diagnostics contain only expected/observed state, deadline and receipt flags, a finite error class, execution count and an allowlisted test location. The 20-second state wait, cancellation requirement and execution-count checks remain unchanged. Full assertions stay in private logs.

These are controlled functional gates. Each broker campaign uses one broker; commit-crash tests use a synced broker test double. They do not establish multi-host availability, sustained export capacity, WAN throughput or live-provider acceptance. Those [production gates](production-status.md) remain separate.

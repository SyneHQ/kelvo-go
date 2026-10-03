# Export validation

The combined `d144a43` candidate passed on the dedicated Linux test VM. The [receipt](evidence/export-runtime-final.json) records source/binary hashes, skipped gates and cleanup; `validation/export-final-20261003` preserves its source.

| Gate | Result |
| --- | --- |
| Ordinary / bridge suites | 3,405 / 3,463 passing test events; 79 / 80 opt-in skips |
| Affected-package race checks | 960 passing events; 41 opt-in skips |
| Worker sandbox, containment, cancellation, source quotas and commit crashes | 17 passing events; no skips |
| Separate NATS roles and actual worker/gateway lifecycle | 20 passing events across 2.14.7 and 2.15.0; no skips |
| Gateway completion/cancellation regression | 20 repetitions; 160 passing events |
| Build and vet | Passed |

The service had one CPU, 3 GiB memory, no swap/capabilities and a private network. Source files stayed unchanged; owned brokers, job groups and the service cgroup were removed.

Real TLS tests cover blocked readers, revocation, HTTP/2 stream isolation and final buffered writes. See [transport evidence](tls-stream-cancellation.md). Crash tests use a durable local broker double; the separate NATS tests cover the real gateway/worker lifecycle. These scopes are not interchangeable.

Earlier [integration](evidence/export-runtime-integration.json), [broker](evidence/export-broker-acl.json) and TLS failures remain recorded. The final receipt also retains the CI fixture race and its regression. Ordinary-suite skips do not count as provider acceptance.

This establishes the tested implementation's behavior with controlled data. It does not certify live providers, sustained export capacity, replicated storage or multi-host availability. [Remaining gates](production-status.md) · [Setup and API](exports.md)

# NATS compatibility and configuration refusal

Kelvo passed a real three-broker upgrade from **NATS 2.14.7 to 2.15.0 and back**, and full-engine client upgrades from **nats.go v1.53.1 to v1.54.0 and back** on both broker releases. The declared application source is `210331831b013e4c79a16794ceb7d1eaf463d0b7`. The default dependency remains v1.54.0.

These are exact compatibility measurements for the recorded artifacts. They do not establish compatibility with arbitrary releases or configurations, or certify a production deployment. The predecessor client is a test fixture; v1.54.0 includes reconnect and subscription race fixes and remains the supported default.

## Tested matrix

| NATS server | Full-engine Go client | Evidence |
| --- | --- | --- |
| 2.14.7 | v1.53.1 | Initial and rolled-back application pairs; exact native and CTE results |
| 2.14.7 | v1.54.0 | Upgraded application pair and broker campaign starting/rollback state |
| 2.15.0 | v1.53.1 | Initial and rolled-back application pairs; exact native and CTE results |
| 2.15.0 | v1.54.0 | Upgraded application pair and broker campaign upgraded state |

| Campaign | Observed work | Elapsed time | Result |
| --- | --- | --- | --- |
| Broker upgrade and rollback | Six serial replacements across three brokers | 43.181 s | Passed |
| Client upgrade and rollback on 2.14.7 | Two gateways and two workers, forward then back | 84.920 s | Passed |
| Client upgrade and rollback on 2.15.0 | Two gateways and two workers, forward then back | 84.467 s | Passed |

Every transition started with two observed running native queries and two queued CTE queries across two tenants. All **88 original admitted handles across 22 transitions** completed with exact results, durable success, principal ownership and consumed-result refusal. The source fixture observed one request per native marker. This is a bounded observation, not a general exactly-once execution guarantee.

Broker transitions also verified that the two surviving processes kept their identities, each restarted peer reported its declared version, all three metadata members agreed on a current leader, and both leader-reported replicas were current and online. Two fresh tenant queries after every broker rejoin exercised new durable writes, dispatch, result claims and completion. Client transitions required draining readiness, graceful exit, current key-revision floors and revoked-key denial. No stream moves, replica changes or cluster scaling occurred while broker versions differed.

Each complete campaign ran in its own Linux service with one CPU quota, 3 GiB memory maximum, zero swap, 256 tasks maximum, a non-root user and a private network containing only loopback. These are enforced test limits, not recommended production sizing. The client campaigns used separate services in parallel. All nine campaign/build cgroups were gone or empty after verification.

## Security configuration refusal

Older binaries must refuse a configuration they cannot enforce. Two historical binaries, `c7ecbf1a8f8391b13cd1bc96a4ae847d5c8b052c` and `0d6b489fb072c823a8cd6d20e6beba98a3aa3158`, first executed a working whole-source principal query. Each then rejected these independent options:

| Unsupported option | Gateway | Worker | Initializer |
| --- | --- | --- | --- |
| `row_column_policy` | Strict YAML refusal | Strict YAML refusal | Strict YAML refusal |
| `audit` | Strict YAML refusal | Strict YAML refusal | Strict YAML refusal |

Each probe returned a nonzero exit and the explicit invalid-cluster-YAML diagnostic. No listener was observed, no broker connection or source request was added, and no audit journal was created. Configuration hashes stayed unchanged; create-only initialization subsequently revalidated the original durable metadata.

The current candidate independently passed baseline, row-policy and audit positive controls in fresh clusters. The row-policy control returned only the permitted row, and the audit control produced principal-attributed gateway and worker receipts. This does **not** authorize rolling those new configurations onto the historical binaries. Keep current security settings when rolling back; choose a binary that supports them or refuse the rollback.

## Artifact and dependency provenance

The broker campaign held the current full-engine binary constant. Client campaigns used two full-engine builds from the same source archive, with identical `duckdb_arrow,duckbridge` tags and `-trimpath` flags. Alternate modfiles changed only the required NATS client; the source `go.mod` and `go.sum` were unchanged. Effective Require/Replace sets, alternate modfile/sum digests, native bridge/header tree digests and the actual embedded module selections are recorded. Only `github.com/nats-io/nats.go` changed among the linked dependencies. This manifest describes the full engine's linked dependencies, not the entire Go module graph.

| Artifact | SHA-256 prefix |
| --- | --- |
| Constant engine for broker campaign | `cef8220459d316ab` |
| Same-source client v1.53.1 engine | `8db014e3d491e61b` |
| Same-source client v1.54.0 engine | `67ef24a80310de33` |
| Shared Landlock launcher | `4df0e390ae250806` |

The [sanitized evidence](evidence/nats-compatibility.json) contains full digests, every matrix cell, resource limits, failure history and final verification. It preserves:

- The initial passing old-binary controls, which did not yet include in-process resource measurements.
- A refused resource preflight: this hardened host denies a non-root read of `/proc/1/ns/net`. The final runner compares its namespace with the launcher's nonsecret host namespace identity and requires loopback-only interfaces. No cluster was provisioned by the failed attempt.
- An offline build-manifest preflight that tried to enumerate unused module-graph entries absent from the cache. The final build records actual embedded dependencies and validates effective Require/Replace sets; both full binaries compiled offline.
- Final positive/refusal controls, broker/client campaigns, 73 passing regression controls and successful report reconciliation. All original runtime-harness hashes were rechecked after execution.

## Reproduction

Use an isolated test VM with the qualified Go toolchain, native bridge, PyArrow/PyYAML, verified source archive and cached official NATS archives. Never point these scripts at an existing application cluster. The harness uses generated local secrets and private state; publish only its sanitized evidence.

Before entering the private network, obtain the launcher's namespace with `readlink /proc/self/ns/net` and pass it as `KELVO_TEST_HOST_NETWORK_NAMESPACE`. Enforce `User`, `PrivateNetwork=yes`, `PrivateTmp=yes`, `CPUQuota=100%`, `MemoryMax=3G`, `MemorySwapMax=0`, `TasksMax=256`, `NoNewPrivileges=yes`, `KillMode=control-group`, `UMask=0077` and a bounded runtime through the service manager. The scripts check the actual cgroup and namespace.

Run the focused controls using the qualified Python environment:

```sh
cd scripts
python -m unittest test_cluster_fixture test_config_refusal_acceptance \
  test_nats_compatibility_acceptance test_build_nats_compatibility \
  test_operational_acceptance test_process_loss_acceptance \
  test_rolling_upgrade_acceptance
```

The three entrypoints have explicit path arguments; inspect `--help` before launching each as a bounded private service:

1. `scripts/config_refusal_acceptance.py`: run `--stage old` for the immutable historical pair, and `--stage candidate` for the exact current candidate. Its matrix names each binary, full revision and checksum. The historical identities are pinned in the script.
2. `scripts/build_nats_compatibility.py`: supply the exact source/archive, already validated bridge modfile, native headers, launcher and Go executable. It builds both full client variants offline in a fresh external directory, checks unchanged source/native inputs and writes the client matrix. The NATS module sums are pinned.
3. `scripts/nats_compatibility_acceptance.py`: run `--mode broker --start-server-version 2.14.7` with a matrix holding both application entries identical. Then run `--mode client` separately with `--start-server-version 2.14.7` and `2.15.0`, using the generated same-source client matrix. Pass both cached broker archives, both source archives, embedded-module manifests, binaries and a fresh fixture/output path.

The shared fixture defaults remain NATS 2.15.0. Compatibility runs must explicitly declare each peer's expected known version. Unknown or missing versions fail readiness. NATS 2.14.7 lacks the `quorum_needed` monitoring field; only that declared version uses its older monitoring contract, still requiring three distinct healthy members and current consensus, followed by real writes. NATS 2.15.0 retains its stronger quorum and rescue checks.

The source is a strict ClickHouse HTTP/Arrow protocol fixture, not a ClickHouse server or benchmark. CTEs use deterministic synthetic Parquet snapshots. These results do not cover WAN behavior, external database-provider semantics, sustained throughput, multi-host faults or fleet-wide SLOs. Repeat the exact intended deployment matrix and configuration before rollout.

## Official compatibility guidance

- [NATS 2.15 upgrade guidance](https://docs.nats.io/release-notes/upgrade-to-2.15): upgrade from at least 2.14.7; downgrade only to 2.14.7 or newer, with mixed-version restrictions.
- [NATS server 2.14.7](https://github.com/nats-io/nats-server/releases/tag/v2.14.7) and [2.15.0](https://github.com/nats-io/nats-server/releases/tag/v2.15.0).
- [nats.go v1.53.1](https://github.com/nats-io/nats.go/releases/tag/v1.53.1) and [v1.54.0](https://github.com/nats-io/nats.go/releases/tag/v1.54.0).

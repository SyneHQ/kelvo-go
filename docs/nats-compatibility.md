# NATS compatibility

Kelvo passed the matrix below at application source `2103318`. NATS Go client v1.54.0 remains the default; the predecessor is a test fixture.

| NATS server | Go client | Result |
| --- | --- | --- |
| 2.14.7 | v1.53.1 | Passed |
| 2.14.7 | v1.54.0 | Passed |
| 2.15.0 | v1.53.1 | Passed |
| 2.15.0 | v1.54.0 | Passed |

## What ran

| Campaign | Transitions | Time |
| --- | --- | --- |
| Broker 2.14.7 → 2.15.0 → 2.14.7 | Three brokers, one at a time, forward and back | 43.181 s |
| Client v1.53.1 → v1.54.0 → v1.53.1 on server 2.14.7 | Two gateways and two workers, forward and back | 84.920 s |
| Same client rollout on server 2.15.0 | Two gateways and two workers, forward and back | 84.467 s |

All **88 original query handles across 22 transitions** returned exact results. Each wave began with two running native queries and two queued CTE queries. Checks covered consumed-result refusal, principal ownership, revoked keys, graceful drain and cleanup. One observed source request per marker is a bounded observation, not an exactly-once guarantee.

Each broker rejoin required the declared peer versions, three-member consensus, current replicas and two new durable queries. Surviving broker identities stayed unchanged. No stream moves, replica changes or scaling occurred during mixed versions.

Each campaign used a separate non-root Linux service: one CPU quota, 3 GiB RAM, no swap, 256 tasks and a private loopback-only network. These are test limits, not production sizing. All nine campaign/build cgroups were empty or gone afterward.

## Security configuration checks

Historical binaries `c7ecbf1` and `0d6b489` first passed a whole-source principal query. Each then rejected `row_column_policy` and `audit` independently for gateway, worker and initializer: **12 refusals**. No added broker/source connection, observed listener or audit journal appeared; configuration and durable metadata stayed unchanged.

The current candidate passed separate baseline, row-policy and audit controls. This does not permit enabling those settings on older binaries. Roll back only to a binary that supports the active configuration.

## Evidence and limits

[Full receipts](evidence/nats-compatibility.json) contain exact source, binary, broker, modfile and native-bridge hashes. Both client builds used identical source and build flags; only the linked NATS client changed. Source `go.mod`/`go.sum` stayed unchanged. The dependency inventory covers linked engine modules, not the entire Go module graph.

The record retains the initial controls and two failed preflights: a denied `/proc/1/ns/net` read and an offline request for uncached, unused module metadata. Final checks use the launcher's namespace identity and actual linked dependencies. All **80 compatibility controls** and [102 integrated controls](evidence/nats-compatibility-integration.json) passed; stricter ordered-transition validation rechecked the retained runtime reports without rerunning them.

The source is a ClickHouse HTTP/Arrow protocol fixture; CTEs use synthetic Parquet snapshots. This proves the declared single-host matrix, not WAN throughput, live-provider behavior, arbitrary-version compatibility or deployment certification.

## Reproduce

Use an isolated Linux test VM with the qualified Go/native bridge, PyArrow/PyYAML and checksum-verified source and broker archives.

```sh
cd scripts
python -m unittest test_cluster_fixture test_config_refusal_acceptance \
  test_nats_compatibility_acceptance test_build_nats_compatibility \
  test_operational_acceptance test_process_loss_acceptance \
  test_rolling_upgrade_acceptance
```

1. Run `config_refusal_acceptance.py --help` for historical and current configuration probes.
2. Run `build_nats_compatibility.py --help` to build both client variants offline using external modfiles.
3. Run `nats_compatibility_acceptance.py --help` for broker and per-server client campaigns. Use fresh fixture/output paths and explicit artifact matrices.
4. Enforce the service limits above, `PrivateTmp=yes`, `NoNewPrivileges=yes`, `KillMode=control-group`, `UMask=0077` and a bounded runtime. Pass the launcher's `readlink /proc/self/ns/net` value as `KELVO_TEST_HOST_NETWORK_NAMESPACE`.

Never target an existing application cluster. Keep generated secrets and raw logs private. Unknown broker versions fail readiness; only explicitly declared 2.14.7 uses its older monitor schema, followed by real durable writes.

[Official upgrade/downgrade rules](https://docs.nats.io/release-notes/upgrade-to-2.15) · [Server 2.14.7](https://github.com/nats-io/nats-server/releases/tag/v2.14.7) · [Server 2.15.0](https://github.com/nats-io/nats-server/releases/tag/v2.15.0) · [Client v1.54.0](https://github.com/nats-io/nats.go/releases/tag/v1.54.0)

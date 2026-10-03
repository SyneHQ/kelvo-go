# Rolling upgrades and rollback

Compatibility is declared for exact artifacts, job metadata, principal policies,
broker versions and configuration. A passing two-version test does not authorize
mixing arbitrary releases. Keep a tested rollback binary and the **current**
security configuration; restoring an old binary must not restore revoked keys
or an older principal/source policy.

The acceptance runner tests this initial matrix:

| Component | Before | After | Boundary |
| --- | --- | --- | --- |
| Kelvo | `c7ecbf1a8f8391b13cd1bc96a4ae847d5c8b052c` | `0d6b489fb072c823a8cd6d20e6beba98a3aa3158` | Same job and whole-source principal protocol |
| NATS server | `2.15.0` | `2.15.0` | Serial restarts of three brokers; no broker-version upgrade claim |
| NATS Go client | `v1.54.0` | `v1.54.0` | Unchanged dependency |
| Principal/source policy | Revision 2 | Revision 2 | Exact durable metadata; stale revisions fail startup |
| Gateway key document | Revision 1 | Overlap 2, then new-only 3 | Both gateway restart floors raised to 3 before rolling |

Both binaries use the pinned DuckDB/Arrow bridge and the same Landlock launcher.
The matrix excludes later row/column policies, auditing and other new options.
Those need their own compatibility acceptance before mixing versions.

For each gateway, worker and broker transition, the runner first observes two
durable running native queries and two queued CTE queries across two tenants.
A bounded loopback ClickHouse-protocol fixture holds one exact Arrow response per
tenant until drain or broker transition is observed. Original handles must
produce exact results, preserve principal ownership, and reject subsequent
result consumption. The runner rolls both gateways and workers forward, restarts
each broker serially, then rolls application roles back with current keys and
policies. It also kills a result-owning gateway and checks terminal failure,
surviving-tenant progress and no replay of that observed attempt.

The separate [NATS compatibility and configuration-refusal matrix](nats-compatibility.md)
now records actual 2.14.7/2.15.0 broker upgrades and rollback, same-source full-engine
v1.53.1/v1.54.0 client rolls, and explicit old-binary refusal of row-policy/audit
configuration. Those newer results retain their own exact artifacts and evidence;
they do not change the historical matrix above.

Run only on an idle Linux test VM with prebuilt binaries and installed PyArrow
and PyYAML. Give the run a fresh private fixture directory in an isolated network
namespace; enforce CPU, memory and runtime limits through its service manager.
Use the Python executable from the qualified environment for both controls and
the runner. `matrix.json` follows the `matrix` object in the published report;
generate its SHA-256 values from the two source archives and actual binaries:

```sh
python3 scripts/test_rolling_upgrade_acceptance.py
python3 scripts/rolling_upgrade_acceptance.py \
  --binary /private-build/old/kelvo \
  --new-binary /private-build/new/kelvo \
  --sandbox /private-build/old/kelvo-landlock \
  --old-archive /private-build/old.tar \
  --new-archive /private-build/new.tar \
  --matrix /private-build/matrix.json \
  --nats-archive /private-build/nats-server-v2.15.0-linux-amd64.tar.gz \
  --fixture /private-build/fresh-broker-fixture \
  --output /private-build/rolling-upgrade-report.json
```

The runner installs nothing. It checks artifact hashes, uses a checksum-pinned
cached NATS archive, preserves all failure reports and private diagnostics, and
stops only its owned application and broker identities. Never publish the
private fixture directory, generated keys, configuration or logs. Publish the
sanitized JSON report with the exact build manifest, including failures before
any successful rerun.

The [2026-10-03 acceptance](evidence/rolling-upgrades.json) passed all 16 gates in
88.45 seconds: 11 transition waves, eight stale-startup probes, key rotation,
ambiguous gateway loss and cleanup. Six evidence/protocol controls also passed. A [stricter evidence check](evidence/rolling-upgrades-controls.json) rejects missing transition details and non-integer counters; it revalidated this retained runtime report without rerunning the cluster.
The [initial prerequisite failure](evidence/rolling-upgrades-prerequisite-failure.json)
is retained: the default Python interpreter lacked PyArrow, so the first control
run stopped before provisioning a cluster. The rerun used an already installed
qualified Python environment; neither run installed packages.

This is single-host lifecycle evidence. It does not establish multi-host fault
tolerance, WAN throughput, arbitrary broker upgrades, source execution exactly
once, or a fleet-wide production SLO. Before a deployment, repeat the declared
matrix with its actual configuration, resource limits and rollback artifacts.

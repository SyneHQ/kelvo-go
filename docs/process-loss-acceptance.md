# Process-loss acceptance

Kelvo's process-loss campaign tests recovery after abrupt, identity-checked
termination of an owned gateway, tenant worker node and one of three NATS
brokers. Each fault starts with **two observed running queries and two observed
queued queries**, across two tenants. Submission alone does not satisfy this
precondition.

The campaign reuses the operational fixture's authenticated HTTPS/mTLS clients,
exact nullable integer dataset and strict HTTP/Arrow completion checks. Both
tenants query 100,000 deterministic Parquet rows. Long bounded analytical queries
hold each worker while queued analytical queries retain their original handles.
No production source database or unrelated host service participates.

## Gates

| Gate | Required evidence |
| --- | --- |
| Gateway loss | Kill one gateway during active delivery; observe failed/cancelled attempt, reject result replay, preserve and deliver admitted queued handles through the second gateway, start a replacement gateway |
| Worker-node loss | Kill tenant A's node during execution; observe terminal attempt without reassignment, preserve its queued handle, deliver tenant B's exact result, wait for ownership expiry and restart the same worker ID |
| Broker loss | Kill one identity-verified fixture broker during admitted work, cancel running attempts and deliver queued results while the broker remains absent; rejoin and establish three current metadata replicas |
| Cleanup | No unexpected forced application cleanup, live observed descendants or worker scratch directories; original YAML/broker configurations unchanged; all fixture brokers healthy |

Every previously running handle must remain failed/cancelled and reject result
requests with HTTP 409 after queued delivery and recovery. This checks Kelvo's
no-automatic-replay result contract. It does not prove exactly-once database
execution, observe every source-side operation, or make partially delivered data
retractable. Queries use the existing materializing DuckDB execution path.

The runner intentionally distinguishes killed processes from unexpected forced
cleanup. It records crash-time and final scratch counts. A node killed with
SIGKILL cannot run deferred cleanup; crash leftovers fail the cleanup gate. The
harness does not delete leftovers before its verdict to manufacture success.
`--managed-scratch` enables [private ownership-based recovery](worker-scratch.md)
for both test nodes and verifies the actual sandbox process inherited a sibling
lease descriptor before fault injection. Recovery must remove crash leftovers
before the final verdict. Omitting the flag exercises legacy system-temp behavior.

## Run on an isolated Linux test host

Use prebuilt binaries and installed PyArrow. The user must own the dedicated
fixture and its broker lifecycle. The standard fixture uses loopback ports
14222–14224, 14440–14445, 16222–16224 and 18222–18224; do not run another fixture
on those ports concurrently. Its provision command downloads a checksum-pinned
NATS executable on the test host. Never run provisioning or builds locally when
[contributor instructions](../AGENTS.md) prohibit them.

```sh
python3 -m unittest discover -s scripts -p test_process_loss_acceptance.py
python3 scripts/process_loss_acceptance.py \
  --binary bin/kelvo \
  --sandbox bin/kelvo-landlock \
  --fixture artifacts/process-loss-private \
  --managed-scratch \
  --output artifacts/process-loss-report.json
```

Provision `artifacts/process-loss-private` with `cluster_fixture.py`'s `DIR`
overridden to that fresh directory before running. Do not attach to a prior
manifest or fixtures owned by another test. Stop all identity-verified fixture
brokers through `cluster_fixture.py` after acceptance, including on failure. The
runner restarts the broker it deliberately kills and updates only its owned PID
manifest; broker teardown belongs to the outer fixture owner.

Public reports contain gate status, timing, safe error categories and binary /
script hashes. Private configurations, credentials, query handles, paths and raw
logs stay in ignored artifacts. A report fails if any required gate is missing,
failed or skipped, if interrupted, or if cleanup/quorum restoration is incomplete.

## Evidence boundaries

This is a bounded, single-host process fault campaign. It does not establish
multi-zone availability, network partition tolerance, storage/power-loss
correctness, provider cancellation, snapshot-publication fault recovery or a
multi-hour capacity envelope. Those remain separate release gates in the
[production checklist](production-status.md). A passing process campaign must
not be represented as completion of the production roadmap.

Live results belong to their exact executable and source hashes. Retain failed
reports beside successful reruns and document what changed between them.

## Recorded validation

On 3 October 2026 the [managed-scratch campaign](evidence/process-loss-acceptance.json)
passed all five required gates. It ran the `duckdb_arrow` candidate built from
runtime revision `7ac9bc3052de6a8fa1f71a221c24c2f0fbc478e5`, SHA-256
`45f9bb28516f941ae168828131c2234d7c19126471bd3fb66e5ed63c8a19d3cd`.
All 332 tracked Go/native inputs were compared with the build host. The affected
worker, cluster and CLI race suites and 12 acceptance-control tests passed.

The actual sandboxed query worker held an inherited ownership descriptor before
its node was killed. One workspace remained immediately after the crash; startup
recovery reclaimed it after the child exited. Final cleanup found no observed
live descendants or worker scratch directories, required no unexpected forced
application kills, verified all 12 original configurations, and observed all
three brokers healthy. The outer fixture owner subsequently stopped only those
owned brokers and verified none remained.

| Whole gate | Observed duration |
| --- | ---: |
| Gateway termination and recovery | 1.226 s |
| Worker-node termination and recovery | 4.467 s |
| Broker termination, surviving quorum work and rejoin | 3.212 s |

These are single-trial gate durations, including fixture assertions. They are
not availability targets, request-latency percentiles or recovery SLAs.

Failure-inclusive reports remain available:

- [Initial harness failure](evidence/process-loss-initial-harness-failure.json):
  legacy worker scratch remained and the broker-restoration check failed. The
  runner also contained a bookkeeping race because a replacement broker briefly
  entered application-sampler ownership; this was removed before the next trial.
- [Confirmed legacy scratch failure](evidence/process-loss-legacy-scratch-failure.json):
  after correcting broker bookkeeping, every execution/recovery gate passed but
  one crash workspace remained. This is the runtime gap addressed by opting into
  managed scratch; legacy temp behavior remains unchanged.
- [Cancellation observation failure](evidence/process-loss-cancellation-observation-failure.json):
  managed scratch cleanup passed, but the broker-loss gate required the first
  cancellation response to be HTTP 200. The harness did not retain that response's
  exact status. It now records bounded retries for idempotent cancellation after
  HTTP 409/429/503 or transport failure, plus bounded status observations. SQL
  submission and result claims are never retried by this recovery policy. The
  passing trial needed no cancellation retry; negative controls separately verify
  the transient-error and deadline paths.

The [hosted CI workflow](../.github/workflows/ci.yml) runs the same campaign after
its existing key-rotation fixture. It reuses that job's exclusively owned brokers,
updates only the restarted broker's PID in the owned manifest, and retains the
outer always-run identity-checked broker teardown. A local pass does not establish
that a subsequent hosted CI run has passed.

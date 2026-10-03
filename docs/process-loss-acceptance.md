# Process-loss acceptance

This campaign kills owned gateways, worker nodes and one of three NATS brokers on an isolated Linux host. Each fault requires **two observed running and two observed queued queries** across two tenants, with 100,000 deterministic Parquet rows per tenant.

## Gates

| Gate | Required evidence |
| --- | --- |
| Gateway loss | Active attempt fails/cancels and rejects replay; second gateway serves original queued handles; replacement starts |
| Worker loss | Tenant A attempt terminates without reassignment; queued handle survives; tenant B succeeds; same worker ID restarts after ownership expiry |
| Broker loss | Queued results succeed while one broker is absent; rejoin restores three current metadata replicas |
| Cleanup | No unexpected forced cleanup, observed live descendants or scratch; original configurations unchanged; brokers healthy |

Previously running handles must remain failed/cancelled and reject results with HTTP 409 after recovery. This checks no automatic result replay, not exactly-once database execution or remote cancellation. DuckDB still materializes execution before Arrow delivery.

Use `--managed-scratch` for [ownership-based recovery](worker-scratch.md). The runner verifies the sandbox inherited a lease before injecting the fault. SIGKILL leftovers must be reclaimed before verdict; the harness does not delete them to manufacture a pass. Omitting the flag tests legacy temp behavior.

## Run on an isolated Linux test host

1. Prepare binaries/PyArrow and provision a fresh owned fixture, overriding `cluster_fixture.py`'s `DIR` to `artifacts/process-loss-private`. Do not reuse another run's manifest.
2. Run the control tests and campaign:

```sh
python3 -m unittest discover -s scripts -p test_process_loss_acceptance.py
python3 scripts/process_loss_acceptance.py \
  --binary bin/kelvo --sandbox bin/kelvo-landlock \
  --fixture artifacts/process-loss-private --managed-scratch \
  --output artifacts/process-loss-report.json
```

3. Stop all identity-verified owned brokers through `cluster_fixture.py`, including after failure. The runner rejoins its killed broker and updates only its owned PID manifest; final teardown belongs to the fixture owner.

Required loopback ports: 14222–14224, 14440–14445, 16222–16224 and 18222–18224. Provisioning downloads pinned NATS on the test host. Follow [contributor build restrictions](../AGENTS.md).

Public reports contain hashes, gate status, timing and safe categories. Credentials, handles, paths and raw logs stay private. Missing/skipped gates, interruption, incomplete cleanup or quorum restoration fail the report.

## Evidence boundaries

This single-host campaign does not validate multi-zone availability, network partitions, storage/power loss, provider cancellation, snapshot-publication faults or multi-hour capacity. See [production status](production-status.md). Evidence belongs to exact executable/source hashes; retain failures beside reruns.

## Recorded validation

The [3 October managed-scratch run](evidence/process-loss-acceptance.json) passed all five required gates at runtime `7ac9bc3`, with 332 tracked native/Go inputs matched to the VM. Affected race suites and 12 control tests passed; the report records the exact binary hash.

One crash workspace remained after node SIGKILL and was reclaimed after its child exited. Final cleanup found no live observed descendants or scratch, required no unexpected forced kills, verified 12 original configurations and restored three healthy brokers. The outer owner then stopped those brokers.

| Whole gate | Single-trial duration |
| --- | ---: |
| Gateway termination/recovery | 1.226s |
| Worker termination/recovery | 4.467s |
| Broker loss, quorum work and rejoin | 3.212s |

These include fixture assertions; they are not recovery SLAs or latency percentiles.

| Retained failure | Finding and correction |
| --- | --- |
| [Initial harness](evidence/process-loss-initial-harness-failure.json) | Legacy scratch remained; broker sampler bookkeeping race was fixed |
| [Legacy scratch](evidence/process-loss-legacy-scratch-failure.json) | Execution/recovery passed but one workspace remained; managed scratch addresses this gap |
| [Cancellation observation](evidence/process-loss-cancellation-observation-failure.json) | Harness required the first response to be 200 without retaining its status; bounded cancellation/status retries now handle 409/429/503 or transport failure |

Cancellation retries never resubmit SQL or repeat result claims. The passing run needed no retry; controls test transient failures/deadlines.

The [CI workflow](../.github/workflows/ci.yml) wires this campaign after key rotation, reusing owned brokers and always-run identity-checked teardown. Local acceptance does not establish a later hosted CI pass.

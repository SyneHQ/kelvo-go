# Worker lease loss and recovery

A worker that loses its coordination lease stops admitting work, cancels execution and performs bounded child cleanup. `kelvo node` exits with status 1 and `Worker coordination lease lost`, including during graceful drain.

Before restarting, the supervisor must confirm that the old children are gone. The replacement keeps the worker ID and configuration but uses a new owner token. Durable compare-and-swap and expiry checks decide whether it can acquire the identity. Never replay a failed query just because its worker restarted.

## Verified recovery

The controlled broker-loss gate preserves two running and two queued query handles. Failed attempts reject result requests; queued and post-recovery queries return exact Arrow values. No SQL is resubmitted.

| Frozen candidate | Result | Evidence |
| --- | --- | --- |
| `eec91a1`, fixture `fcd4d4d` | Component race/build/vet controls and broker recovery passed | [Receipt](evidence/node-lease-recovery-eec91a1.json) |
| `b09f2da` | Combined package checks and broker recovery passed | [Package](evidence/node-lease-integration-b09f2da.json) · [Recovery](evidence/node-lease-recovery-b09f2da.json) |
| `b09f2da`, 600-second smoke | All ten gates passed | [Smoke](evidence/lease-recovery-smoke-b09f2da.json) |
| `b09f2da`, 7,200-second load | **Strict acceptance failed**; all ten operational gates passed | [Failed trial](evidence/lease-recovery-sustained-failed-b09f2da.json) |

For the combined broker gate, the old worker exited 4.960 seconds after the fault. Its conservative expiry bound was 9.960 seconds; verified cleanup allowed restart at 9.981 seconds, and the replacement was ready at 10.201 seconds. These are observations for one broker placement.

The expiry bound is the observed parent exit plus the configured five-second lease, not a persisted-heartbeat reading. The fixture keeps its ten-second cancellation, 25-second queued-status and separate 25-second supervisor deadlines. Restart requires the exact old process identity, empty child/cgroup membership and unchanged configuration.

## Two-hour run: acceptance failed

The paced workload completed **19,491 queries, 240 slow-reader checks, 239 cancellations and 959 refreshes** over 7,200.001 seconds. Each of two tenants had one million synthetic Parquet rows.

Strict reconciliation rejected `PROCESS_IDENTITY_READ_FAILED` and `PROCESS_READ_FAILED`. These are two categories, not an event count. The old sampler discarded errno, operation and timing details, so their cause remains unknown. A later sampler fix cannot turn this receipt into a pass.

Workload failures, resource-counter failures and OOMs were zero. Three cancellation-control transport errors recovered within the existing gate. Final cleanup left no descendants, scratch or containment records; source identity stayed unchanged.

The service cgroup peaked at **862,814,208 bytes (about 823 MiB)**, covering two gateways, two workers, three brokers and the inner fixture. The outer coordinator and SSH were excluded. Process-RSS evidence failed acceptance and must not be presented as a certified footprint.

## Process-sampling correction

The [focused receipt](evidence/process-sampling-exit-correction-0e80e9e.json) records 38 passing Linux controls, with no failures or skips. A real child-process test proves that reading already-open `/proc` descriptors after exit can return `ESRCH`; it does not establish the historical failure's cause.

- Only `ENOENT` and `ESRCH` count as disappearance; permission, I/O and malformed-data errors still fail.
- PIDs and start ticks must be bounded ASCII integers. Explicit zombie state counts as exit.
- Diagnostics retain operation, outcome, errno, count and elapsed times in at most 32 buckets plus overflow. They omit PIDs, paths, payloads and exception text; overflow preserves error flags.

The service watchdog bounds execution even if the outer coordinator disappears: workload duration plus 300 seconds, giving 900 seconds for smoke and 7,500 seconds for sustained load. A matching smoke and a new sustained run on one frozen source/binary are still required.

## Scope

The lifecycle fixture runs under two CPUs, 6 GiB, no swap and a private network. Older `b09f2da` results predate object-policy, retained-export and TLS-delivery changes. These receipts do not establish maximum throughput, minimum RAM, WAN performance, live-provider behavior or multi-host availability.

[Production checklist](production-status.md) · [Capacity profile](node-capacity.md) · [Process-loss gates](process-loss-acceptance.md)

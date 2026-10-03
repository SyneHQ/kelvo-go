# Runtime recovery fixes

These fixes address failures found in the sustained and Oracle WAN campaigns.
They preserve fail-closed cleanup and the existing query deadlines.

| Failure | Change |
| --- | --- |
| Heartbeat CAS repeatedly collides with result publication | Poll cancellation as before; renew after one-third of the lease. Serialize renewal and result publication per reservation. |
| Late renewal error overwrites a completed result | Stop the watcher when its reservation has already been released. |
| WAN startup spends its budget on repeated metadata reads | Reuse each fresh lookup response and run four independent resource checks concurrently. Keep the five-second deadline and every policy check. |
| Transient inherited scratch lease prevents cleanup | Wait at most one second for lock contention, then revalidate the inode. Delete only after acquiring exclusive ownership. |

Cancellation and failure can still fence publication. Persistent scratch holders,
changed ownership and other cleanup errors still quarantine the worker. Result
publication retains its three-second deadline and final Arrow EOS checks.

## Validation

[The receipt](evidence/runtime-recovery-validation.json) pins source `2103318`
and both binaries. Nine package race checks, vet, 16 sustained-run controls and
repeated startup regressions passed on Azure. The [handoff regression](evidence/node-handoff-regression.json) fails before the fix and passes afterward; [scratch regressions](evidence/scratch-lease-regression.json) retain their separate source identity. A separate isolated three-broker
NATS race run passed all nine top-level tests without skips.

The earlier failed sustained trials and WAN preflights remain recorded. The
[600-second smoke and two-hour campaign](sustained-acceptance.md) and refreshed
[Oracle capacity profile](oracle-micro-capacity.md) remain separate required gates.
These package results establish no new throughput or production-readiness claim.

## Run the focused checks

Use the designated Linux build environment and its pinned dependencies:

```sh
go test -race ./internal/cluster -run 'TestStartup|TestNode'
go test -race ./internal/worker ./internal/containment
python3 -m unittest discover -s scripts -p test_sustained_acceptance.py
```

Broker, containment and provider tests need their documented fixtures. Preserve
failed reports and skipped test names; do not count a skipped gate as passing.

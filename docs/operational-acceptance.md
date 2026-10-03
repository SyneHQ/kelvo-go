# Repeatable operational acceptance

Run this gate on an isolated Linux host with built Kelvo/Landlock binaries, Go, Python and PyArrow. It uses an idle, loopback-only three-broker fixture and installs nothing.

1. Provision the owned fixture; application ports must be free.
2. Run with a new report path. `--duration` selects 30–14,400 seconds of mixed load; startup, faults and cleanup add time.
3. Always stop the owned brokers after the campaign, including on failure.

```sh
python3 scripts/cluster_fixture.py provision
python3 scripts/operational_acceptance.py \
  --binary bin/kelvo --sandbox bin/kelvo-landlock \
  --duration 30 --output artifacts/operational-30s.json
python3 scripts/cluster_fixture.py stop
```

Use `--duration 7200` for two hours and `--go` for an existing toolchain. The runner copies YAML into a private directory; original fixture configuration and broker state remain intact.

| Gate | Required behavior |
| --- | --- |
| Mixed queries | Four clients, two tenant workers, 100,000 Parquet rows/tenant; exact nullable CTE values/types and complete HTTP/Arrow/EOS with recognized gateway capability |
| Refresh progress | Five-second schedules advance while query/refresh reservations overlap |
| Resource accounting | Counts, memory and scratch reservations stay within budget; oversized configured query rejected at startup |
| Saturation/recovery | Eight tenant handles, two parked waiters and two active requests saturate their respective limits; status/probes remain accessible; cancellation restores capacity |
| Drain | SIGTERM changes readiness, completes accepted query correctly and exits within grace |
| Faults | Four actual sandboxed resource/source-lease cases execute without skips, retain snapshots and recover after reset |
| Cleanup | Every named gate passes with owned-process cleanup |

Source-lease failure is injected in an in-memory fixture. A reclaimed status 404 passes only after certified delivery, never by itself. Failure/interruption leaves dependent gates unpassed. Reports contain hashes, counts and safe categories; private logs stay in ignored artifacts.

The 50ms sampler records owned application/fault-test processes and observed descendants, excluding brokers/Python clients. It can miss brief peaks. Aggregate RSS can double-count mappings; optional cgroup counters may include unrelated workloads. Neither is reservation enforcement or a deployment sizing recommendation. Keep actual container/disk limits.

## Recorded validation

The [3 October five-minute run](evidence/operational-acceptance.json) passed seven gates at runtime [`eacb7e2`](https://github.com/SyneHQ/kelvo-go/tree/eacb7e2aedd8020575ecfede53db819112c40199); 319 compilation inputs matched the VM. Binary, launcher and runner hashes are recorded.

| Observation | Result |
| --- | ---: |
| Mixed interval / exact completed CTEs | 300.193s / 7,288 |
| Scheduled refreshes | 30 per worker |
| Query/refresh overlap samples / maximum node reservations | 117 / 2 |
| Sampled aggregate process RSS peak | 779,673,600 bytes |
| Observed cgroup `memory.current` peak | 387,211,264 bytes |
| OOM / OOM-kill event increases | 0 / 0 |
| Forced kills / surviving descendants / scratch leftovers | 0 / 0 / 0 |

Memory observations cover the entire campaign, including faults. `PROCESS_SAMPLE_UNAVAILABLE` is retained: transient samples and short peaks can be missed.

The [initial attempt](evidence/operational-initial-failure.json) failed saturation with `CONDITION_DEADLINE`; drain did not run. The runner was corrected to retry transient holder 429s, select observed queued/assigned states and require the specific waiter-pool rejection. The full rerun used the same runtime binary; the failed record does not prove its exact scheduling interleaving.

The later key-rotation binary passed a separate [30-second gate](evidence/operational-key-rotation.json): seven checks, 693 exact queries in 30.152s and continued refreshes. Its [file-key gate](gateway-key-rotation.md#recorded-acceptance) covers opt-in authentication. Measurements do not transfer between binaries.

These synthetic local runs establish lifecycle/result correctness for their recorded artifacts. Pair them with [capacity evidence](federation-capacity.md), [storage gates](storage-conformance.md) and the [roadmap](production-roadmap.md); they do not certify WAN, managed providers or arbitrary production capacity.

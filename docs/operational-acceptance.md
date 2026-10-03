# Repeatable operational acceptance

Run the operational gate on an isolated Linux testing host with built Kelvo and
Landlock binaries, Go, Python and PyArrow already installed. It uses the idle,
loopback-only three-broker fixture from `scripts/cluster_fixture.py`. No package
installation, production connections or broker shutdown occurs in the runner.
Application ports must be idle; occupied ports fail before any process is touched.

```sh
python3 scripts/cluster_fixture.py provision
python3 scripts/operational_acceptance.py \
  --binary bin/kelvo --sandbox bin/kelvo-landlock \
  --duration 30 --output artifacts/operational-30s.json
python3 scripts/cluster_fixture.py stop
```

Use a unique output path: existing reports are never overwritten. `--duration`
sets the mixed workload interval, between 30 and 14,400 seconds. Startup, fault
checks and cleanup add time beyond that interval. A two-hour run uses
`--duration 7200`. `--go` selects the VM's existing Go toolchain. The runner copies
node/gateway YAML into a fresh private directory and leaves original fixture
configuration and broker state intact. Always stop the broker fixture after the
campaign, including failed runs.

The gate verifies these behaviors using two tenant-bound workers, four concurrent
query clients and the existing mTLS/JetStream path:

- Full typed CTE results over 100,000 deterministic Parquet rows per tenant,
  including NULL-aware sums, counts and tenant labels. Every result must advertise
  the recognized gateway completion capability and pass complete HTTP, Arrow and
  final EOS validation. Status handles reclaimed after certified delivery are
  counted separately; a 404 alone never passes a query.
- Scheduled five-second snapshot refresh progress during query load, with an
  observed interval of query and refresh reservations overlapping on a node.
- Configured reservation counts, memory and scratch accounting stay within their
  budgets. An oversized query reservation is rejected during node startup.
- Filling eight tenant handles rejects another submission. Two parked result
  waiters reject an additional waiter while status remains accessible. Two actual
  running analytical requests separately fill active HTTP permits; `/health` and
  `/ready` remain accessible. Cancelling handles releases capacity and subsequent
  queries return correct results for both tenants.
- A node changes readiness during SIGTERM drain, continues serving an accepted
  query, returns exact Arrow data and exits before its grace deadline.
- The existing four sandboxed resource/source-lease fault cases run against the
  same binary and launcher. They must actually execute, with no skipped tests.
  These cover prior snapshot retention, permit and scratch cleanup, reset and
  exact recovered values for single-file and multipart layouts. Source-lease
  failure uses an in-memory coordination injection, not live broker lease loss.

`passed: true` requires every named gate and cleanup check to pass. A failed or
interrupted stage records its category; dependent stages remain explicitly
unpassed. Logs stay under ignored `artifacts/operational-private`. Evidence records
binary, launcher and script hashes, observed work, refresh counts and cleanup.
It contains no credentials, SQL, private paths or raw error text.

After startup, the sampler records the aggregate RSS of owned application
processes, the separate fault-test process and observed descendants every 50
milliseconds. It excludes brokers and the Python clients,
and can miss short-lived peaks. If available, cgroup v2 `memory.current` and
`memory.events` are recorded separately. Cgroups can include unrelated workloads;
these counters are not Kelvo-only RSS or evidence that reservation accounting
enforces memory limits. Missing counters are listed explicitly. Keep actual
container memory and storage limits when deploying.

## Recorded validation

On **3 October 2026**, the [five-minute operational run](evidence/operational-acceptance.json)
passed all seven named gates, including saturation/recovery, accepted-query drain,
the four sandboxed fault cases and cleanup. Its runtime source was
[`eacb7e2`](https://github.com/SyneHQ/kelvo-go/tree/eacb7e2aedd8020575ecfede53db819112c40199),
with all 319 tracked compilation inputs verified against the VM build. The report
records the exact binary, launcher and fixture script hashes.

The fixture used two tenant-bound workers, four concurrent clients and 100,000
synthetic Parquet rows per tenant. Every completed CTE result was checked for
exact groups, values, NULL handling, tenant labels and Arrow types.

| Observation | Recorded result |
| --- | --- |
| Mixed workload interval | 300.193 seconds |
| Correct completed CTE queries | 7,288 |
| Successful scheduled refreshes | 30 per worker |
| Observed query/refresh reservation overlap | 117 samples |
| Maximum observed reservations on a node | 2 |
| Sampled aggregate process-tree RSS peak | 779,673,600 bytes |
| Observed cgroup `memory.current` peak | 387,211,264 bytes |
| Observed cgroup OOM / OOM-kill event increases | 0 / 0 |
| Forced application kills / surviving descendants / worker scratch directories | 0 / 0 / 0 |

RSS and cgroup values cover the complete campaign, including the fault checks,
not just the mixed query interval. Aggregate RSS can count shared mappings in
multiple processes; the observed cgroup can contain unrelated workloads. Neither
number is a minimum deployment size or a per-query memory bound. The sampler also
reported `PROCESS_SAMPLE_UNAVAILABLE`; transient process observations and brief
peaks can be missed. The report preserves this limitation.

The [initial five-minute attempt](evidence/operational-initial-failure.json) remains
**failed**: its mixed workload passed, but the saturation stage reached
`CONDITION_DEADLINE`, leaving drain unrun. Investigation reproduced a scheduling
deficiency in the test: a holder could stop retrying after a transient active
HTTP admission `429`. The test also assumed assignment followed submission order
and accepted any `429` as waiter-pool saturation. The failed report does not prove
the exact thread interleaving that occurred in that attempt.

The corrected runner selects handles from observed queued/assigned states,
retries transient holder admission failures, checks both holders are still
pending and requires the specific parked-waiter-pool rejection. The complete
rerun passed using the same runtime binary. These measurements apply to the
recorded runtime and runner; later builds require their own validation.

This fixture proves local lifecycle and result correctness for the tested run.
Its dataset is synthetic; it does not establish WAN throughput, managed-provider
behavior, a supported tenant count or production readiness for arbitrary queries.
Pair it with the [capacity evidence](federation-capacity.md),
[storage conformance gate](storage-conformance.md) and
[production roadmap](production-roadmap.md).

The later live-key-rotation binary separately passed the same [30-second
operational gate](evidence/operational-key-rotation.json) using legacy environment
tokens: all seven checks, 693 exact queries in 30.152 seconds, and
continued scheduled refreshes. Its [two-gateway file-key gate](gateway-key-rotation.md#recorded-acceptance)
checks the opt-in authentication path. Each report identifies its own binary and
script hashes; the five-minute measurements above do not transfer to a later build.

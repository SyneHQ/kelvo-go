# Worker lease loss and recovery

A worker that loses its coordination lease fences itself immediately. It stops admitting query and refresh work, cancels refresh execution, and runs bounded child cleanup before exiting. The `kelvo node` command then exits with status 1 and the fixed diagnostic `Worker coordination lease lost`, allowing restart-on-failure supervision to replace the process. Cleanup can time out; supervisors must verify that old descendants are gone before replacement. Normal shutdown remains distinct from lease failure. Lease loss also interrupts an ordinary graceful drain.

The replacement uses the same node configuration and worker ID with a new owner token. The durable store still decides whether that owner may acquire the worker identity. A fenced process does not resume work or renew its old ownership; clients must not infer that a failed source query is safe to replay.

## Verified recovery

The [sanitized evidence](evidence/node-lease-recovery-eec91a1.json) records focused race tests, vet, an offline Linux build, 47 Python negative controls, and a contained three-broker failure gate. Runtime source is pinned to `eec91a1`; the acceptance fixture is pinned to `fcd4d4d`. These are component validation receipts, separate from later combined release candidates.

The gate admitted two running and two queued queries, killed one broker, and observed a real lease-loss exit from worker `b1`. It retained every original query handle. Failed attempts rejected result requests; queued queries delivered the expected Arrow values; post-rejoin queries passed.

| Observation | Seconds after broker fault |
| --- | ---: |
| Exact old worker process exit observed | 4.909407 |
| Conservative lease-expiry upper bound | 9.909407 |
| Old children confirmed absent; replacement started | 9.929815 |
| Replacement ready | 10.173460 |

The expiry bound is the observed parent exit time plus the configured five-second lease. It is not a reading of persisted heartbeat time. Store compare-and-swap and expiry checks remain authoritative, including when clocks differ.

The fixture preserves the existing ten-second cancellation and 25-second queued-status deadlines. Its supervisor has a separate 25-second deadline from the fault. It checks the exact old process identity, new log bytes from each process, unchanged configuration and catalog bytes, empty native-child and delegated-cgroup membership before restart, and the final replacement identity. It never resubmits SQL or creates replacement query handles. Bounded startup retries accept only recognized lease/store-unavailable failures.

The run finished in 21.884 seconds under a two-CPU, 6 GiB, no-swap service limit with a private network namespace and no capabilities. Cleanup reported zero forced application kills, live descendants, scratch directories, and containment records. All three validation service cgroups were absent after completion.

This single broker placement demonstrates the exit-and-replacement path. It does not certify multi-hour throughput or recovery deadlines for every leader placement, network partition, or load level. Earlier failed campaigns remain separate evidence; this result does not rewrite them as passes.

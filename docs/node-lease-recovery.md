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

## Combined candidate check

The [combined package checks](evidence/node-lease-integration-b09f2da.json) pin `b09f2da`: 429 full-package race pass events, ten focused recovery passes, six real snapshot/principal passes, 60 harness controls and vet. Twelve ordinary external/optional skips remain listed.

The [combined candidate receipt](evidence/node-lease-recovery-b09f2da.json) repeats the contained broker gate on `b09f2daad7caccf5caa7e35df5805696c9bc13ba`, with its exact build manifest and binary hashes. Worker `b1` exited at 4.960175 seconds after the fault, reached its conservative expiry bound at 9.960175 seconds, and restarted after its old children were confirmed absent at 9.981010 seconds. The replacement was ready at 10.201490 seconds. Original queued handles and Arrow values passed, failed attempts remained rejected, and cleanup left no descendants or containment records. The run took 22.042 seconds. Matching smoke and sustained campaigns are separate evidence.

The [matching 600-second smoke receipt](evidence/lease-recovery-smoke-b09f2da.json) passed all ten gates on the same source and binaries, including saturation, gateway loss, worker loss, broker loss, and final cleanup. Its two tenants each had one million synthetic Parquet source rows. The deliberately paced workload completed 1,577 queries, 20 slow-reader checks, 20 cancellations, and 79 refreshes over 600.150 seconds of mixed load. Both source identity and binary identity remained matched; the owned service and cgroup were removed afterward.

The complete smoke took 614.028 seconds, with zero workload client/resource errors and zero OOM events. Fault cancellation-control calls recorded three transport errors before successful recovery within the existing gate. The whole-service cgroup memory peak was 761,511,936 bytes (about 726 MiB), including the fixture's two gateways, two workers, three brokers, and inner Python fixture process. The outer launch coordinator and SSH are excluded. Its service limits remained two CPUs and 6 GiB. These are lifecycle measurements from a paced workload, not maximum-throughput or minimum-machine sizing claims. A ten-minute smoke is separate from the required two-hour sustained gate.

## Two-hour run: acceptance failed

The [retained failed sustained receipt](evidence/lease-recovery-sustained-failed-b09f2da.json) records the two-hour run on the same frozen `b09f2da` source and binaries. All ten operational gates passed, including gateway loss, worker loss, broker loss, and final cleanup. The paced mixed workload ran for 7,200.001 seconds and completed 19,491 queries, 240 slow-reader checks, 239 cancellations, and 959 refreshes against one million synthetic Parquet rows per tenant. The complete campaign took 7,214.300 seconds.

Final reconciliation failed because process-RSS sampling recorded `PROCESS_IDENTITY_READ_FAILED` and `PROCESS_READ_FAILED`. These are two distinct categories, not an event count. The sampler retained neither errno, failing operation, per-category occurrence count, nor event timing, so the historical cause cannot be reconstructed. Normal process-exit races are a hypothesis; they do not turn this report into a pass. A corrected observer needs focused validation, a matching smoke, and a new sustained campaign.

Workload client failures, resource-counter errors, resource-limit violations, and OOM events were zero. Three fault cancellation-control transport errors preceded successful recovery. The complete service cgroup peak was 862,814,208 bytes (about 823 MiB), including two gateways, two workers, three brokers, and the inner Python fixture; it excludes the outer coordinator and SSH. Process-RSS sampling is incomplete evidence despite its recorded 140,233 samples and must not be used as a certified footprint.

Final cleanup left zero live descendants, worker scratch directories, and containment records, with zero forced application kills. The owned service and cgroup were removed, source identity remained unchanged, and the build/source identities matched. The failed raw report remains identified by SHA-256 `c2a7ddf4ef219a471c369a85a122d8c2e5623850bfb84ec2a3e9d720bdd76158`.

This run predates later object-snapshot, retained-export, and TLS-delivery changes. It provides operational observations for the frozen revision, not a passed sustained gate, a maximum-throughput result, or WAN, provider, multi-host, or sizing certification.

## Process-sampling correction

The [focused correction receipt](evidence/process-sampling-exit-correction-0e80e9e.json) records 38 passing Linux sampler/lease controls with no failures, errors, or skips. A deterministic test opens its own live child's `/proc` stat and status files, terminates and reaps that child, then reads the already-open descriptors. Linux returns `ESRCH`; the old sampler treated this known exit race as an observation failure. This proves a sampler defect, without establishing the cause of either category retained by the historical campaign.

The corrected observer classifies only `ENOENT` and `ESRCH` as disappearance across stat, status, task enumeration, and child-list reads. Permission errors, other I/O errors, and malformed data remain hard failures. Child PIDs and process start ticks are bounded ASCII integers; malformed Unicode or oversized values cannot terminate the sampling loop. Explicit zombie state is recorded as exit. Diagnostics retain operation, outcome, errno, occurrence count, and first/last elapsed time in at most 32 buckets plus an overflow count. They contain no PID, path, command line, payload, or exception text; overflow never removes hard-error flags.

The focused service used one CPU and 1 GiB, with no swap or capabilities, and exited successfully with an empty cgroup. No replacement campaign was launched as part of this correction. The original two-hour report remains failed; a matching smoke and a new sustained run on the frozen integrated revision are still required.

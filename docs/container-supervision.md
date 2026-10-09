# Container supervision

Kelvo has an internal Linux PID-1 supervisor and a container backend for qualification.
Worker configuration does not activate this backend.

## Ownership

1. Start as nonroot PID 1, with no other processes and finite, read-only cgroup controls.
2. Give the supervisor exclusive process-launch and `wait4` ownership for that worker boot.
3. Pass explicit child files. Their offsets and status flags are shared; prepare blocking pipe endpoints before launch.
4. Keep output and operation capacity until both native termination and the caller's cleanup contract finish.
5. On shutdown, stop admission, kill remaining namespace residents, and join registered and adopted children.

`Wait` cancellation does not kill a child. Use its pinned `pidfd` to send TERM or KILL.
A traced stop does not complete its exit future. `Close` respects its caller's deadline;
an uncertain return keeps admission closed while the shutdown owner continues.

The supervisor never resets within a boot. An ownership failure or incomplete cleanup
requires worker replacement. Root and runtime administrators remain trusted: they can
inject tasks beyond `pids.max`, so the limit is not an administrative security boundary.

## Before enabling a container executor

- Queries and adapter operations already share explicit command I/O ownership.
  Refreshes and exports use the same executor and retain their outer publication holds.
- Keep the existing shared admission pool. A container backend must permit only
  one native operation at a time and prove the namespace empty before reuse.
- Route physical launch, cancellation and exit status through the PID-1 supervisor;
  preserve explicit descriptor slots, including optional adapter descriptors.
- Qualify PID storms, native and whole-worker OOM, supervisor death and worker replacement.
- Preserve old worker custody after restart; do not replay work whose outcome is unknown.

Command I/O owns a private input copy, concrete inherited files and explicit pumps.
It clears input after delivery, bounds query diagnostics to 64 KiB, and keeps reservation
holds until pumps, cancellation callbacks and the result consumer return. A cleanup deadline drains admission;
it cannot turn a blocked writer into free capacity or a cleanup acknowledgement.

[Command I/O evidence](evidence/command-io.json) covers race tests and actual
delegated query/operation execution at 1 CPU and 1 GiB. It does not qualify
container execution or whole-worker crash recovery.

Whole-worker death has no in-process cleanup callback. The external worker owner must
fence the old incarnation and resolve its durable claims before replacement work.

## Fixed container budget

- Use the existing shared pool with one slot. Each query, refresh, export and adapter
  operation reserves the full container memory cap before resolving credentials.
- Keep the native engine budget smaller, with explicit space for PID 1 and IPC. The
  read-only cgroup caps the whole container; it does not enforce a separate child cap.
- Keep the same reservation identity through output, publication and cleanup. Partial,
  foreign, copied or released admission handles cannot authorize native execution.
- A failure after `ForkExec` can already have source effects. Mutations remain unknown
  even if later cleanup reaps the child; never replay from a failed launch alone.

The namespace backend reports cumulative container-cgroup usage. Its memory peak
includes the supervisor and cache, and can predate a query or worker boot.
It is not a per-query peak.

The native test lives in `internal/containment/supervisor_live_linux_test.go`. It needs a
dedicated nonroot PID-1 fixture, the compiled C probe and finite container limits.
Running an ordinary host test suite skips this gate and does not qualify an executor.
`TestNamespaceWorkerLive` covers physical query/adapter execution and inherited
publication custody. `TestNamespaceSupervisorUncertainLaunchLive` requires a separate
boot and forces a failure after actual native execution. Controllers must reject skips.

[Qualification evidence](evidence/container-supervisor.json) covers the race-enabled
native test at 1 CPU, 256 MiB and 64 kernel tasks, with independent cleanup checks.

# Container supervision

Kelvo has an internal Linux PID-1 supervisor. It is a prerequisite for a future
container executor; worker configuration does not activate it.

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

- Share one admission limit across queries, operations, refreshes and exports.
- Retain source cancellation, output cleanup and result finalization under the same custody owner.
- Qualify PID storms, native and whole-worker OOM, supervisor death and worker replacement.
- Preserve old worker custody after restart; do not replay work whose outcome is unknown.

The native test lives in `internal/containment/supervisor_live_linux_test.go`. It needs a
dedicated nonroot PID-1 fixture, the compiled C probe and finite container limits.
Running an ordinary host test suite skips this gate and does not qualify an executor.

[Qualification evidence](evidence/container-supervisor.json) covers the race-enabled
native test at 1 CPU, 256 MiB and 64 kernel tasks, with independent cleanup checks.

# Process containment

Optional Linux cgroup v2 limits apply before each native worker starts. Missing delegation, controllers, permissions, or `clone3` support reject node startup; execution never falls back to an unlimited child.

For a node whose query/refresh `memory_mb` is 128, use this configuration fragment:

```yaml
resources:
  max_concurrent: 4
  memory_mb: 2048
  baseline_mb: 256
  overhead_mb: 224
  scratch_mb: 4096
scratch_directory: /var/lib/kelvo/scratch
containment:
  root: /sys/fs/cgroup/system.slice/kelvo-node.service/jobs
  state_directory: /var/lib/kelvo/containment
  native_overhead_mb: 64
  parent_overhead_mb: 32
  max_processes: 64
  max_groups: 64
  cleanup_timeout: 5s
```

Use Linux with Landlock ABI 3+, unified cgroup v2, and a node `sandbox_path`. Create scratch/state directories as the service user with mode `0700` on trusted local storage. Configure systemd `User=kelvo` and `Delegate=yes`; set whole-service memory/CPU limits for parent overhead too.

Run this bootstrap **inside that exact service**, then exec the node. The manager must share its delegated ancestor with the empty jobs directory; running it from an SSH session fails startup placement.

```sh
cgroup_root=/sys/fs/cgroup/system.slice/kelvo-node.service
mkdir -p "$cgroup_root/supervisor" "$cgroup_root/jobs"
printf '%s' "$$" > "$cgroup_root/supervisor/cgroup.procs"
printf '+cpu +memory +pids' > "$cgroup_root/cgroup.subtree_control"
exec /usr/local/bin/kelvo node --config /etc/kelvo/node.yml
```

State and delegation allow one manager each. Do not grant query workers cgroup write paths. Adapt only the exact owned service name; never move an existing session or unrelated service.

For each query **and refresh**, with configured DuckDB memory `M`:

| Budget | Required MiB |
|---|---:|
| Native process-tree cap | `M + native_overhead_mb` |
| Parent Arrow allowance | `M + parent_overhead_mb` |
| Minimum total reservation | `2M + native_overhead_mb + parent_overhead_mb` |
| Minimum `resources.overhead_mb` | `M + native_overhead_mb + parent_overhead_mb` |

Each tree has zero swap, grouped OOM handling, a process-count cap, and CPU quota derived from `limits.threads`. Parent schema, codecs, and sinks still require measured service/container headroom. DuckDB materializes execution before Arrow delivery; these budgets do not promise bounded parent RSS.

With configured resources, query reservations span process/scratch cleanup and durable result publication through Arrow EOS. Operation reservations also cover receipt persistence and prepared-resource cleanup; refresh reservations cover publication, pruning and staging cleanup.

Uncertain cleanup retains capacity, drains admission/readiness and reports failure. Retrying cleanup never silently resumes admission.

Recovery kills only inode-verified recorded trees. Unknown groups, corrupt records, or stale root identities fail closed. Recreating the service hierarchy with residual records requires operator repair: stop the owned service, verify its processes are gone, preserve the old state for inspection, then provision a fresh private state directory. Arbitrary reboot recovery is not automatic.

Run acceptance on a dedicated Linux test machine with noninteractive permission for the runner's exact transient service:

```sh
python3 scripts/containment_acceptance.py --go /path/to/go --report artifacts/containment.json
```

The 19 required baseline gates cover native OOM, CPU/pids limits, descendants, quarantine/recovery, safe syscall denial, Go/native threads, cancellation, refresh custody, a real DuckDB CTE, and valid/invalid startup placement. Missing, duplicate or skipped gates and uncertain service cleanup fail. Reports bind source/binary hashes and reject changes during execution; use a fresh output path to preserve prior evidence.

Add `--protected-objects` for the native bridge and protected snapshot fixture: 20 required root gates plus seven child cases under the protected worker gate. The [protected-reader receipt](evidence/protected-object-readers.json) records the current run and 36 runner controls.

[Historical publication evidence](evidence/process-containment-publication.json) records its 18-gate run and 13 runner controls. The [historical checkpoint](evidence/process-containment-acceptance.json) retains its earlier build-tag failure and metadata ledger. New publication evidence explicitly excludes `.DS_Store`/`._*` metadata from source fingerprints. Correctness checks do not certify throughput, cold-start footprint or multi-host production capacity.

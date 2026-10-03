# Managed worker scratch

Cluster nodes can opt into a private local scratch root that reclaims query
workspaces left by a killed node after their worker processes have exited.
Configure a separate directory for each node/service identity:

```yaml
scratch_directory: /var/lib/kelvo/node-a/scratch
```

Create this directory before startup, owned by the node's effective user with
mode `0700`. Its absolute path must contain no symlink components. Ancestors must
be owned by root or the service identity and must not be group/world writable; a
root-owned sticky temporary ancestor is permitted when its child remains under
trusted ownership. Keep it on a
local Linux filesystem with working `flock` semantics. Do not rename or replace
the directory while a node is using it. This is not shared NFS, object-storage
coordination or an alternative to disk/cgroup limits.

Both interactive queries and scheduled refreshes use the configured root. With
the setting absent, Kelvo retains its ordinary disposable system-temp behavior;
it does not scan `/tmp` or other applications' directories for garbage.

Each managed workspace has a random identifier and a sibling versioned ownership
record. The parent holds an exclusive advisory lock on that record and passes the
same open file description into the sandboxed worker. The sandbox permits writes
inside its own workspace; the record descriptor does not grant path access to
sibling directories. A live child retains its lock after its parent dies. Normal
cleanup drops the parent reference, then acquires a new independent nonblocking
lease before removing anything. A surviving descendant keeps deletion blocked
even after its leader has exited. Such cleanup returns an error and retains the
workspace for a later recovery attempt.

At startup and before allocation, Kelvo enumerates at most **4,096 root entries**
and attempts nonblocking locks. It skips workspaces whose lease is still held.
It removes an inactive workspace only when the matching complete ownership record
passes owner, mode, regular-file and single-hardlink checks. All filesystem
operations use an opened directory boundary; nested symlinks are removed without
traversing their targets. An independent root mutation lock serializes allocation
and reclamation across cooperating processes; contention fails explicitly.
The 4,096-entry bound covers the root inventory, not the recursive contents of a
workspace. Recursive deletion and filesystem operations have no guaranteed
wall-clock completion bound. Size and inode usage still need host/filesystem
limits and measured recovery objectives.

Unknown names outside the managed namespace are preserved. Reserved names without
valid records, incomplete records, ownership changes, top-level symlinks and an
oversized inventory fail closed. Inspect such cases manually during a stopped,
owned maintenance window. Do not repair them by broadly deleting the scratch root
while workers may still run. A node killed during record creation can leave an
incomplete record; automatic deletion deliberately does not guess its ownership.

These guarantees concern process crashes. The directory is not a durable result
store, and recovery does not restore query results or silently replay SQL. Power
loss, disk corruption and malicious processes with unrestricted access under the
same operating-system identity remain outside this ownership protocol. Parent
death also cannot ensure every remote database has canceled its native operation.

The [process-loss campaign](process-loss-acceptance.md) checks actual sandbox
lease inheritance, parent SIGKILL, preserved queued handles, failed-result
rejection and eventual scratch cleanup. The operational safety and longer-running
release requirements remain in the [production checklist](production-status.md).

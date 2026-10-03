# Managed worker scratch

Opt-in local scratch recovers workspaces left by a killed node once their workers exit. Queries and scheduled refreshes share the manager; omission retains disposable system-temp behavior.

1. Create a separate existing root per node/service identity, owned by its effective UID with mode `0700`.
2. Use an absolute path without symlink components on a local Linux filesystem supporting `flock`.
3. Configure the node:

```yaml
scratch_directory: /var/lib/kelvo/node-a/scratch
```

Ancestors must be root/service-owned and not group/world writable; a root-owned sticky temp ancestor is allowed under trusted ownership. Do not rename or replace the root while active. This is not NFS/object coordination or a disk quota.

Each random workspace has a versioned sibling ownership record. The parent's exclusive advisory lease passes into the sandbox; a live child/descendant retains it after parent death. Cleanup must obtain a new independent nonblocking lease before deletion, so surviving descendants block cleanup and leave the workspace for later recovery.

At startup and before allocation, the manager inventories at most **4,096 root entries**. It skips live leases and deletes inactive workspaces only with valid owner/mode/regular-file/single-hardlink records. Open-directory boundaries prevent nested symlink traversal; a root mutation lock serializes cooperating processes and fails explicitly on contention.

The root-entry limit does not bound recursive contents or deletion time. Apply host size/inode limits and measure recovery duration.

Unknown names outside the managed namespace remain untouched. Invalid/incomplete reserved records, ownership changes, top-level symlinks or excess entries fail closed. Inspect them during a stopped, owned maintenance window; never broadly delete the root while workers may run. A crash during record creation can require manual recovery.

This is process-crash ownership, not result persistence, SQL replay, power-loss durability or protection from unrestricted same-UID processes. Parent death also cannot guarantee remote database cancellation.

The [process-loss campaign](process-loss-acceptance.md) checks real lease inheritance, SIGKILL recovery, queued handles and failed-result rejection. See [production status](production-status.md) for broader release gates.

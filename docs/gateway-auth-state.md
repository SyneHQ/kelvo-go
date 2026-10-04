# Authentication history across restarts

Opt-in local state preserves the highest accepted key-file revision and retired-token ownership. It protects one gateway on a retained Linux volume. Each replica needs its own directory.

## Set up once

1. Drain and stop the gateway. When enabling this on an existing installation, issue fresh keys: today's key file cannot reconstruct retired-key history.
2. Add state to [file authentication](gateway-key-rotation.md):

```yaml
authentication:
  keys_file: /etc/kelvo/gateway-keys.yml
  reload_interval: 1s
  min_revision: 1
  state:
    directory: /var/lib/kelvo/gateway-auth
    scope: analytics-gateway-a
```

3. Provision the parent directory for your gateway's operating-system user. For an account named `kelvo`:

```sh
sudo install -d -m 0700 -o kelvo -g kelvo /var/lib/kelvo
```

4. Run once as that user:

```sh
kelvo auth-state-init --config gateway.yml
```

5. Start the gateway and check `/ready`. Retain this directory across restarts, upgrades and rescheduling.

Initialization creates only the final state directory; its parents must already exist. Use a canonical absolute path and a scope beginning with a lowercase letter, followed by lowercase letters, digits or hyphens, at most 63 characters.

Initialization reads configuration and keys without contacting databases or NATS. It creates private state and refuses overwrite. Normal startup requires the initialized files; missing or invalid state never falls back to memory.

## Rotate keys

Use the existing [overlap and revocation steps](gateway-key-rotation.md#rotate-and-revoke). Every changed key file needs a higher revision. New keys become active only after state is durably written; unchanged keys keep their existing request contexts during a timely, successful rotation.

Removed tokens remain bound to their original tenant and principal after restart. A higher revision may reintroduce a token for that same identity. Never reassign tokens. The scope and configured tenant set must remain unchanged; this version provides no migration or reset command.

| Boundary | Behavior |
| --- | --- |
| History | At most 8,192 fingerprints; no eviction or raw tokens |
| State | Bounded YAML, owner-only files and directory, one lifetime writer lock |
| Normal reload | Identical accepted content needs no persistence write |
| Requests | No filesystem I/O on authentication lookup |
| Invalid candidate | Authentication fails closed; a fresh valid file can recover |
| Storage failure or uncertain write | Authentication stays unavailable until the gateway is stopped and successfully reopened |
| Blocked I/O | Authority still expires; bounded shutdown reports uncertainty while the writer retains its lock until I/O finishes |

Use a local filesystem with working file locks, atomic rename and file/directory sync. NFS, FUSE and shared multi-writer volumes are outside this guarantee. Protect fingerprint metadata like credentials; keep state out of source control and logs.

## Recovery limits

Preserve the directory when initialization or startup fails. Check ownership, storage and the deployed key revision; do not delete files to clear an error. Reopening validates and syncs committed state before trusting it. An abandoned staging file never replaces committed authority.

Restoring an old volume can restore old history. Keep `min_revision` as an external deployment floor and protect backups. Same-UID/root modifications are outside the threat model.

This does not coordinate replica rollout, provide cluster-wide revocation, or make export completion atomic with revocation. Those remain open in the [production roadmap](production-status.md).

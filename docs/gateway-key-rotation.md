# Live gateway API-key rotation

Opt-in file authentication rotates tenant service keys without restarting gateways. Existing `token_env` keys still load once at startup. Every current key has its provisioned tenant's authority and access to existing handles.

## Configure file authentication

1. Remove `token_env` from **every** gateway tenant, retaining its scheduling, broker and worker policy.
2. Add the file reference:

```yaml
authentication:
  keys_file: /run/kelvo/gateway-keys.yml
  reload_interval: 1s
  min_revision: 1
```

3. Create an owner-only regular file under trusted directories, normally mode `0600`. Keep its contents out of source control:

```yaml
version: 1
revision: 1
tenants:
  analytics:
    - REPLACE_WITH_AN_INDEPENDENT_RANDOM_SERVICE_KEY
  reporting: []
```

Generate an independent cryptographically random key, such as 32 random bytes encoded as URL-safe base64. Name exactly the configured tenants; `[]` disables a tenant, while omission is invalid.

| Limit | Value |
| --- | --- |
| File / tenants / keys per tenant | 256 KiB / 256 / 4 |
| Key | 32–256 visible ASCII bytes; no commas or whitespace |
| Request | Exactly one `Authorization: Bearer <key>` header |

Symlinks, hardlinks, unsafe ancestors/modes, in-place changes, duplicate keys/fields, unknown fields, YAML aliases/merges and extra documents are rejected. Linux or macOS is required. Public errors omit keys and tenant identifiers. Hashes and cancellation contexts persist in memory; secret-buffer wiping is best effort, not guaranteed erasure.

## Rotate and revoke

These steps cover file-only authentication. With shared authority enabled, use the [authority-first rotation sequence](gateway-key-authority.md#4-rotate-keys).

1. Publish a complete higher revision containing old and new keys: fsync a private staged file, then atomically rename it onto the live path.
2. Verify the new key on **every replica**, then switch clients.
3. Publish another higher revision removing the old key. Verify old-key `401`, new-key access and readiness on every replica; remove stale/unreachable replicas from service.
4. Raise deployment `min_revision` before future restarts and retain the corresponding file.

Changed bytes, including whitespace, require a higher revision. By default, revision and retired-key history are in memory; restarting with an old `min_revision` and file can restore revoked keys. Enable [persistent authentication state](gateway-auth-state.md) to retain both across restarts. A higher revision can reintroduce a key for the same identity. The 8,192-binding limit rejects new ownership without eviction. Never reuse keys for another identity.

File-only propagation is operator-managed: a replica with its old valid file can continue accepting old keys. [Shared key authority](gateway-key-authority.md) adds fresh broker verification and a three-second local lease; rollout still needs per-replica checks and is not globally atomic. A valid current file can recover file-only authentication after I/O or permission failure.

## Failure and cancellation semantics

These are the file-only defaults. Shared authority adds [fixed renewal and cleanup rules](gateway-key-authority.md#failure-and-recovery).

| Condition | Behavior |
| --- | --- |
| Reload cadence | Default 1s; allowed 1–60s; read acceptance deadline 2s |
| Cached authority | Expires at read start + interval + 2s, enforced by timer and request path |
| Invalid, unavailable, late or expired file | Protected requests `401`, `/ready` `503`, `/health` `200`; no environment or last-good fallback |
| Reader blocked in filesystem | At most one read remains outstanding; late results cannot renew authority |
| Removed key | New requests denied; requests already authenticated with it canceled |
| File failure/expiry | All file-authenticated request contexts canceled |

Unchanged keys retain request contexts during overlap. Authentication requests do no filesystem I/O. A valid reload restores service after key-file rejection; uncertain persistent-state writes require a successful reopen.

Revocation cannot recall delivered bytes, undo durable success or guarantee immediate remote database cancellation. Require normal complete HTTP/Arrow/EOS checks. With version-1 keys, durable jobs remain tenant-owned: another current tenant key can retrieve/cancel an unconsumed handle. [Version-2 principal keys](principal-access.md) restrict handles to their submitting principal. Cancellation before a parked request claims results leaves the handle unclaimed.

Version 1 does not implement per-user access. Key documents alone do not define row policies, create tenants or rotate TLS identities. Version 2 can opt in to [shared document authority](gateway-key-authority.md); policy changes still require a drained rollout.

## Run the two-gateway acceptance gate

Use prebuilt binaries and an idle fixture on the isolated Linux test VM:

```sh
python3 scripts/cluster_fixture.py provision
python3 scripts/gateway_key_rotation_acceptance.py \
  --binary bin/kelvo --sandbox bin/kelvo-landlock \
  --output artifacts/gateway-key-rotation.json
python3 scripts/cluster_fixture.py stop
```

Choose a new report path and always stop owned brokers after the run. The runner installs nothing, retains private keys/logs in ignored artifacts and hash-checks original fixture YAML.

The gate covers overlap, delayed replica rollout, revocation, tenant-header nonoverride, invalid files/revisions, restart floors, request cancellation and exact typed results. Unit/race checks add blocked-reader, expiry and registration races. See [cluster boundaries](cluster.md) and [production status](production-status.md).

## Recorded acceptance

The [3 October 2026 record](evidence/gateway-key-rotation.json) passed eight gates with two gateways, two tenant workers and 100,000 rows per tenant. It includes seven file/revision failures, two parked-request cancellations, one running-query cancellation and successful exact results after rotation.

Cleanup found no forced application kills, observed live descendants or scratch leftovers; fixture hashes were unchanged. The local two-step rollout took 1.148s. That is an observation for the recorded binary/runner, not a deployment latency or globally atomic revocation guarantee.

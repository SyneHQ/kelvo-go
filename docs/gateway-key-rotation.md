# Live gateway API-key rotation

Gateway file authentication supports overlapping tenant service keys and bounded
per-replica revocation without restarting the gateway. It is opt-in. Existing
`token_env` configurations continue to load their key once at process startup.

Keys authenticate the tenant already provisioned in gateway and worker policy.
This feature does not add tenant creation, per-user access, row/column policy,
TLS certificate rotation, or a distributed authorization-epoch service. Every
current key for a tenant can access that tenant's existing query handles.

## Configure file authentication

Remove `token_env` from **every** tenant entry in gateway YAML, preserving each
entry's scheduling policy, broker configuration and worker endpoints. Add:

```yaml
authentication:
  keys_file: /run/kelvo/gateway-keys.yml
  reload_interval: 1s
  min_revision: 1
```

The key file must be a private regular file owned by the gateway service UID.
Use trusted directories and mode `0600` or `0400`. Every path component is opened
without following symlinks. Hardlinks, group/other access, nonregular files,
unsafe ancestors and detected in-place changes are rejected. This file mode
requires Linux or macOS; unsupported platforms fail closed.

The **private key file**, kept out of source control, contains:

```yaml
version: 1
revision: 1
tenants:
  analytics:
    - REPLACE_WITH_AN_INDEPENDENT_RANDOM_SERVICE_KEY
  reporting: []
```

Replace the illustrative value with a cryptographically random key, such as a
32-byte random value encoded with URL-safe base64. It is not a usable example
credential. A file must name exactly the tenants configured in gateway YAML.
An empty list explicitly disables a tenant's keys; omitting a tenant is an error.
No tenant identity comes from a caller-supplied header.

The file accepts up to four keys per tenant, 256 configured tenants and 256 KiB
total. Keys must contain 32–256 visible ASCII bytes without commas or whitespace.
Duplicate keys anywhere in the file, duplicate YAML fields, unknown fields,
anchors, aliases, merge keys, extra documents and unsupported versions fail
validation. Requests must contain exactly one `Authorization: Bearer <key>`
header. Key values, file contents and tenant identifiers are never included in
public authentication errors or new metrics.

The gateway retains token hashes and per-key cancellation contexts after loading;
parsing still creates transient secret strings in Go memory. Owned byte buffers
are cleared best effort. This is not a guarantee of secure memory erasure.

## Rotate and revoke

1. Write a complete new private file containing both the existing and replacement
   key for a tenant. Increase `revision`. Fsync the file and atomically rename it
   onto the configured path; do not edit the live file in place.
2. Publish that file to every gateway replica. Verify the replacement key works
   through each replica before switching clients. The old and replacement keys
   have the same existing tenant authority during overlap.
3. After clients switch, publish a higher revision containing only the replacement
   key. Verify the removed key returns `401` and the replacement works on **every**
   replica. Probe the intended tenant's actual operation as well as readiness.
4. Raise `authentication.min_revision` in the deployment configuration to the
   accepted revision before future restarts. Keep the corresponding key file
   available on every replica.

A running gateway rejects revisions below its last accepted revision. Reusing a
revision requires the **exact same file bytes** as the last accepted file;
comment, whitespace or ordering edits also require a higher revision. Invalid
updates cannot reset that in-memory revision floor. Restoring the exact valid
file can recover after an I/O or permission failure.

`min_revision` pins the startup floor. The in-memory highest revision is not a
persistent revocation ledger: restarting with an old configuration floor and an
old key file can restore old keys. Updating the deployment floor is part of the
rollout contract. An operator may intentionally introduce a previously used key
in a higher valid revision; use independently generated replacement keys.

Replica file propagation is an operator responsibility. A replica with its old
valid local file continues to accept its old keys until its file changes. This
is not globally atomic revocation. Remove an unreachable or stale replica from
service before declaring a security rollout complete.

## Failure and cancellation semantics

The reload interval defaults to one second and must be between one and 60
seconds. Each read has a two-second acceptance deadline. A successful snapshot
expires at its read start plus the reload interval plus two seconds. Both a
background timer and the HTTP authentication path enforce that monotonic
expiry; a delayed reload loop cannot let a new request use expired keys.

A read, permission, parse or revision failure immediately disables file-mode
authentication on that replica. `/ready` returns `503`; `/health` continues to
return `200`. Protected requests receive the same generic `401` response as an
unknown key. The gateway never falls back to environment tokens or an expired
last-known-good key set. A valid reload restores authentication and readiness.

There is at most one file read in flight per gateway. A filesystem syscall may
outlive its deadline; the gateway rejects its late result and waits for that
same reader to finish before launching another. Expired keys remain unusable
while it is blocked. Authentication requests perform no filesystem I/O.

Once a replica observes a valid revision that removes a key, new requests using
it are rejected. Requests already authenticated with that key are canceled,
including parked result requests and active result relays. File failure or
expiry cancels all file-authenticated request contexts. Unchanged keys retain
their contexts through a normal overlapping-key reload.

Cancellation cannot recall bytes already sent. A result whose durable success
commit completed before cancellation remains successful; revocation cannot undo
that commit. Incomplete or canceled result delivery must still pass the normal
complete HTTP, Arrow and final EOS checks before a client accepts it as a
completed result. Remote database cancellation retains its existing best-effort
semantics.

Previously submitted durable jobs remain tenant-owned. Removing a key does not
replay SQL or delete every accepted job for that tenant. Another current tenant
key can retrieve or cancel an unconsumed handle. A parked result request canceled
before claim leaves the handle unclaimed. Tenant data-policy changes and their
revocation requirements are a separate contract.

## Run the two-gateway acceptance gate

On the isolated Linux test VM, use the prebuilt binary and sandbox launcher with
an idle, provisioned fixture. The runner performs no package installation and
does not stop the fixture's broker processes:

```sh
python3 scripts/cluster_fixture.py provision
python3 scripts/gateway_key_rotation_acceptance.py \
  --binary bin/kelvo --sandbox bin/kelvo-landlock \
  --output artifacts/gateway-key-rotation.json
python3 scripts/cluster_fixture.py stop
```

Always stop the owned broker fixture after the campaign, including failures.
Choose a new output path. Private generated keys, YAML and logs stay under the
ignored operational fixture directory. Original fixture YAML is hash-checked
before and after; gateway/node cleanup follows the operational runner's owned
process checks.

The gate covers legacy startup, overlapping keys on two live gateways,
intentional replica propagation delay, removed-key denial, tenant-header
nonoverride, malformed/unavailable/unsafe key files, revision rollback and
equivocation, recovery, startup floors, parked/active request cancellation and
exact typed Arrow results after rotation. Unit and race tests additionally check
late request registration, one blocked reader, request-time expiry and unchanged
key contexts. Passing this local fixture is not live deployment certification or
a globally atomic revocation guarantee. See [cluster operation](cluster.md) and
[production delivery status](production-status.md) for the wider boundaries.

## Recorded acceptance

The [3 October 2026 acceptance record](evidence/gateway-key-rotation.json) passed
all eight gates with two gateways, two tenant-bound workers and 100,000 synthetic
Parquet rows per tenant. It includes seven invalid-file/revision failure modes,
restart-floor enforcement, cancellation of two parked requests and one running
query, and exact typed results after rotation. Cleanup recorded no forced
application kills, observed live descendants or worker scratch leftovers; the
original fixture YAML hashes were unchanged.

The deliberate two-step replica rollout took 1.148 seconds in this local run.
That observation is not a deployment latency guarantee. The record includes the
actual binary, launcher and runner hashes; it validates the tested credential
rollout, not global atomic revocation, tenant data-policy changes or TLS rotation.

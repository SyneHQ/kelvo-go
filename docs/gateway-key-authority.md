# Shared gateway key authority

Opt in to verify a fixed fleet's key-document revision and exact bytes through NATS before keys become active. File-only authentication keeps its current behavior.

Public authority setup and rotation commands are not available yet. Use reviewed in-repository tooling around the [authority protocol](key-authority-protocol.md); do not edit raw broker messages.

## 1. Prepare the fleet

- Use Linux, [version-2 principal keys](principal-access.md) and separate [durable authentication state](gateway-auth-state.md) for each gateway.
- Keep the complete tenant/gateway roster fixed. Match policy bindings across gateways, workers and job stores; membership changes require a drained reprovisioning.
- Use NATS **2.14.7 or 2.15.0**, TLS 1.3 and the plain, file-backed, three-replica stream. Keep [gateway, control and initializer permissions](key-authority-protocol.md#broker-and-credentials) separate from job dispatch.

A gateway can verify its own witness. Only the control identity can rotate shared authority.

## 2. Configure each gateway

Use that replica's ID, private local state directory and broker credentials. Authority mode requires the one-second reload interval.

```yaml
authentication:
  keys_file: /etc/kelvo/gateway-keys.yml
  reload_interval: 1s
  min_revision: 1
  state:
    directory: /var/lib/kelvo/gateway-auth
    scope: analytics-east
  authority:
    scope: analytics-fleet
    replica_id: east
    gateways: [east, west]
    nats:
      url: tls://authority.example.net:4222
      ca_file: /etc/kelvo/authority-ca.pem
      credentials_file: /etc/kelvo/authority-east.creds
```

Add this fragment under every tenant's existing `policy.access`, preserving its grants:

```yaml
key_authority:
  version: 1
  scope: analytics-fleet
  membership_sha256: REPLACE_WITH_VERIFIED_MEMBERSHIP_SHA256
```

Compute the binding with `Scope.MembershipSHA256()` over the complete tenant/gateway roster. This membership digest differs from the rotating key-file digest. Missing or mismatched bindings reject configuration.

## 3. Initialize and start

1. Drain and stop replicas before changing policy bindings. Provision separate state and credentials for each replica.
2. Run `kelvo auth-state-init --config gateway.yml` as the gateway user. This initializes local history offline; it does not create or verify shared authority.
3. Initialize the shared record once through reviewed control tooling, binding its positive revision to the exact private key-file SHA-256. Obtain a fresh control witness.
4. Start every replica with that exact file. Verify `/ready` and authorized handles before routing traffic.

## 4. Rotate keys

1. Prepare a complete higher-revision file. Conditionally advance shared authority, then atomically publish the file on every replica.
2. Verify new-key access, removed-key rejection and readiness everywhere. Expect a temporary availability gap while replicas catch up.
3. Preserve local history and raise deployment `min_revision` for future restarts. Never reassign a token to another identity.

A missing or ambiguous mutation acknowledgement means **unknown**. Obtain a fresh control check of the expected document; do not infer success or replay blindly.

## Failure and recovery

| Boundary | Behavior |
| --- | --- |
| Renewal | One fresh client; the original two-second budget includes file reading, verification, cleanup, durable admission and publication |
| Local lease | Expires three seconds after attempt start; late work cannot extend it |
| Failed renewal or expired lease | Requests denied, readiness fails and active key contexts cancel |
| Uncertain cleanup or state write | Instance stays unavailable and retains loader/writer ownership until work joins |
| Restored local history | Supplied keys must still match current shared authority |

Never delete state or start a competing writer to clear blocked I/O. Recoverable file/network failures need a fresh valid renewal; a permanently fenced instance needs a successful stopped restart.

## Scope and acceptance

[Linux acceptance](evidence/gateway-key-authority.json) on `84b6d20` passed 24 stages: 369 ordinary roots and 27 authority fixture paths in each regular/race profile, across both broker versions. Source, binary and cleanup receipts were independently checked.

Job stores and upstream Arrow responses are fixtures. Source execution, sandboxed workers, exports/downloads and deployment capacity need separate evidence.

Shared authority does not provide a global retired-token ledger, automatic enrollment, protection against simultaneous broker/local-state rollback, atomic export revocation or recall of delivered bytes. See [production status](production-status.md) and [validation](validation.md#shared-gateway-authority).

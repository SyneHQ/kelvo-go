# TLS identity rotation

Kelvo can reload a process's certificate/key pair without restart. [Trust rotation](tls-trust-rotation.md) separately changes peer CAs and revocations; certificate issuance, NATS credentials and replica rollout remain operator-managed.

## Configure private identity files

1. Replace `cert_file` and `key_file` with one private PEM containing the matching leaf, intermediates in issuer order and private key:

```yaml
tls:
  identity_file: /run/kelvo/tls/server-identity.pem
  reload_interval: 1s
  ca_file: /etc/kelvo/tls/ca.pem
```

2. Configure a gateway's outbound identity separately:

```yaml
worker_tls:
  identity_file: /run/kelvo/tls/gateway-client-identity.pem
  reload_interval: 1s
  ca_file: /etc/kelvo/tls/worker-ca.pem
```

The first example is a worker listener. Omit `ca_file` on a public gateway listener; it does not enable client-certificate authentication there. Public requests use [gateway authentication](gateway-key-rotation.md). Worker/gateway SPIFFE role checks remain mandatory.

Paths may be YAML-relative. Reload defaults to 1s and accepts 1s–1h. Mixing rotating/static identity fields, or setting an interval without `identity_file`, is invalid.

| Private-file requirement | Accepted value |
| --- | --- |
| Ownership and path | Effective service UID; owner-only mode; trusted ancestors; no symlink components or extra hardlinks |
| Size and shape | Regular file, at most 256 KiB; concurrent changes rejected |
| Key and chain | One unencrypted PKCS#8, PKCS#1 RSA or SEC1 EC key; at most 16 certificates |
| Validation | Matching key/leaf, current validity and issuer signatures, configured SPIFFE URI and intended EKU; absent EKU follows X.509 rules |

Unknown/malformed PEM blocks, PEM headers and trailing data fail. Store only paths in YAML; keep PEM secrets out of logs and tickets.

## Publish a new identity

1. Stage the complete pair on the same filesystem under trusted ownership.
2. Set owner-only permissions and atomically rename it over the live file. Never overwrite in place:

```sh
chmod 600 /run/kelvo/tls/server-identity.next.pem
mv /run/kelvo/tls/server-identity.next.pem /run/kelvo/tls/server-identity.pem
```

3. Fsync the staged file and parent directory when crash durability is required.
4. Verify readiness and the new serial/fingerprint over a **new TLS connection** on every replica. Existing connections do not renegotiate. Keep issuer overlap until all peers trust the replacement.

A read already holding the old inode can finish with that complete identity; a later poll sees the replacement.

## Failure and lifecycle contract

Each server has one reloader; a gateway shares one outbound reloader across workers. Each permits one outstanding read, with a 2s acceptance deadline. Blocked syscalls cannot spawn replacement readers or renew authority through late results.

Accepted authority ends at the earlier of chain expiry or `read start + reload_interval + 2s`, using elapsed time after load. Handshakes and newly admitted requests enforce this bound.

Invalid pairs, roles, permissions, reads or expiry disable the identity without last-good fallback. New handshakes fail; new requests on keepalive connections become unavailable. Missing outbound identity fails gateway readiness. A valid replacement restores service.

Already admitted requests keep their existing deadlines. Identity rotation does not cancel running queries, revoke peer certificates or undo results; [trust rotation](tls-trust-rotation.md) rechecks peers on new requests. CA, DNS/IP, SPIFFE and TLS 1.3 verification remain enabled. Rotating listeners disable session tickets; outbound clients use no session cache.

Shutdown stops reload loops without waiting indefinitely for blocked filesystem calls. A still-valid older identity may be reinstalled: this feature has no revision-based rollback floor.

## Validation

On the designated Linux test host:

```sh
GOMAXPROCS=2 go test -race -p 2 ./internal/cluster
GOMAXPROCS=2 go test -p 2 -tags duckdb_arrow ./cmd/kelvo
```

Tests cover actual HTTPS/mTLS reuse, server/client replacement, invalidation and recovery; wrong SPIFFE/DNS/issuer, missing certificates and TLS 1.2; malformed/expired PEM, file safety, bounded reads and drain behavior.

These are in-process Linux checks. [Trust acceptance](tls-trust-rotation.md#validation-scope) adds CA rollover and stale replicas. A certificate-manager deployment and coordinated revocation require separate [production validation](production-status.md).

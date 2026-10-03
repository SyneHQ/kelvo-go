# TLS trust rotation and peer revocation

Opt-in trust reloads govern gateway/worker mTLS. Every new handshake and HTTP request, including reused connections, requires current trust. [Identity rotation](tls-identity-rotation.md) separately replaces each process's certificate/key.

## Configuration

Replace `ca_file` with `trust` in gateway `worker_tls` and worker `tls`:

```yaml
worker_tls:
  identity_file: /run/kelvo/tls/gateway-client.pem
  reload_interval: 1s
  trust:
    file: /run/kelvo/tls/worker-trust.yml
    minimum_epoch: 42
    reload_interval: 1s
```

`minimum_epoch` must be positive. Trust reload defaults to 1s, accepts 1–60s and resolves paths relative to YAML. Static cert/key identities can use rotating trust. Public gateway listeners, static TLS builders and configurations combining `ca_file` with `trust` reject it.

## Immutable trust documents

Publish a private YAML document with current dates and complete CA PEM:

```yaml
version: 1
epoch: 42
issued_at: 2026-10-03T12:00:00Z
expires_at: 2026-10-03T12:30:00Z
roots_pem: |
  -----BEGIN CERTIFICATE-----
  ...complete CA certificate...
  -----END CERTIFICATE-----
revoked_identities:
  - spiffe://kelvo/tenant/example/worker/retired-node
revoked_certificates: []
```

The document is filesystem-authenticated, **not signed**. Require service ownership, owner-only permissions, trusted ancestors, a regular file ≤256 KiB, and no symlinks/hardlinks. Concurrent modification fails. Atomically replace a complete staged file; fsync file and parent for crash-durable publication.

| Field | Contract |
| --- | --- |
| Roots | At most 16 unique valid CA certificates with signing usage and valid CA constraints |
| Validity | No future issuance, expired authority or window longer than 1h |
| Revocations | At most 1,024 entries per list |
| Identity | Exact gateway or tenant/worker SPIFFE URI; no wildcard/prefix matching |
| Certificate | Lowercase 64-character SHA-256 of DER; matches a verified leaf, intermediate or root |
| YAML | One bounded mapping; no unknown/duplicate fields, aliases, merges or extra documents |

Malformed PEM and unsupported critical extensions fail. An alternative valid chain without revoked certificates may authorize a peer; identity revocation denies every certificate with that identity until a later epoch removes it.

Normal CA, DNS/IP SAN, EKU, validity, SPIFFE and TLS 1.3 checks remain mandatory. There is no trust-all callback.

## Epochs, rollback and stale replicas

Changed bytes or renewed validity require a higher epoch; the same epoch must have identical bytes, including whitespace. Lower epochs and same-epoch changes disable trust without last-good fallback. An exact valid current document or valid higher epoch restores service.

`minimum_epoch` is the configured persistent restart floor; the observed high-water mark is only in memory. Ratchet deployment/recovery configuration before restart. An old floor plus old unexpired document can restore old authority. There is no global epoch consensus or autonomous durable revocation ledger.

A stale replica cannot discover another replica's newer document. Its old valid authority lasts until document expiry, at most 1h after issuance. Use shorter validity and verify every replica when a shorter revocation window is required.

Each reloader permits one read with a 2s acceptance deadline. Cached authority expires at the earliest of document expiry, CA expiry or `read start + interval + 2s`. Elapsed-time checks prevent delayed timers or blocked/late reads from renewing authority. Failures deny new handshakes and requests.

## CA rollover procedure

1. Publish epoch N containing old and new roots everywhere; verify each replica.
2. Rotate identities to the new issuer. Verify new connections and presented chains.
3. Publish a higher epoch removing the old root. Raise the configured minimum epoch, including recovery configuration.
4. Check old-CA rejection, stale-document rejection and revocations on new and reused connections.

Rollback is another higher-epoch policy change. Reintroducing roots or removing revocations restores their authority. Identity/trust files change separately; a mistimed rollout fails closed. Kelvo does not issue certificates, rotate NATS/public-client trust or distribute documents.

## Existing connections and bounded resources

Inbound middleware rebuilds peer chains under current roots/revocations before query admission; rejection returns generic unavailability. Server session tickets and outbound client session caches are disabled.

Outbound pools bind normalized HTTPS authority, verification hostname, full local certificate chain and trust digest. Every request checks current identity/trust and every admitted peer chain. Changes or invalid peers retire idle pools; new handshakes perform normal verification plus current-trust checks.

An endpoint permits at most four active pools and 16 peer chains per pool. Rapid rotation can reject admissions until earlier requests finish. Body ownership releases at EOF or Close, not an arbitrary read error; callers must close bodies. Already admitted requests keep their deadlines and active bodies. Rotation cannot retract bytes or undo completed work.

## Validation scope

The [Linux acceptance record](evidence/tls-trust-acceptance.json) identifies source `0e4967f`, the frozen input hashes and built binary. Cluster race checks passed 274 test events, including 32 trust cases; 52 CLI tests and vet passed. Nine unrelated live-NATS tests skipped because no broker fixture was supplied; trust tests had no skips.

Two gateway runtimes and two worker HTTPS servers exercised CA overlap, identity replacement, old-CA removal, certificate/identity revocation, connection reuse and unaffected peers. Additional cases cover stale replicas, expiry, restart floors, DNS/IP checks, bounded readers/pools and active body ownership. The record retains the initial IP-hostname binding failure and its fix.

```sh
GOMAXPROCS=2 go test -race -p 2 ./internal/cluster -run TestTLSTrust -count=1
```

Run on the designated Linux host. These are real loopback exchanges inside tests; the built binary received a version smoke check. They do not certify a deployed certificate manager, WAN partitions, distributed consensus or throughput.

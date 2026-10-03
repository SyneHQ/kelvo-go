# TLS trust rotation and peer revocation

Kelvo can reload the trust roots and peer revocations used between gateways and
workers. Each new mTLS handshake and HTTP request requires current trust,
including requests on existing connections. Accepted requests retain their
existing execution and transport deadlines.

Trust rotation is opt-in. It does not issue certificates, manage NATS trust,
rotate public client trust, or synchronize epochs across machines. An operator
publishes the private documents and coordinates rollout. The
[identity rotation](tls-identity-rotation.md) mechanism rotates each process's
own certificate and key independently.

## Configuration

Replace `ca_file` with `trust` in a worker's `tls` section and in each gateway's
`worker_tls` section:

```yaml
worker_tls:
  identity_file: /run/kelvo/tls/gateway-client.pem
  reload_interval: 1s
  trust:
    file: /run/kelvo/tls/worker-trust.yml
    minimum_epoch: 42
    reload_interval: 1s
```

A worker uses the same fields beneath `tls`. Static `cert_file`/`key_file`
identities remain supported with rotating trust. `minimum_epoch` is mandatory
and positive. `trust.reload_interval` defaults to one second and accepts one
second through one minute. File paths are relative to the node/gateway YAML.

Public gateway listeners authenticate API clients through the gateway token
policy. They reject `trust`, because they do not authenticate mTLS peers.
Configuring `ca_file` and `trust` together is an error. The managed server and
gateway lifecycle is required; static TLS builders reject rotating trust.

## Immutable trust documents

An example private document is:

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

The document is authenticated by filesystem ownership and permissions. It is
not a signed document and has no external authority service. Files must be
service-user-owned regular files with no group/other permissions, hard links,
or symlink path components. Trusted ancestors are checked through opened
descriptors. The existing private-document reader enforces a 256 KiB limit and
rejects concurrent content modification. Prepare a complete private replacement
and atomically rename it into place; fsync the staging file and parent directory
when publication must survive a host crash.

The parser requires one YAML mapping, rejects unknown/duplicate fields,
multiple documents, aliases and merge keys, and bounds nesting and node count.
It accepts at most 16 unique currently valid CA certificates and 1,024 entries
in each revocation list. CA certificates need valid CA constraints and
certificate-signing key usage. Unsupported critical extensions, malformed PEM,
future issuance, expired trust and validity longer than one hour are rejected.
Every renewal requires a strictly higher epoch. The same epoch may be re-read
only when its bytes are identical, including whitespace.

`revoked_identities` denies exact gateway or tenant/worker SPIFFE identities.
It does not match prefixes or accept wildcards. `revoked_certificates` contains
lowercase 64-character SHA-256 hashes of DER certificates. A revocation can
match a leaf, intermediate or root in a currently verified certificate chain.
If an alternative valid chain contains no revoked certificate, that chain may
authorize the peer. Revoking a SPIFFE identity denies every certificate that
presents that identity until a later epoch explicitly removes the revocation.

Normal CA verification, server DNS/IP SAN checks, EKU, certificate validity,
TLS 1.3 and the configured SPIFFE identity remain mandatory. Certificate-name
matching alone cannot establish trust. No trust-all TLS callback is used.

## Epochs, rollback and stale replicas

Each process retains its highest accepted epoch and its exact document digest.
A lower epoch or a changed document at the same epoch invalidates its current
trust. An invalid replacement does not retain a last-known-good authorization.
An exact valid document at the current epoch, or a valid higher epoch, can
restore service.

The configured `minimum_epoch` is the persistent rollback floor. Ratchet that
configuration as part of the rollout before restarting a replica. The runtime
high-water mark is in memory: restarting with both an old configuration floor
and an older unexpired document can accept that old epoch. Kelvo does not claim
a durable autonomous revocation ledger or global epoch consensus.

A replica repeatedly reading an older valid document cannot discover that
another machine has a newer one. Its authority ends at its document's absolute
expiry, at the latest one hour after issuance. Use shorter validity when a
shorter stale-replica window is required and monitor rollout across every
replica. A configured higher floor rejects a stale document immediately at
startup. Missing or invalid documents fail closed independently on each
replica; they do not silently renew trust.

The reloader accepts each read within two seconds and allows at most one
filesystem read at a time. A blocked syscall cannot spawn repeated readers or
renew authority when it eventually returns. Cached authority ends at the
earliest of document expiry, CA expiry, or read start plus reload interval plus
two seconds. The elapsed-time clock bounds that cached lease. Reload failures,
permissions changes, late reads and expiry deny new handshakes and requests.

## CA rollover procedure

1. Publish epoch N with both current and replacement CA roots to every worker
   and gateway. Verify availability on each replica before changing identities.
2. Rotate gateway and worker identities to certificates issued by the new CA.
   Establish new connections and verify the presented certificate chain on each
   replica. Existing connections do not renegotiate their certificate.
3. Publish a higher epoch containing only the replacement CA once all peers
   have moved. Ratchet the configured minimum epoch to the new floor, including
   deployment and recovery configuration.
4. Verify that old-CA peers and stale documents fail and that both old and new
   connections obey the intended revocations. Keep rollback documents at new
   epoch numbers; never reuse an earlier epoch for changed policy.

Rollbacks are explicit policy changes with a new epoch. Reintroducing an old
root or removing a revocation deliberately restores that authority, so review
the complete replacement before publishing it. Identity and trust are separate
files: a mistimed rollout fails closed rather than atomically changing both.

## Existing connections and bounded resources

Inbound HTTP middleware reconstructs each peer's chain against the current
roots and revocations before calling the application handler. Rejected requests
receive a generic unavailable response; the request does not reach query
admission. Server session tickets are disabled for rotating trust.

Outbound transports retain separate immutable pools for each trust-document
and local certificate-chain fingerprint. Before every new request they check
local identity, current trust and every peer chain admitted to the selected
pool, including current hostname and certificate validity. A changed identity,
trust document or invalid peer retires the old idle pool. New handshakes use
normal verification against the selected roots and also recheck current trust.
Client TLS session caching is disabled.

An endpoint keeps at most four pools with active requests and at most sixteen
distinct peer chains per pool. A rapid sequence of rotations that fills this
bound rejects additional admissions until an older request finishes. Body
ownership ends at EOF or explicit close; a non-EOF read error retains the lease
until close. Operators must retain the existing request deadlines and callers
must close response bodies. Retiring an idle pool does not interrupt active
bodies. Already admitted requests may complete under their existing deadlines;
rotation cannot retract delivered bytes or undo a running query.

## Validation scope

The focused suite creates two gateway client runtimes and two worker HTTPS
servers with independent trust and identity reloaders. It tests CA overlap,
identity replacement, old-CA removal, certificate/identity revocation, reused
inbound and outbound sessions, and unaffected-peer progress. Additional tests
exercise stale/fresh replicas, expiry and recovery, configured and in-process
rollback floors, same-epoch mutation, DNS and IP SAN verification, bounded
readers, overlap capacity, body ownership and active-request completion.

These are real loopback TLS exchanges in the Linux test process. They do not
validate a certificate-manager deployment, a distributed consensus service or
real network partition behavior. Run the race suite on the designated Linux
host and retain revision/hash evidence before marking the production ticket
accepted:

```sh
GOMAXPROCS=2 go test -race -p 2 ./internal/cluster -run TestTLSTrust -count=1
```

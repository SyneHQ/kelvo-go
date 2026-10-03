# TLS identity rotation

Kelvo can reload a gateway or worker's TLS certificate and private key without
restarting its process. Rotation is opt-in and publishes one complete identity
per process. HTTP requests and handshakes fail closed if that identity becomes
invalid or stale.

This feature rotates **local certificate/key identities**. Optional
[trust rotation and peer revocation](tls-trust-rotation.md) separately manage
private CA bundles and peer policy between gateways and workers. Operators
coordinate their rollout and NATS credentials. Identity rotation does not provide
certificate issuance, a global revocation ledger, or an atomic change across replicas.

## Configure private identity files

In a gateway or node configuration, replace `cert_file` and `key_file` with one
private PEM file containing the leaf certificate, any intermediate certificates
in issuer order, and its private key:

```yaml
tls:
  identity_file: /run/kelvo/tls/server-identity.pem
  reload_interval: 1s
  # Required for a node accepting gateway client certificates.
  ca_file: /etc/kelvo/tls/ca.pem
```

For a gateway's outbound worker connections, configure its client identity
separately:

```yaml
worker_tls:
  identity_file: /run/kelvo/tls/gateway-client-identity.pem
  reload_interval: 1s
  ca_file: /etc/kelvo/tls/worker-ca.pem
```

For a public gateway listener, omit `ca_file`; setting it does not enable
inbound client-certificate authentication. Its API requests continue to use the
[gateway authentication policy](gateway-key-rotation.md).
The node listener always requires the configured gateway SPIFFE identity, and
outbound worker connections always verify the selected tenant/worker identity.
Enabling identity rotation does not change these roles.

Paths may be relative to the YAML configuration. `reload_interval` defaults to
one second and accepts values from one second to one hour. It is an error to
mix `identity_file` with `cert_file`/`key_file`, or to configure an interval
without an identity file. Existing static file configuration remains supported.

Each identity file must:

- Be owned by the Kelvo effective user, with no group or other permissions
  (normally mode `0600`), and reside under trusted directories.
- Be a regular file, no larger than 256 KiB, with no symlink path components or
  additional hard links. The existing private-file reader checks ownership,
  permissions, and concurrent modifications against its opened descriptor.
- Contain one unencrypted PKCS#8, PKCS#1 RSA, or SEC1 EC private key and at most
  16 certificates, beginning with its matching leaf. PEM headers, unknown
  blocks, malformed blocks, and trailing non-PEM data are rejected.
- Have a currently valid leaf and supplied intermediate chain with valid issuer
  signatures, a matching configured SPIFFE URI, and the intended server/client
  extended key usage. An omitted EKU permits both usages under X.509 rules.

Do not put private PEM data into repository configuration, logs, or tickets.
The file holds the secret; YAML only holds its path.

## Publish a new identity

Prepare the complete replacement under the same protected directory, apply the
owner and mode, then rename it over the configured identity file on the same
filesystem. The certificate and key must be published together; do not overwrite
an active file in place. For an existing private staging file:

```sh
chmod 600 /run/kelvo/tls/server-identity.next.pem
mv /run/kelvo/tls/server-identity.next.pem /run/kelvo/tls/server-identity.pem
```

A rename avoids exposing a half-written pair. A read that already opened the old
inode can finish with that entire previous identity; the next successful poll
observes the replacement. If crash durability is required, the publisher must
also fsync the staged file and parent directory. Kelvo reads identities and does
not write or issue them.

On each replica, verify readiness and establish a **new TLS connection** to
observe the new certificate serial/fingerprint. An existing TLS connection does
not renegotiate its identity when the file changes. Allow sufficient overlap
for peers to trust the new certificate before retiring any issuer, and stage
trust changes through a separate rollout.

## Failure and lifecycle contract

Each server has one reloader. A gateway shares one outbound identity reloader
across all worker endpoints; its number of filesystem readers does not grow
with the number of workers. Each reloader has at most one in-flight read and a
two-second acceptance deadline. A filesystem call that cannot be interrupted
may remain blocked, but it cannot create replacement reader goroutines or
renew authority with a late result.

An accepted identity is valid until the earlier of certificate/chain expiry or
`read start + reload_interval + 2 seconds`. Expiry uses the elapsed-time clock
after loading, so a delayed reload loop does not extend accepted authority.
Each handshake and each newly admitted HTTP request checks this bound.

An invalid read, bad certificate/key pair, wrong local SPIFFE role, permission
failure, expiry, or read timeout invalidates the cached identity. There is no
last-known-good fallback. New handshakes fail, and new requests on existing
keepalive connections receive an unavailable response. Gateway readiness also
fails when its outbound worker identity is unavailable. A valid subsequent
atomic replacement restores operation on that replica.

Requests admitted before invalidation may finish under their existing query,
transport, and drain deadlines. Rotation does not cancel authenticated running
queries or undo delivered results. Identity rotation alone does not reauthenticate
established peer connections or revoke a peer certificate; opt-in
[trust rotation](tls-trust-rotation.md) performs current peer checks on new requests.
Current TLS trust verification, DNS checks,
SPIFFE checks, and TLS 1.3 minimum remain enabled. Rotating server listeners
disable TLS session tickets; outbound clients do not configure a session cache.
This ensures a new connection selects the current local identity.

Closing a server or gateway stops its reload loop after its ordinary lifecycle
and does not wait indefinitely for a blocked filesystem syscall. A previously
issued certificate can be installed again if it remains valid and trusted;
identity rotation does not implement revision-based rollback prevention.

## Validation

The automated cluster suite exercises actual HTTPS/mTLS handshakes and HTTP
reuse: old-to-new server and gateway client certificates, invalid-file rejection
on both new and established connections, and successful recovery. It also checks
wrong tenant/worker identity, incorrect DNS, an untrusted client issuer, a missing
client certificate, TLS 1.2 rejection, malformed/mismatched/expired/future PEM,
private-file permissions and links, bounded reads, late-result rejection,
concurrent snapshots, expiry, and preserved drain behavior.

Run on the designated Linux test host:

```sh
GOMAXPROCS=2 go test -race -p 2 ./internal/cluster
GOMAXPROCS=2 go test -p 2 -tags duckdb_arrow ./cmd/kelvo
```

These checks validate in-process HTTP servers and gateway runtime wiring. They
do not certify a live certificate-manager deployment or globally coordinated
revocation. The separate [trust suite](tls-trust-rotation.md#validation-scope)
covers loopback CA rotation and stale-replica behavior. See the
[production checklist](production-status.md) for remaining deployment gates.

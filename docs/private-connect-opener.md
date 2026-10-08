# Parent CONNECT opener

`internal/transportbroker/rabbitconnect` is dormant. No CLI, worker, adapter or application route uses it. It implements the parent side of Rabbit v1 CONNECT; it does not enable private-source querying.

## What it enforces

- Operator-selected proxy address, CA roots, TLS 1.3, hostname and fixed client identity. Configuration buffers are copied; source requests cannot change them.
- Exact issuer, audience, tenant, source revision, original authority, execution/worker/owner/claim, physical-open ID and client-certificate binding before dialing.
- A fresh issuer call for each physical open. No cache, retry, redirect or direct-source fallback.
- One setup deadline, at most two seconds, shared by issuance, TCP, TLS and CONNECT. Admission, certificate and caller deadlines can shorten it.
- A bounded, headerless v1 response parser. Malformed tokens, duplicate JSON keys, alternative framing and redirects fail closed. Bytes received after CONNECT remain available to the native driver.
- Joined setup cancellation before returning a socket. A failed close returns the socket with a cleanup error so the broker retains its reservation.

The opener resolves only its configured proxy. Database TLS and the original database hostname remain the native driver's responsibility. Outer TLS supports `CloseWrite`; unsupported `CloseRead` returns an explicit error.

## Remaining gates

1. Implement the application [issuer endpoint](private-issuer-protocol.md). The bounded mTLS HTTP client exists, but the endpoint must verify current source mapping and live execution custody.
2. Add parent/child IPC with exact process and execution custody. A Go interface is not a process isolation boundary.
3. Wire native source DNS and TLS hooks. PostgreSQL needs an explicit cancellation hook; the current `pgx` configuration does not select a separate cancellation capability.
4. Qualify the opener against actual Rabbit, native PostgreSQL/MySQL, revocation, shutdown and cleanup. Local TLS fixtures alone do not establish this path.
5. Qualify reserved cancellation across issuer, both Rabbit socket halves and the source. V1 has no downstream reserved lane. A raw auxiliary socket can carry same-source traffic; it cannot prove the bytes are a PostgreSQL cancellation request.

See [broker ownership](private-transport-broker.md) and [the integration issue](https://github.com/SyneHQ/kelvo-go/issues/162). No throughput or saturation guarantee is made for this dormant code.

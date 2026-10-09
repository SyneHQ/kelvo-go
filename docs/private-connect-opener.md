# Parent CONNECT opener

`internal/transportbroker/rabbitconnect` implements the parent side of Rabbit v1 CONNECT for opt-in [private operations](private-operation-transport.md). Configuration alone does not qualify a deployment.

## What it enforces

- Operator-selected proxy address, CA roots, TLS 1.3, hostname and fixed client identity. Configuration buffers are copied; source requests cannot change them.
- Exact issuer, audience, tenant, source revision, original authority, execution/worker/owner/claim, physical-open ID and client-certificate binding before dialing.
- A fresh issuer call for each physical open. No cache, retry, redirect or direct-source fallback.
- Fresh source resolution uses the configured resolver timeout, at most 30 seconds. Only after verification does the separate two-second issuance/TCP/TLS/CONNECT budget start. Admission, certificate and caller deadlines can shorten either phase.
- A bounded, headerless v1 response parser. Malformed tokens, duplicate JSON keys, alternative framing and redirects fail closed. Bytes received after CONNECT remain available to the native driver.
- Joined setup cancellation before returning a socket. A failed close returns the socket with a cleanup error so the broker retains its reservation.

The opener resolves only its configured proxy. Database TLS and the original database hostname remain the native driver's responsibility. Outer TLS supports `CloseWrite`; unsupported `CloseRead` returns an explicit error.

## Deployment acceptance

Qualify the deployed worker, adapter, issuer and Rabbit together. Tests must cover real PostgreSQL queries, source termination, revocation, shutdown and physical cleanup. Local TLS fixtures do not establish those deployment properties.

PostgreSQL uses [typed cancellation and signed acceptance receipts](private-source-cleanup.md). A closed socket alone does not prove that a source query stopped. Keep private MySQL disabled until its cancellation path is qualified.

See [broker ownership](private-transport-broker.md) and [activation gates](private-transport-activation.md). No throughput or saturation guarantee follows from this component alone.

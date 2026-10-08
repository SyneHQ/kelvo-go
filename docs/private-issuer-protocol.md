# Private connection issuer

`transportissuer` defines the application endpoint contract. Kelvo's internal
`rabbitconnect.HTTPIssuer` implements its client. Both remain disconnected from
runtime configuration until source authorization and process custody are wired.

The trusted parent sends `POST /v1/private-transport/issue` over verified TLS 1.3
with its worker certificate. The body includes the source revision, original
`host:port`, execution/worker/owner/claim, authority expiry and a fresh open ID.
It contains no database password, registration token or destination proxy URL.

The application must independently verify:

1. The client certificate matches `worker_identity` and `worker_cert_sha256`.
2. The current tenant/source revision maps to the exact Rabbit token and live owner.
3. The execution grant, worker incarnation and claim are still authorized.
4. The requested deadline is within the current source and execution leases.

Return `version: 1`, `request_sha256` of the exact request body and the signed
Rabbit v1 `token`. The parent still verifies its signature and complete scope.
A request body or certificate alone does not prove source or execution authority.
Use generic rejection messages; never echo tokens, credentials or request bodies.

The client bounds requests to 8 KiB, responses to 12 KiB and headers to 8 KiB.
Its operator-selected concurrency limit is 1–64; excess work fails immediately.
One two-second setup budget includes issuance and the later CONNECT handshake.
Redirects, response compression, duplicate JSON keys and retries are disabled.
Each issuance uses a fresh mTLS connection; database connection pooling can reduce
issuance frequency. This does not establish a throughput guarantee.

`Shutdown(ctx)` stops admission, cancels active requests and joins their owners.
A timeout retains the same cleanup owner; it does not claim cleanup completed.

This is separate from Rabbit's `/transport-lease` contract. That endpoint rechecks
live custody throughout an admitted session and returns leases of at most 15s.
The issuer service, lease service and parent/child IPC are still required before
production activation. Version-2 cancellation reservations remain disabled.

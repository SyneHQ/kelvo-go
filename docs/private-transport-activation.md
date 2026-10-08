# Private transport activation order

The HTTP issuer client is implemented. Production activation still needs these
owners; a CLI switch cannot replace them.

| Step | Owner | Concrete change and acceptance |
|---|---|---|
| 1 | Application authority / DBAPI | Extend the private resolver after its existing delegation, metadata and live-lease checks. Mint a parent-only proof bound to the source revision, original authority, worker certificate and exact execution claim. Retain enough signed grant information to repeat live-lease validation. |
| 2 | Kelvo parent | Accept that proof only through the verified resolver response. Keep it beside the execution owner, outside the child catalog, input, environment and credential files. Reject unsupported source adapters before launch. |
| 3 | Application issuer | Authenticate worker mTLS; verify proof and current source/team access; fetch Rabbit route with a private management credential; sign one fresh physical-open ticket. Expose the separate Rabbit lease endpoint and repeat revocation/custody checks on every renewal. |
| 4 | Kelvo IPC | Inherit a dedicated Unix socketpair into the exact child. Bind server ownership to its process handle, incarnation and execution; expose only the admitted source dial operation. Transfer accepted socket descriptors, never tickets or worker TLS keys. Close and join all pending requests on process exit. |
| 5 | Native adapters | PostgreSQL first: source TLS keeps its original hostname; normal and cancellation opens use explicit capabilities. The parent retains cancellation/cleanup custody after query EOF. Add MySQL and other drivers only after their own qualification. |
| 6 | Deployment | Pin tested images; provision worker/authority PKI, exact network peers and fixed private14443. Test actual customer client → Rabbit → Oracle worker → DBAPI before enabling user requests. |

The existing DBAPI resolver accepts a signed delegation and canonical query,
then calls `ValidateConnectionLease` before and after source resolution. A request
containing only `grant_sha256`, a tenant name or worker mTLS cannot replace that
proof. Do not add a fallback that trusts caller-selected source or network data.

The source-to-Rabbit token mapping belongs to the application catalog. Rabbit
route discovery proves the live tunnel identity, not the customer's permission
to query that source. The application must check both.

Version-2 reservations stay disabled until issuer admission, live parent ownership,
all socket/handshake reservations and PostgreSQL cancellation pass saturation
and shutdown tests together. The current uncongested native fixture is reusable
transport evidence, not evidence for this new activation path.

No source query may be retried automatically after an uncertain write. Tests must
observe the source backend, both Rabbit stream directions and retained execution
ownership before reporting cancellation or cleanup complete.

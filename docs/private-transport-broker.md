# Private transport broker

The internal broker is a dormant building block for [private CONNECT](https://github.com/SyneHQ/kelvo-go/issues/162). No CLI, configuration, worker, adapter or DBAPI route uses it. It does not yet implement an issuer client, CONNECT handshake or child IPC.

## Ownership

| Owner | Holds |
|---|---|
| Application authority | Source-to-tunnel mapping, current source revision, execution authority and ticket signing key |
| Kelvo parent | Proxy endpoint, verified mTLS identity, tickets and connection reservations |
| Native driver | Database credentials, database TLS and a dial capability for one admitted source |
| Rabbit | Registered tunnel routing, ticket consumption and short authority leases |

The parent freezes issuer, audience, cluster tenant, principal, application tenant, source revision, original `host:port` and execution ID/grant/worker/owner/claim. A driver can dial only that exact TCP authority. It cannot choose another source or the proxy endpoint.

Each physical open gets a fresh 256-bit ID. A Rabbit opener must map it to a new one-use ticket and bind every scope field. It must validate the current route, authenticate the proxy, preserve buffered bytes and reject redirects or malformed responses. It must never retry or fall back to direct dialing.

## Capacity and cancellation

Configure maximum admitted source sessions, total data connections and data connections per session. Each session reserves `MaxDataPerSession` separate cancellation slots so its connections can cancel concurrently. Total physical reservations are bounded by `MaxDataConnections + MaxSessions * MaxDataPerSession`; there is no waiting queue. Each pool is capped at 65,536 connections; an invalid or overflowing reservation product is rejected before admission.

Data saturation cannot consume another source's cancellation slots. Cancellation uses the same authority and a fresh ticket, not a more privileged grant. Rabbit currently has no separate reserved cancellation lane, so broker capacity alone does not establish cancellation under end-to-end saturation.

The parent custody context remains valid through source cancellation and cleanup. Cancelling a query request must not prematurely close this context. Closing a session stops both types of open and closes its sockets; expired or revoked authority never permits cancellation to bypass authorization.

Opening, closing and uncertain connections keep their reservations. A close error or provider panic drains new data admission. Accepted sessions retain their cancellation slots while their custody remains valid. A bounded shutdown that cannot join cleanup returns an error and retains capacity.

## Connector integration gates

1. Add an authenticated issuer/CONNECT implementation in the trusted parent. Keep tickets and mTLS keys out of child input, environment and files.
2. Pass a per-execution inherited IPC capability to the owning child. Verify exact peer/process custody; an in-process Go interface is not a security boundary.
3. Wire supported native drivers explicitly. Keep the original database TLS hostname and bypass worker-side source DNS. Reject unsupported adapters.
4. Give PostgreSQL cancellation its reserved dial capability. `pgx` uses `RemoteAddr()` and its configured dial function; the broker exposes the original source authority, but selecting the cancellation capability still needs a driver hook.
5. Qualify verified source TLS, revocation, owner replacement, saturated cancellation, shutdown and cleanup against real PostgreSQL/MySQL and federation scans. Measure setup and transfer costs before making performance claims.

Current tests cover the internal scope and lifecycle contract, capacity isolation, distinct open IDs, late opens, cleanup failures and TCP half-close. They do not qualify a live Rabbit path.

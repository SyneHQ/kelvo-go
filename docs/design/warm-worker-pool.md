# Warm worker pool for small operations

Status: draft for review. Nothing in this document is implemented.

## Why

A production Kelvo node runs one job at a time, and every job costs about
2.6–3.3 s even when the query is tiny. A Postgres table browser opens 3–6
requests per click, so a single user already queues, and 100 users do not fit.
The target is 100–1000 concurrent small requests (a `SELECT … LIMIT 100`, a
table list, a table's columns) with a warm p95 well under one second.

### Where the time goes today

Measured from production audit rings and probes on 2026-10-11 (Oracle node,
db-api in AWS us-east, 313 ms round trip between them):

| step | cost | per job |
| --- | --- | --- |
| gateway submit | ~30 ms | yes |
| node → db-api resolver call (3 round trips: TCP + TLS + request) | ~0.9–1.0 s | yes |
| fresh credentials: DB lookup + 3 KMS decrypts | 100s of ms | yes |
| sandbox launcher + new process + cgroup job + scratch workspace | ~0.5 s (estimate) | yes |
| open a database session (TCP + TLS + auth) and ping | 0.2–0.5 s | yes |
| the query itself | ms | yes |
| result fetch | ~10 ms | yes |

The sandbox and session setup are paid per job. They are not the query. The
node also rejects, instead of queueing, a job that arrives while another runs
(`max_concurrent: 1`, one 320 MB job on a 950 MB host), which is what turned
parallel UI requests into 409/502 responses.

Phase 0 below replaces the estimates with measurements before anything else is
built.

## Current model (as of 3fd0728)

`internal/worker/operation_process.go` `ExecuteResolvedOperation` for every job:

1. reserves resources from the pool (320 MB class) and creates a scratch workspace;
2. fetches fresh credentials from the db-api resolver;
3. starts the Landlock launcher and `kelvo-adapter-go worker` inside a new
   cgroup job; credentials travel on stdin, the receipt on fd3, rows on stdout;
4. the adapter opens a database session, runs one operation, closes it, exits;
5. the parent verifies the receipt, tears down the cgroup and scratch.

Every property that matters for safety (no ambient authority, credentials only
on stdin, a receipt that proves the effect, containment) is enforced by this
per-job lifecycle. The pool must keep those properties, not trade them away.

## Design

### Session worker

A long-lived `kelvo-adapter-go serve` process bound to exactly one
`(cluster_tenant, app_team, connection_id, source_revision)`. It runs inside the
same Landlock launcher and its own cgroup. It holds one pooled database session
(`pgx`/`database/sql`) between requests and serves one operation at a time.

Concurrency comes from the number of workers, not from sharing a worker. This
keeps cancellation, memory accounting and failure containment per request.

### Protocol

Length-prefixed frames on a socketpair instead of one-shot stdin/stdout:

- `OPEN`: source description and credentials, sent once, with `valid_until`.
- `RUN`: operation id, request digest, signed grant claims, limits.
- `RESULT`: Arrow IPC chunks, then a final `RECEIPT` frame.
- `CANCEL`, `CLOSE`.

A worker that sees any malformed, unexpected or oversized frame exits.

### Security invariants (all must hold, each gets a test)

1. A worker serves one tenant, one team, one connection and one source
   revision. It never serves another, and its credentials never leave it.
2. Every `RUN` is authorized again by the node against the current grant and
   lease before it reaches the worker. The worker verifies the grant signature
   and that the request digest matches.
3. A revision change, credential expiry, tenant policy change or resolver
   denial evicts the worker. `valid_until` is short (60 s default).
4. A worker is killed, never reused, after any failed, rejected, cancelled,
   uncertain or timed-out job, any protocol error, and after `max_requests`.
5. The receipt still proves the effect. Mutations are not run on a warm worker
   in the first release: only `QueryRead`, `MetadataInspect`, `ConnectionTest`.
6. Per-request scratch is a fresh subdirectory, removed before the next request.
7. Memory is accounted per worker, not per job, and a worker over its limit is
   killed by the cgroup.
8. Idle workers exit after `idle_ttl` (30 s default) and release their memory.

### Fresh credentials without a round trip per job

Today every job calls the resolver. Instead the resolver issues a **session
grant** when a worker is opened: credentials plus `valid_until`, bound to
`(tenant, team, connection, revision)` and signed. Subsequent jobs on that
worker need only a cheap lease check, not a new resolver call and not new KMS
decrypts. This removes the largest per-job cost and keeps the property that a
revoked connection stops working within `valid_until`.

This needs a new db-api resolver endpoint (`resolve-session`). It is additive;
the existing per-job endpoint stays.

### Pool manager (node)

Keyed by the tuple above. Settings: `max_workers`, `max_workers_per_connection`,
`idle_ttl`, `max_requests`, per-worker memory (64 MB class for small reads,
the existing class for everything else). A job that finds no idle worker for its
key opens one if the memory budget allows, otherwise **waits in a bounded queue**
and fails with `RESOURCE_EXHAUSTED` only after the queue timeout. Large or
unbounded jobs keep the current one-shot path.

### Gateway

Queueing, not rejection: the gateway and node accept jobs up to a configured
depth and run them as workers free up. Retention and slot sizing are retuned for
interactive load (completed operations are dropped from the gateway after the
result is fetched rather than held for the full retention window).

### Capacity

A warm 64 MB worker holds an open session and serves one job in single-digit to
low tens of milliseconds. Rough sizing for a node with `M` GB available to
workers: `M × 1024 / 96` workers (64 MB limit plus runtime overhead). A 16 GB
node holds ~170 warm workers; with a typical 50–200 ms job that is hundreds to
low thousands of requests per second, bounded by database connection limits,
not by Kelvo. Per-connection worker caps protect customers' databases.

## Egress through a fixed address

Customers allow-list `140.245.31.20`. Kelvo nodes may run anywhere only if the
database traffic still leaves from that address.

Use a Tailscale exit node on the Oracle host:

- Oracle: `tailscale set --advertise-exit-node`, `net.ipv4.ip_forward=1`,
  approve the exit node in the tailnet (or an `autoApprovers` ACL).
- Kelvo node hosts: `tailscale set --exit-node=<oracle> --exit-node-allow-lan-access`.
  Tailnet traffic (db-api, gateway, resolver) stays direct; only internet-bound
  traffic, which is the customer database connection, goes through Oracle.
- Fail closed: a firewall rule on the node host allows the Kelvo service user to
  reach the internet only through `tailscale0`, so an exit-node outage breaks
  database connections instead of leaking from the host's own address.

Constraints:

- Node hosts must be dedicated VMs. An exit node replaces the default route, so
  it must never be set on a host that also serves public ingress or other
  applications (for example the Hakopod k3s host).
- Every database connection now takes an extra hop through Oracle. A warm worker
  pays it once per session, not per request, which is the main reason this
  design depends on the pool. Oracle's bandwidth and availability become the
  limit; run two exit nodes with the same address only if the cloud supports
  moving it, otherwise Oracle is a single point of failure.

## Phases

0. **Measure** (days). Spans for each step above in the node, exported with the
   audit record; a load generator for 1/10/100/1000 concurrent small requests.
   Quick wins found here (resolver connection reuse, resolver slot count, queue
   instead of reject) ship on their own.
1. **Session worker, read-only** (1–2 weeks). Protocol, worker `serve` mode, pool
   manager, `QueryRead` only, behind a per-deployment flag, off by default.
2. **Session grants** (1 week). `resolve-session` in db-api, lease-only checks.
3. **Metadata and connection test** on warm workers.
4. **Capacity and egress**: node fleet on dedicated VMs, exit node, fail-closed
   rule, load test at 1000 concurrent.
5. **Qualification**: extend the compatibility evidence with the gates below,
   then release through the existing upgrade process.

### Gates to add before release

- one worker never serves two tenants, teams, connections or revisions;
- revision change, credential expiry and resolver denial evict within one request;
- kill on every failure class; no reuse after an uncertain outcome;
- scratch isolation between consecutive requests;
- cgroup memory kill of a warm worker, with the node staying healthy;
- worker crash during a result stream yields an uncertain/failed receipt, never success;
- 1000 concurrent small requests: no errors, p95 target met, memory flat;
- exit-node outage: connections fail, no direct egress.

## Open questions

- Which engines get warm workers first? Postgres and MySQL families only, as
  the connection pool support is there.
- Per-connection worker cap default, and how it is configured for customers with
  low database connection limits.
- Whether session grants may be cached by the node across workers of one
  connection (cheaper, slightly longer revocation window).
- Whether the classic analytics query path (DuckDB) should also reuse workers.

# Warm worker pool for small operations

Status: Implemented in branch `feat/warm-worker-pool`, behind feature flag `warm_pool.enabled=false`.

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

## Implementation Details (feat/warm-worker-pool)

The implementation consists of the following components:

1. **Framed Protocol (`adapter/frame`, `adapters/go/internal/frame`)**:
   - Length-prefixed binary frames: `[type:1][length:4][payload:length]`.
   - Frame types: `Request` (carries `adapter.ProcessRequest`), `Result` (carries chunks of Arrow IPC bytes), `Receipt` (JSON-encoded `operations.Receipt`), `Error`, `Ping`, `Pong`, `Close`.
   - Strictly bounded sizes: `MaxRequestBytes` (3 MiB), `MaxResultBytes` (16 MiB), `MaxReceiptBytes` (64 KiB), `MaxErrorBytes` (4 KiB).
   - Strict decode: unknown types, oversized frames, or truncated reads immediately terminate the stream.

2. **Adapter Serve Mode (`kelvo-adapter-go serve`, `adapters/go/internal/runtime/serve.go`)**:
   - Long-lived worker process serving framed requests over stdin/stdout.
   - Wrapped with `SessionCache.Get/Put` keyed by `(tenant_id, app_team, connection_id, source_revision)`.
   - Read-only eligibility gate: only `QueryRead` and `MetadataInspect` are accepted. Mutations, migrations, ingestion, JDBC, and external file sources keep the one-shot path.
   - Invariant enforcement: on ANY error, cancelled context, timeout, or short read, the session is closed (not returned to cache) and the worker exits immediately.
   - Per-request deadline enforcement from `ExpiresAt` and `Limits.TimeoutMS`.
   - Ephemeral per-request scratch directory wiped immediately after execution.

3. **Node Pool Manager (`internal/worker/warm_pool.go`)**:
   - Manages warm worker lifecycle within the node executor.
   - Sandboxed identically to the one-shot path: Landlock launcher + cgroup job + per-worker memory cap (64 MB class).
   - Enforces idle TTL (default 30m) with background reaper.
   - Bounded queue with fail-fast when `queue_depth` is exceeded (`RESOURCE_EXHAUSTED`).
   - One connection per worker; requests for the same key serialize or spin up an additional worker up to `max_per_key`.
   - Clean drain on node shutdown.

4. **Per-Request Authorization**:
   - Every request passes the lease check, audit begin/finish, and operation-input fetch.
   - Session grant carries the source revision and credential expiry.
   - Revision changes or credential expirations evict workers immediately.
   - Credentials exist strictly in worker process memory; never persisted to disk, logs, receipts, or audit records.

---

## Operator Note

### Configuration
Configured in `node.yml` under `operations.warm_pool`:

```yaml
operations:
  warm_pool:
    enabled: false          # Feature flag: default false (must be explicitly enabled)
    idle_ttl: 30m           # Worker idle lifetime before termination (default: 30m)
    max_workers: 4          # Maximum total warm workers per node (default: 4)
    max_per_key: 1          # Maximum warm workers per (tenant, team, conn, rev) (default: 1)
    queue_depth: 32         # Maximum pending requests in queue before fail-fast (default: 32)
    queue_timeout: 10s      # Maximum wait time in queue before timeout (default: 10s)
    worker_memory_mb: 64    # Per-worker cgroup memory limit (default: 64 MB)
```

### Disabling
To disable, ensure `enabled: false` or omit the `warm_pool` block entirely. When disabled:
- Node behavior is 100% byte-for-byte identical to the classic one-shot path.
- No warm worker processes are spawned.
- All operations reserve resources and execute via the existing one-shot Landlock launcher.

### Observability & Metrics
The pool exposes the following counters on `WarmPoolMetrics`:
- `Hits`: count of requests served by an existing warm worker (skipping spawn + connect + TLS).
- `Misses`: count of requests requiring a cold worker spawn or waiting for a slot.
- `Spawns`: total warm worker processes spawned.
- `QueueWaitMS`: cumulative milliseconds spent waiting in the pool admission queue.
- `EvictionsTTL`: workers terminated due to idle TTL or credential expiration.
- `EvictionsRev`: workers evicted due to source revision changes.
- `EvictionsErr`: workers killed due to query errors, timeouts, or cancellations (poison prevention).
- `EvictionsDrain`: workers cleanly shut down during node drain.


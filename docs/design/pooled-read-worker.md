# Pooled read workers

Status: draft for review. Nothing here is built unless a section says so.

## Why

Ordinary reads (open a table, run a short `SELECT`) are the bulk of interactive
traffic. On 2026-10-12 the production node (2 vCPU, 951 MB, Mumbai) was driven by
a real dashboard while the caller (db-api) ran in US-East. Measurements, all from
that day:

| Observation | Value |
| --- | --- |
| One read, idle system, end to end | 4-5 s |
| Node execution of that read | 2.3-2.5 s |
| Of which: credential resolve | about 2.0 s |
| Of which: database connect, query, close | about 0.25 s |
| Of which: process start | 0.1-0.2 s |
| New db-api → gateway connections per read | about 8 |
| Memory per query job, 14-28 in parallel | 6-8 MB |
| Reads in parallel before the node saturates CPU | fewer than 28 |
| At 28 in parallel: node execution median / max | 6.3 s / 15.7 s |
| Same load, seen by the caller | 26 of 74 reads and 35 of 35 table inspections failed |

The failures were not memory. Each read is its own sandboxed process with its own
database connection, its own credential resolve, and several new TLS connections.
At a few dozen in parallel the two cores are full, everything slows down, and
five-second credentials and grants expire before they are used.

Raising limits does not fix this. Four workers queued; forty collapsed.

## What is wrong with the current read path

1. **One process per read.** `Executor.execute` (`internal/worker/worker.go`)
   launches a child for every query. The cost is CPU and latency, not memory.
2. **No connection reuse to the source.** A relational session consumes its pool
   on the first read (`adapters/go/connectors/relational/query.go`), and the
   query child exits after one statement.
3. **Credentials are resolved per read.** One resolve is a new TLS connection to
   the caller plus two lease-validation calls back to the gateway.
4. **No connection reuse between services.** Both the SDK and the node resolver
   disabled keep-alive. Addressed by #211; not yet released.
5. **Capacity is frozen into cluster metadata.** `policy.workers` and
   `policy.limits` are compared byte for byte with a NATS record at startup.
   Changing parallelism means deleting that record and re-initializing. The
   audit journal header also binds `max_pending`, which caps concurrent
   executions, so raising it needs a new journal.
6. **Every limiter is try-or-fail.** A burst becomes errors instead of a short
   wait. Addressed on the caller by db.api.go#60; the node and gateway still
   reject.
7. **Failures are not explained.** The audit journal records a category only.
   The caller logs a status code and no error code or message.

## Goals

- A read on a warm connection costs one query round trip to the source and no
  process start, no connect, no credential resolve.
- One customer connection can run 50 reads at once on the current 2 vCPU node.
- A burst above capacity waits briefly, then gets a clear retryable answer.
- Parallelism is a node setting that takes effect on restart.
- Isolation between tenants and between connections is unchanged.

## Non-goals

- Analytical and federated queries. They keep the DuckDB child and its limits.
- Writes, migrations, ingestion, file sources, JDBC and private-transport
  (tunnel) sources. They keep the one-shot operation path for now.
- Cross-node pooling or routing.

## Design

### One worker per connection, many reads per worker

A **read worker** is one sandboxed adapter process for one
(cluster tenant, app team, connection, source revision). It is launched exactly
like today's warm worker: the Landlock launcher with the `serve` entrypoint, a
delegated cgroup, the pinned adapter binary on fd 5 (#209).

Inside the worker:

- a pool of `pool_size` source connections (default 4), opened lazily;
- each request runs in its own goroutine on its own pooled connection, in a
  read-only transaction with the request's deadline and row/byte limits;
- a failed or cancelled request discards **its connection**, not the worker;
- a protocol error, a limit violation by the worker itself, or any sign of a
  confused worker kills the worker, as today.

The isolation unit moves from "one query" to "one tenant's connection". Two reads
of the same customer on the same database share a process. Reads of different
tenants, teams or connections never do. This is the one deliberate trade.

### Multiplexed frames

Today's serve protocol (`adapter/frame`) is one request at a time. It gains a
stream identifier:

| Frame | Change |
| --- | --- |
| `Request` | carries a stream ID chosen by the node |
| `Result`, `Receipt`, `Error` | carry the stream ID they answer |
| `Cancel` (new) | node → worker, cancels one stream |
| `Ping`, `Pong`, `Close` | unchanged, connection-level |

Per-stream byte and row limits are enforced by the node's reader exactly as the
single-stream reader does now. One slow consumer must not block the others: the
worker writes frames through a bounded per-stream buffer and a single writer.

### Node side

`WarmPoolManager` already keys workers and handles TTL, revision eviction, drain
and fallback. It changes from a `busy` flag to an in-flight count:

- `max_in_flight` per worker (default 16) instead of one request at a time;
- a bounded wait when a worker is full, then `RESOURCE_EXHAUSTED` with a retry
  hint;
- one resource reservation per **worker**, sized for the worker and its pool,
  not one reservation per read.

Native-mode reads on supported relational engines are routed to a read worker
from the query path. Everything else takes the existing path. If no worker can be
obtained and nothing was sent, the read falls back to the one-shot child, as the
warm pool does today.

### Credentials

A worker receives credentials when it opens a connection, not per read:

- the first read for a key resolves credentials once;
- later reads carry no secrets; the node still checks the caller's authority
  (principal, policy, delegation) for every read before it forwards a frame;
- the node re-validates the worker's authorization on an interval
  (`reauthorize_every`, default 60 s) and on every pool growth; a failed
  re-validation or a changed source revision kills the worker;
- idle workers end at `idle_ttl` (default 30 min).

This relaxes "credentials are fresh for every read" to "a connection opened with
valid credentials stays usable for at most `reauthorize_every` without a new
check". #209 already made the smaller version of this trade for warm workers.
It needs explicit security sign-off.

### Capacity

- `policy.workers` stops being the read concurrency. Read workers are sized by
  node settings: `read_workers.max_workers`, `max_in_flight`, `pool_size`,
  `idle_ttl`, `queue_wait`. They are not part of cluster metadata.
- The audit journal header stops binding `max_pending`, or the limit is derived
  at open time, so concurrency changes do not require a new journal.
- Reservations: one per worker. A node admits `max_workers` workers; each worker
  admits `max_in_flight` reads.

### Between the caller and Kelvo

- Connection reuse on both hops (#211).
- One lease validation per resolve instead of two, or none when the resolve
  response is already bound to the lease it was requested under. To be decided
  with the caller's owners.
- Later: a single streaming call for a transient read instead of submit, wait
  and fetch.

### Backpressure

Every admission point waits a bounded time before refusing, and a refusal says
when to retry. The caller already does this after db.api.go#60. The gateway and
node get the same behaviour for reads.

### Observability

- The node logs one line per failed read: code, message class, stage, duration.
  No SQL, no credentials.
- Stage timings (admission, resolve, connect, execute, drain) are exported as
  metrics by default for the read path.
- The caller logs the Kelvo error code with the status it returns.

## What is given up

| Property today | After |
| --- | --- |
| A read cannot affect another read | Reads on the same connection share a process; a crash fails that connection's in-flight reads |
| Credentials leave memory when the read ends | They stay in the worker until it ends |
| Every read re-authorizes source access | Caller authority per read; source authorization at most `reauthorize_every` old |
| Memory limit per read | Memory limit per worker |

## Phases

Each phase ends with a gate that runs under the real launcher against a real
PostgreSQL, like the two gates added in #209.

| Phase | Content | Gate |
| --- | --- | --- |
| 0 | Release #211 and db.api.go#60; caller logs error codes | Trace shows one gateway connection per burst; a 3x-capacity burst returns no errors |
| 1 | Stream IDs and `Cancel` in the frame protocol; concurrent serve loop; connection pool in the relational session | 50 concurrent reads on one worker; `pg_backend_pid()` shows at most `pool_size` backends; one failing read does not disturb the others |
| 2 | In-flight accounting in the pool manager; route native reads to read workers; fallback | Dashboard replay: 50 parallel reads on the 2 vCPU node, no failure, p50 and p95 recorded |
| 3 | Credentials once per connection; periodic re-authorization; revision eviction under load | Revoked or changed source is refused within `reauthorize_every` |
| 4 | Read capacity out of cluster metadata; audit journal no longer bound to `max_pending` | Capacity change by restart only |
| 5 | Single-call transient read between caller and gateway | Trace shows one request per read |

Phase 0 is independent and should ship first. Phases 1-2 deliver the bulk of the
gain. Phase 3 is the security-sensitive one.

## Targets

To be confirmed by the phase 2 gate, not promised here:

- 50 concurrent reads for one connection on the current node without failures;
- warm read latency dominated by the source round trip plus one caller round
  trip;
- CPU per read low enough that the two cores are not the limit at that load.

## Open questions

1. Is per-connection isolation acceptable for all tenants, or do some need the
   per-read process?
2. What is the right `reauthorize_every`, and who signs it off?
3. Should a worker's pool size follow the customer database's connection limit?
4. Does the transient read keep using the query API, or become a transient
   operation kind so that reads and metadata inspection share one path?
5. How are read workers accounted when several nodes serve one tenant?

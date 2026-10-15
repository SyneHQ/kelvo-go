# Pooled read workers

Status: draft for review. Nothing here is built unless a section says so.
Phase 1 (#215), phase 2 (#217) and phase 2b (see "Phase 2b as built") are implemented.

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

## Product workloads

Kelvo and db-api serve a DataGrip-style product: a database explorer, a query
console, charts and dashboards, and data-analysis jobs, across many sources and
tenants. These are different workloads and the engine has to treat them
differently. Today it has two paths, a transient query and a durable operation,
and the product's traffic is spread over them by accident of history.

| Workload | Shape | Needs | Right path |
| --- | --- | --- | --- |
| **Explorer**: schema tree, table list, columns, row preview (`LIMIT 50`), counts | Bursts of 20-50 tiny reads when a page opens; same connection; results small | Sub-second, no durability, never refused on a burst | Pooled read worker. Metadata inspection moves here too |
| **Console**: user-typed SQL, paged results, cancel | One or two at a time per session; results can be large; writes possible | Streaming results, prompt cancellation, writes with receipts | Reads: pooled worker. Writes and migrations: durable operation |
| **Dashboards and charts**: N panels, periodic refresh, many viewers of one dashboard | Parallel small aggregates; the same query repeats across viewers and refreshes | Seconds are fine; dedupe and cache identical queries; a dashboard must not starve the explorer | Pooled worker behind a result cache, or an accelerated snapshot |
| **Analysis jobs**: AI-driven query sequences, cross-source joins, exports | Minutes; parallel inside a job; federated through DuckDB | Durable, resumable, receipts, per-job memory, queued behind a fair scheduler | Durable operation with the DuckDB child, in its own capacity class |

Two consequences drive the rest of this document:

1. **Explorer and dashboard traffic is interactive and small; it must not share
   a process-per-query engine or a durable store with analysis jobs.** Today
   metadata inspection is a durable operation (`operation_submit`), which is why
   explorer page opens fill the operation store and fail with 409 under load.
2. **Capacity is per workload class, not one number.** Kelvo's admission pool
   already has classes (interactive, refresh, export). The read worker pool is
   the interactive class; analysis jobs and refreshes take the background class;
   neither can take the other's slots. Within a class, per-tenant and
   per-connection caps keep one dashboard from starving another tenant.

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

- Explorer and dashboard reads on a warm connection cost one query round trip
  to the source: no process start, no connect, no credential resolve.
- One customer connection can run 50 interactive reads at once on the current
  2 vCPU node; an explorer page open never produces an error from capacity.
- Metadata inspection leaves the durable path and shares the read worker.
- Identical dashboard queries within a short window are served once.
- Analysis jobs keep durability, receipts and per-job limits, in a capacity
  class that cannot delay interactive reads.
- A burst above capacity waits briefly, then gets a clear retryable answer.
- Parallelism per class is a node setting that takes effect on restart.
- Isolation between tenants and between connections is unchanged.

## Non-goals

- Changing how analysis jobs execute. Federated and analytical queries keep the
  DuckDB child, durability and per-job limits; this design only fences their
  capacity off from interactive reads.
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
| 2 | In-flight accounting in the pool manager; route native reads **and metadata inspection** to read workers; fallback | Explorer page replay: 50 parallel reads and inspections on the 2 vCPU node, no failure, p50 and p95 recorded |
| 2b | Admission classes: interactive (read workers) and background (durable operations, refreshes) with separate slots; per-tenant and per-connection caps | A 5-minute analysis job running at full background capacity does not change explorer p95 |
| 2c | Dashboard result cache: identical (tenant, connection, revision, SQL, params) within `cache_ttl` served from one execution | Ten viewers of one dashboard produce one source query per panel per refresh |
| 3 | Credentials once per connection; periodic re-authorization; revision eviction under load | Revoked or changed source is refused within `reauthorize_every` |
| 4 | Read capacity out of cluster metadata; audit journal no longer bound to `max_pending` | Capacity change by restart only |
| 5 | Single-call transient read between caller and gateway | Trace shows one request per read |

Phase 0 is independent and should ship first. Phases 1-2 deliver the bulk of the
gain. Phase 3 is the security-sensitive one.

## Phase 2 as built

- The pool manager counts reads in flight per worker up to `max_in_flight`
  (default 16). One demux reader per worker routes frames by stream ID; a
  burst for a launching worker shares that launch. A full worker makes a
  read wait up to `queue_timeout` (with `queue_depth`, default 64, waiting),
  then answers `RESOURCE_EXHAUSTED` with a retry hint; it does not fall back,
  since fifty one-shot children is the collapse this design avoids.
- A local failure (caller cancel, sink error, node-side row or byte limit, a
  consumer more than 8 MiB behind) abandons only its stream: the worker gets a
  Cancel, the stream's frames are dropped, and the worker is kept if the
  stream still ends with a Receipt (even a Completed one) within the 2s
  grace. Workers are killed only for protocol violations, terminal Errors,
  receipt mismatches, I/O errors, or an abandoned stream that does not end.
  Each stream buffers at most `warm_pool.stream_buffer_mb` (default 8 MiB)
  of unread results. The trade-off: a large result to a slow client fails
  fast with `RESOURCE_EXHAUSTED` instead of holding node memory for it.
  `max_in_flight` × `stream_buffer_mb` per worker must fit the per-job memory
  overhead (`resources.overhead_mb`); config validation checks it.
- A worker's launch (reservation, process start, Hello) runs under a
  pool-owned context bounded by its own 30s launch timeout: a caller that
  cancels only stops waiting, and others sharing the launch still get it.
- Native reads route to a read worker from `Executor.execute`, before the
  per-read reservation, for PostgreSQL-family sources whose DSN the adapter
  accepts and when the node's limits fit the adapter's (1M rows, 64 MiB).
  Otherwise, or when no worker can be obtained and nothing was sent, the read
  takes the one-shot child.
- Metadata inspection becomes a transient query: `query.Request.inspect`
  carries an `operations.MetadataSpec` for one native connection. It is signed
  like SQL (the digest gains an `inspect` key only when present) and never
  enters the operation ledger. It runs on the read worker; when no read worker
  can take it (pool off, source shape, limits, caller custody) it runs on a
  one-shot adapter, the path inspections took before. This answers open question 4 for inspection; db-api
  must switch its inspection calls to it.
- The gate replays an explorer page open (50 parallel reads and inspections)
  through `Executor.Execute`, the node's query entry. The node's job permits
  (`policy.workers`) still bound how many reach it at once; that is phase 4.

## Phase 2b as built

- Admission gains an `operation` class for durable operations (analysis jobs
  run as durable operations); with refresh and export it is background work.
  Query API reads, federated or native, and read workers stay interactive.
- `resources.separate_classes: true` makes the query reserve
  (`query_reserve_slots`, `_memory_mb`, `_scratch_mb`) the interactive share
  and the rest the background share. Background never had the reserve; now
  interactive work cannot take the background share either.
- Durable operations never use read workers: their class is decided first and
  is always background. Only interactive reads (inspections from the query
  path) may run on a read worker through the operation path.
- Validation: the interactive share must hold `warm_pool.max_workers` plus one
  slot for fallbacks and result downloads, and its memory one result download
  (3 x `max_result_bytes` + 2 MiB). With separate classes an interactive
  query must fit the reserve itself.
- Validation also requires the interactive memory to hold `max_workers` x
  (`worker_memory_mb` + `overhead_mb` + stream buffers) plus one result
  download.
- Caps inside a class, FIFO per tenant among waiters that fit: a release grants
  to the tenant's earliest waiters whose connection has room (a waiter held by
  a full connection never holds back the tenant's other connections), and at most `queue_depth` reads wait (interactive) or
  `max_concurrent` operations (background). A federated read has no single
  connection and is bounded by its tenant cap only. Interactive:
  `warm_pool.max_per_tenant` (default 64 reads in flight, at most
  max_workers x max_in_flight; it must exceed the largest burst of one user,
  about 42 reads for an explorer page open) and
  `max_per_connection` (default max_per_key x max_in_flight) apply to every
  interactive read, held until it ends, one-shot fallback included. The cap
  wait and a full-worker wait share one `queue_timeout`; a refusal after
  waiting is `RESOURCE_EXHAUSTED` with a retry hint equal to the whole wait,
  caps included; a refusal because the wait queue is full returns at once and
  hints `queue_timeout` without claiming a wait.
  Background: `operations.max_per_tenant` and `max_per_connection` (0 = none)
  are taken before an operation starts, waiting at most
  `operations.queue_wait` (default 10s); a refusal is rejected before start
  with a retryable `RESOURCE_EXHAUSTED`. A tenant is one app team of one
  cluster tenant at one issuer; a connection is counted under its tenant.

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
6. Dashboard cache: what `cache_ttl`, and does a viewer ever need to bypass it?
7. Console writes: keep the durable operation path, or add a lighter
   "interactive write" with a receipt but no 30-minute retention?

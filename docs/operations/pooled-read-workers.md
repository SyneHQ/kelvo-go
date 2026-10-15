# Pooled read workers: production configuration and rollout

Status: **measured on an integration build, not on released code.** The numbers come from
`scripts/pooled_read_load.py` (workflow `Pooled read load`) run against
`feature/pooled-read-worker-integration` (#223) with one measurement-only workaround
(`benchmarks/pooled-read-load/patches/`). Six product defects found by that run are open
(see [Open defects](#open-defects-that-gate-rollout)); every number below must be re-measured,
and the patch directory must be empty, before this is rolled out. No claim here is a promise.

## What was measured

| | |
| --- | --- |
| Traffic | 50 virtual users for 5 minutes through the gateway's single-call read (`client.QueryStream`), a signed delegation grant per request |
| Mix | 20 explorer users (a page open is 42 requests at once on one connection, then 15-45 s think time); 2 dashboards x 10 viewers (10 panels each, refresh every 30 s from a random phase, 5% explicit refresh); 9 console users (one unique query every 5-15 s); 1 background analysis job (durable operations, one after another, ~13 s each) |
| Sources | PostgreSQL 17 with 20 databases and 20 roles, so 20 customers and 20 workers; 5 teams of 4 connections; TLS (`verify-full`) |
| Network | 80 ms round trip to the source (a delaying TCP proxy, 40 ms each way) |
| Credentials | the real `resolver.Handler` and the real lease checks against the gateway, plus 300 ms per resolve |
| Node | real gateway, node, contained read workers, audit journals and a single-node TLS JetStream broker; the gateway and node run in one process inside a delegated systemd cgroup (`MemoryMax`, `CPUQuota`) |
| Node sizes | 2 vCPU / 1 GB (the 2026-10-12 node) and 4 vCPU / 8 GB |
| Runs | CI run 38783149779 (v3), 38784590203 (v4, repeat), 38785952871 (audit measurement). All tables below are v4 |

What the harness does **not** model: the tunnel to the source (only added round trip), a
dedicated node (it shares the 4-vCPU CI runner with the generator, PostgreSQL and the proxy: runner
CPU was 60-97% busy, so the 4 vCPU numbers are pessimistic), several nodes, customer databases
with real latency variance, and db-api's own behaviour. The resolver and lease client run in the
node's process here; in production they are db-api's cost, so node CPU is overstated by that
small share.

## Results (v4, single runs)

`w4` is the default pool (`max_workers: 4`), `w20` is `max_workers: 20` (one per customer).
Failed/sent counts include deadline expiry at 30 s and the load generator's own cap of 256
requests in flight (client-admission refusals), listed separately below.

| | 2 vCPU / 1 GB w4 | 2 vCPU / 1 GB w20 | 4 vCPU / 8 GB w4 | 4 vCPU / 8 GB w20 | 4 vCPU / 8 GB w20, source pool 16 |
| --- | --- | --- | --- | --- | --- |
| explorer reads failed / sent | 2083 / 2112 | 1370 / 2000 | 567 / 2016 | **43 / 2704** | 62 / 2704 |
| explorer inspections failed / sent | 3386 / 3432 | 2234 / 3250 | 890 / 3276 | **95 / 4394** | 98 / 4394 |
| dashboard panels failed / sent | 1217 / 1500 | 674 / 1620 | 149 / 1490 | **17 / 1800** | 24 / 1860 |
| page open (42 requests) p50 / p95 / p99 | 12.6 / 30 / 30 s | 13.7 / 30 / 30 s | 19.1 / 26.3 / 28.8 s | **5.8 / 9.3 / 11.1 s** | 3.4 / 8.2 / 9.9 s |
| explorer read p50 / p95 | 9.0 / 29.4 s | 8.1 / 24.6 s | 11.3 / 23.0 s | 3.5 / 7.4 s | 2.4 / 7.4 s |
| dashboard refresh (10 panels) p95 | 28.5 s | 23.4 s | 20.0 s | 6.2 s | 7.0 s |
| console p50 / p95 | 24.8 / 29.6 s | 7.5 / 24.8 s | 6.0 / 14.9 s | 1.0 / 4.3 s | 1.0 / 3.7 s |
| server capacity errors (`RESOURCE_EXHAUSTED`) | 2 | 2 | 15 | 0 | 0 |
| generator-cap refusals | 4506 | 3095 | 1498 | 123 | 185 |
| in flight, mean / peak | 226 / 275 | 182 / 266 | 189 / 263 | 106 / 260 | 83 / 271 |
| node CPU, mean cores / of limit | 1.91 / 95% | 1.56 / 78% | 2.58 / 65% | 1.52 / 38% | 1.50 / 38% |
| CPU-throttled periods | 2962 | 1956 | 0 | 0 | 0 |
| cgroup memory peak | 451 MiB | 660 MiB | 969 MiB | 1057 MiB | 897 MiB |
| RSS: gateway+node / all workers, peak (shared binary pages counted once per worker) | 338 / 257 MiB | 340 / 773 MiB | 342 / 1762 MiB | 348 / 1806 MiB | 344 / 1316 MiB |
| workers spawned / one-shot fallbacks | 4 / 1891 | 12 / 248 | 4 / 6535 | 20 / 129 | 20 / 0 |
| resolver calls | 849 | 697 | 4544 | 622 | 576 |
| dashboard: source queries per viewer refresh | 0.61 | 1.84 | 6.02 | 2.41 | 2.52 |
| source connections, max | 8 | 42 | 25 | 77 | 290 |

Reading the table:

* **No configuration met "no failures".** The best, 4 vCPU / 8 GB with 20 workers, had 0 server
  capacity errors but 43 of 2704 explorer reads failed (client cap and `UNAVAILABLE`) and a page open
  took 9.3 s at p95. Nothing here supports a claim of 50 users without errors.
* **2 vCPU / 1 GB does not carry this load**, with 4 or 20 workers, or at half the users (not
  in the table: 880 of 928 explorer reads failed). It ran at 78-95% of its CPU limit and was
  throttled 2000-3000 periods. Under queueing a node spends its CPU on polling and signature checks
  (see defect 5), which turns a backlog into a collapse.
* **`max_workers: 4` collapses on either size**, including 4 vCPU where CPU was 65% used: with 20
  customers the pool is full of other keys' workers, so most reads run one-shot (6535 fallbacks),
  and metadata inspections cannot (they need a worker).
* At 4 vCPU the gain from a larger source pool (4 to 16 connections per worker) is a lower
  median (page open 5.8 to 3.4 s, explorer read 3.5 to 2.4 s in v4; 5.6 to 3.1 s in v3) and the same p95,
  at 4x the source connections. At 80 ms round trip a read in a read-only transaction costs about 4
  round trips (idle explorer read p50 was 0.32 s), so a 4-connection pool serves about 12 reads per
  second per worker. The tail did not improve; do not read more into one repeat.
* Dashboards: ten viewers on one dashboard cost 1.8-2.5 source queries per refresh (10 without
  the cache) at a hit rate of 0.76-0.82, not 1.0, because viewers refresh from random phases and
  the TTL is 10 s.
* Source traffic was small: 99 MB from the source in 310 s (about 2.6 Mbit/s mean).

## Recommended production configuration

For a 4 vCPU / 8 GB node serving about 50 concurrent users over about 20 hot connections. Where a
value was not varied in the measurements it is the code default and says so. Every `warm_pool` key
sits under `operations:`; **`read_workers` is validated but not read by anything yet** (defect 3),
so the pool is configured only by `operations.warm_pool`.

```yaml
policy:                      # cluster metadata: changing these means re-initialising the cluster
  max_queries: 256           # queued + running jobs per tenant; measured peak in flight 270 with the cap at 256
  job_ttl: 2m
  lease_duration: 5s
  replicas: 3                # production brokers are replicated; the load test used 1
  workers: {a1: 64}          # node job permits for the query path. read_workers does NOT replace it
  limits:
    max_rows: 1000000        # reads route to read workers only when max_rows <= 1M ...
    max_bytes: 67108864      # ... and max_bytes <= 64 MiB. The default 256 MiB silently turns routing off
    timeout: 30s
    memory_mb: 32
    threads: 1
    max_temp_mb: 16

resources:
  max_concurrent: 32
  memory_mb: 7000
  baseline_mb: 500           # gateway + node process measured at ~340 MiB RSS
  overhead_mb: 64            # must be >= limits.memory_mb + containment native + parent overhead
  scratch_mb: 1024
  separate_classes: true     # analysis jobs cannot take the interactive share
  query_reserve_slots: 28    # the interactive share: read workers and native reads
  query_reserve_memory_mb: 5500
  query_reserve_scratch_mb: 768

containment:
  max_groups: 128            # >= max_workers + one-shot fallbacks; peak seen: 28 cgroups
containment_budget: {native_overhead_mb: 16, parent_overhead_mb: 16, max_processes: 64}

operations:
  max_concurrent: 2          # background analysis jobs; must be <= resources.max_concurrent and <= audit.max_pending
  max_downloads: 2
  max_result_bytes: 16777216 # 16 MiB is the maximum
  warm_pool:                 # the read workers
    enabled: true
    max_workers: 20          # >= hot connections (measured). 4 (default) collapsed. No LRU eviction yet
    max_per_key: 1           # default; one worker per connection
    max_in_flight: 16        # default; reads in flight per worker
    max_per_tenant: 32       # default; reads in flight per team
    max_per_connection: 16   # default max_per_key x max_in_flight
    queue_depth: 64          # default; reads waiting for a full worker
    queue_timeout: 10s       # default; then RESOURCE_EXHAUSTED, retry after 1 s
    idle_ttl: 10m            # default is 30m; shorter bounds memory until idle workers are evicted for new keys
    worker_memory_mb: 64     # default; a cap. Measured cgroup memory per worker is 21-27 MiB (profile-plain-read.md)
    reauthorize_every: 60s   # signed off by the owner: a revoked source is refused within 60 s
    cache_disabled: false
    cache_ttl: 10s           # signed off; refresh bypass via X-Refresh only (defect 2)
    cache_max_entry_bytes: 4194304
    cache_max_total_bytes: 67108864

read_workers:                # accepted and validated, currently inert; keep it equal to warm_pool
  max_workers: 20
  max_in_flight: 16
  pool_size: 4               # the adapter hard-codes 4 (relational PoolSize); see defect 3
  idle_ttl: 10m
  queue_wait: 2s

audit:                       # gateway and node, each its own journal directory
  max_entries: 524288        # see "Audit journal sizing"
  max_pending: 512           # >= operations.max_concurrent; covers queued starts and active receipts
  retention: 1h
  write_timeout: 2s

gateway:
  max_http_requests: 512     # measured peak in flight 270

connection_resolvers:
  <issuer>:
    timeout: 15s
    max_concurrent: 16       # one resolve costs the resolver ~300 ms: 16 slots allow ~50 resolves/s
```

db-api's client: `MaxConcurrent: 256` (the maximum) and `MaxControlConcurrent: 128`. The Go client refuses
immediately (`RESOURCE_EXHAUSTED`) when its own limit is full, which looks like a node capacity
error and is not one; the harness reports it as `CLIENT_ADMISSION_LIMIT`. Every run above hit it at 123-4506 requests.

The two states are separate on purpose: `separate_classes` and `query_reserve_*` split the admission
pool, `warm_pool.*` shapes the read workers inside the interactive share.

## Node sizing for 50 concurrent users

Derived from the 4 vCPU / 8 GB, 20 worker run (v4: 9014 single-call requests in 306 s, about 29
per second):

| Resource | Measured | Rule |
| --- | --- | --- |
| CPU | 1.52 cores mean, 52 ms of node CPU per request (CPU of the NATS broker and PostgreSQL excluded) | cores >= 2 x (requests per second x 0.05); about 3 cores at 30 requests per second. Plan **4 vCPU**. 2 vCPU ran at 78-95% of its limit and collapsed |
| Memory | gateway + node 340 MiB; each read worker 21-27 MiB of its own (anonymous: Go heap, stacks) plus a 42 MiB mapping of the 154 MB adapter binary that all workers share ([profile-plain-read.md](profile-plain-read.md)); cgroup peak 1.06 GiB with 20 workers, which also holds the page cache of the audit journals | 0.4 GiB + 22 MiB x (hot connections within `idle_ttl`) + 42 MiB. 20 workers: about 0.9 GiB; 100 workers: about 2.7 GiB |
| Source connections | 4 per worker, 77 at 20 workers | check the customer databases' `max_connections`; 16 per worker is 290 |
| Disk (audit) | 2 KiB per audit slot, preallocated | see below |

**Recommended node: 4 vCPU / 8 GB.** Memory is not what limits a node: 20 workers fit in 1 GiB and 100 in about 3 GiB; CPU is (see above). 4 GB is enough for the memory; 8 GB leaves room for fallback children, journals and growth. Take `max_workers` from the number of distinct connections active within `idle_ttl`, not from
users: idle workers are not evicted for new keys (defect 4), so a pool smaller than the hot set collapses.

### Workers per GB

A worker's own memory is 21-27 MiB (anonymous pages); the adapter binary's 42 MiB of resident pages are shared by all workers and
charged once ([profile-plain-read.md](profile-plain-read.md)); gateway and node are 340 MiB. With a quarter of the cgroup kept free:

| cgroup memory | Workers that fit in memory (24 MiB each) |
| --- | --- |
| 1 GB | 16 |
| 2 GB | 48 |
| 4 GB | 112 |
| 8 GB | 240 |

(workers = (0.75 x memory - 382 MiB) / 24 MiB.) Only 20 workers were run; the 0.9 GiB for 20 is extrapolated from 12 (cgroup peak 660 MiB) and
measured at 1.06 GiB at 20 including the journals' page cache. **The admission pool, not memory, is the limit that bites first:** each worker
reserves `limits.memory_mb + overhead_mb` (96 MiB logical with the 32 + 64 used here, four times what it uses), and the interactive share
admits `min(query_reserve_slots, query_reserve_memory_mb / 96)` of them: 28 with the recommended config. Raise `query_reserve_slots` and
`query_reserve_memory_mb` together to admit more (for example slots 64 and memory 6144 MiB on 8 GB; not tested), and `containment.max_groups` above the worker count
plus one-shot fallbacks (peak 28 cgroups seen). `warm_pool.max_workers` may not exceed `resources.max_concurrent`.

### Source connections per worker (`pool_size`)

A warm read costs 4 source round trips (2 after #229), so one worker, with a pool of P connections to a source at round trip R,
serves at most **P / (4 x R)** reads per second, and a burst of B reads on one connection takes about B x 4 x R / P:

| RTT to the source | Warm read | Pool 4: ceiling / 42-read page open | Pool 8 | Pool 16 | Pool 4 with 2 round trips (#229) |
| --- | --- | --- | --- | --- | --- |
| 20 ms | 80 ms | 50/s, 0.8 s | 100/s, 0.4 s | 200/s, 0.2 s | 100/s, 0.4 s |
| 40 ms | 160 ms | 25/s, 1.7 s | 50/s, 0.8 s | 100/s, 0.4 s | 50/s, 0.8 s |
| 80 ms (measured) | 320 ms | 12.5/s, 3.4 s | 25/s, 1.7 s | 50/s, 0.8 s | 25/s, 1.7 s |
| 100 ms | 400 ms | 10/s, 4.2 s | 20/s, 2.1 s | 40/s, 1.1 s | 20/s, 2.1 s |
| 160 ms | 640 ms | 6.3/s, 6.7 s | 12.5/s, 3.4 s | 25/s, 1.7 s | 12.5/s, 3.4 s |

The 80 ms row was measured: 12.1 reads/s from the adapter alone with 4 connections, 14.1 through the whole stack, page open 3.4 s. Pool 16
cut the explorer page-open median from 5.8 to 3.4 s in the mixed run (the tail did not move) and took the source from 77 to 290 connections.

Recommendation: size `pool_size` so a 42-read page open finishes in about 1.5 s: `pool = ceil(42 x turns x R / 1.5 s)`, which is **9 at 80 ms**, 12 at 100 ms and 18 at 160 ms (4
turns): use 8-16; do not go past the customer database's connection allowance (`pool_size x
workers` per source: 20 workers x 16 = 320). **Through the Tailscale exit node the RTT is
the sum of both legs**: Kelvo to the exit node plus the exit node to the source. An 80 ms exit-to-source leg plus 20 ms to the exit node is the 100 ms row:
12 connections for a 1.5 s page open. `pool_size` is not configurable yet (defect 3: the adapter constant is 4; the Phase 4 wiring adds
`--pool-size`), so until then the ceiling is the pool-4 column and a page open on a 100 ms path takes about 4 s.

### Audit journal sizing

Each journal is preallocated at 16 KiB + 2 KiB x `max_entries` (262,144 entries was a 512 MiB file) and **never
overwrites an unexpired entry**; when it is full the gateway answers 503 `Audit storage unavailable`. Measured at
9014 single-call requests plus 23 analysis operations: the **gateway journal used 17,662 entries (1.96 per request) and the node
journal 8,844 (0.98 per request)**. Size each journal at `requests per second x receipts per request x retention seconds`:

| Request rate | Gateway, retention 1 h | Node, retention 1 h |
| --- | --- | --- |
| 30/s | 212,000 entries (434 MiB) | 106,000 entries (217 MiB) |
| 100/s | 706,000 entries (1.4 GiB) | 353,000 entries (0.7 GiB) |
| the maximum | 1,048,576 entries (2 GiB), about 148 requests/s for 1 h | |

The 4096-entry journal first used in the harness was an early mistake of mine, not a measured threshold; the
sizes above come from the usage count, not from a failure. Alert on usage (see below) because the failure
mode is a hard stop for all traffic of that service.

## Deploy order and mixed versions

Deploy: **node (and its adapter), then the gateway, then db-api**.

* Node and adapter are a pair. The node pins the adapter binary by SHA-256 (`operations.adapter.sha256`), and frame
  protocol v2 (multiplexed streams) fails closed: a v1 adapter's Hello is refused with a version mismatch, so no
  read worker starts. Native reads then fall back to the one-shot path, and metadata inspections (which have no one-shot path) fail
  `UNAVAILABLE`. Ship the node binary and the adapter built from the same revision.
* db-api after the node. db-api's single-call read, `client.QueryStream`, needs `POST /v1/queries/stream`. Against a node without it the client returns
  `ErrStreamUnsupported` before anything is sent, and db-api falls back to submit, wait and fetch. The new `inspect` request field
  has no such fallback: send it only to nodes that accept it.
* db-api must send one query alias per saved connection (defect 6) and TLS (`sslmode=verify-full&connect_timeout=5`) DSNs from the resolver
  (the delegated query path rejects `sslmode=disable`; the operation path allows it).
* **Untested:** mixed gateway/node versions from this stack. `docs/rolling-upgrades.md` only authorizes tested pairs. Roll all gateways and
  nodes of a tenant as one step; do not run a new gateway against old nodes.
* A node restart ends its workers; credentials and cache are in memory. After a restart the first read per connection resolves again
  (300 ms resolve in the test, 2 s on 2026-10-12).

Verify after each step: `kelvo read failed` log lines (below), `GET /resources`, and a probe read through db-api per tenant.

## Rollback

1. **db-api first**: stop sending `inspect` requests and fall back to its previous metadata path. A node with the warm pool disabled
   (or an older node) answers inspections `UNAVAILABLE`, because read workers are the only way to run them.
2. Node: set `operations.warm_pool.enabled: false` and restart (reads return to one-shot children), or deploy the previous node *and* adapter together.
3. **Audit journal `max_pending`.** From #222 the journal header no longer binds `max_pending`, but a journal records the
   value in force **when it was created**. An older binary compares the header with its configuration (`reflect.DeepEqual`) and
   treats a difference as corruption, so after you changed `audit.max_pending` on the new version, a rolled-back binary will refuse
   to open the old journal. Record each journal's creation value (`max_pending` in the header, or the config of the first start)
   before you ever change it; on rollback set `audit.max_pending` back to that value, or start the old binary on a new, empty
   directory and keep the old one for reconciliation. Never delete a journal to get past the error.
4. `policy.workers` and `policy.limits` are pinned in cluster metadata. Do not change them as part of rollback; if you must,
   the metadata record has to be re-initialised.
5. Keep the previous security configuration current: rolling back a binary must not restore revoked keys or an older source policy.

## Metrics and alerts

Available now (node `GET /metrics` and `GET /resources`, gateway mTLS; log lines from the node). Thresholds are starting points chosen from these runs, not tuned values:

| Signal | Source | Alert (starting point from the measurements) |
| --- | --- | --- |
| Failed read, by code, class and stage | node log `kelvo read failed: code= class= stage= duration_ms=` | any `class=capacity`; `stage=resolve` or `validation` rate above 1% of reads |
| Job duration | `kelvo_job_duration_seconds` | p95 above the db-api timeout budget. `kelvo_job_queue_wait_seconds` is **not** a useful signal: it measures the node permit, 99.8% of jobs waited under 1 ms in the healthy run and 98.1% in the collapsed 2 vCPU run (99.3% under 1 s), because the waiting happens in the gateway and the pool |
| Rejections | `kelvo_jobs_rejected_total` | any increase |
| Credential resolve time | `kelvo_worker_resolve_seconds` | p95 over 1 s (300 ms was the test's resolve) |
| Admission pool | `GET /resources` | interactive share above 80% for 5 minutes |
| CPU throttling | cgroup `cpu.stat` `nr_throttled` on the node's unit | **any increase**: every collapsed run had 2000-3000; healthy runs had 0 |
| Memory | cgroup `memory.current` / `memory.peak` against `MemoryMax` | above 80% |
| Audit journal | no metric; compute from the request rate (about 2 gateway receipts and 1 node receipt per request) | projected fill above 70% of `max_entries` within `retention` |

**Not exported today, and needed:** warm pool counters (workers, spawns, hits, misses, one-shot fallbacks, evictions by
cause, authorizations, queue wait) and result cache hits. They exist in process (`WarmPoolMetrics`) and the load harness reads
them there; production cannot. Until they are exported, one-shot fallbacks are only visible as CPU and latency.
Alert on fallbacks above 1% of reads and on `workers == max_workers` with misses once they exist.

## The hosting change

Plan: run Kelvo next to db-api and keep the Oracle server only as a Tailscale exit node, so sources see one stable egress IP.

What it changes (reasoning from the measurements; the tunnel itself was not measured):

* **db-api to Kelvo becomes local.** On 2026-10-12 a read took 4-5 s, of which about 2 s was the credential resolve across the
  Mumbai-US-East link. The resolve and its two custody checks per read are now local calls, and with reauthorization
  every 60 s a warm connection pays them once a minute, not per read.
* **Kelvo to the source gains the exit node.** Every source packet takes the extra path, so the source round trip is
  RTT(Kelvo, exit node) + RTT(exit node, source). The test's 80 ms models that sum. A warm read costs about 4 source round
  trips (measured idle p50 0.32 s at 80 ms), a connection open several more, and with the adapter's fixed 4-connection pool a worker
  serves at most about 1/RTT reads per second (12 per second at 80 ms). Put the exit node close to the customers' databases, or
  raise the pool (defect 3) before moving: if source round trips dominate, doubling the round trip roughly doubles a page open (an estimate, not measured).
* **Bandwidth.** All result bytes cross the exit node: 99 MB in 310 s at the 50-user mix, about 0.3 MB/s mean (2.6 Mbit/s). Console pages
  and analysis exports dominate. Size the exit node's uplink for the largest simultaneous export, not the mean.
* Tailscale adds per-packet encryption on both ends; at this rate that is small next to the node's 52 ms of CPU per request, but
  it was not measured.
* The Oracle node stops running Kelvo, so its 951 MB limit no longer applies to workers; the new node is sized as above.

## Observations that are not defects of this stack

* **Unconstrained `NUMERIC` results fail.** `sum(amount)` or `avg(amount)` over `numeric` returned `SOURCE_FAILED` for every panel until the
  query cast to `numeric(18,2)`. The adapter fails explicitly on a type it cannot map (as AGENTS.md requires), but dashboards produce these
  aggregates constantly. Tracked separately; db-api or the user must cast for now.
* **The delegated query path accepts only `sslmode=verify-full&connect_timeout=5`** PostgreSQL DSNs, with the CA passed as the
  `tls_ca_pem` source option. Operations accept `sslmode=disable`.
* **Result cache TTL and viewers.** A 10 s TTL does not turn ten viewers into one query unless they refresh within the same window.

## Open defects that gate rollout

Evidence is in the artifacts of the runs above; owners are the pull requests that introduced them.

1. **Every read-worker read fails the client's receipt check** (`PROTOCOL_ERROR`): the job's `Stats.Batches` is never set on the read-worker path (#219).
   Unpatched, 386 of 388 requests failed. Measured with patch 0001.
2. **`bypass_cache` does not reach the cache** (#220, #219): the gateway sets `BypassCache`, the cache reads `Refresh`. In every run `bypass_cache_reaches_source: false`, `refresh_reaches_source: true`.
3. **`read_workers` is inert** (#222): not read by the warm pool; the adapter's pool size is a constant 4.
4. **Idle workers are not evicted for a new key** (read-path stack): a full pool sends new keys one-shot and fails their inspections.
5. **A waiting single-call read polls every 50 ms and re-verifies the signed grant each time** (#220, `Gateway.relayResults`): 47% of node CPU on the 2 vCPU runs.
6. **Workers are keyed by the request alias** (#217/#221): connections of one team that share an alias evict each other.

Before rollout: all six fixed, `benchmarks/pooled-read-load/patches/` empty, the full matrix rerun on the fixed stack with
0 server capacity errors and 0 client-cap refusals, the metrics gap above closed, and the mixed-version step tested.

## Running the test

Workflow `Pooled read load` (pull requests that touch the harness, or manually). Locally on Linux, as a non-root user with `sudo -n systemd-run`:

```sh
python3 scripts/pooled_read_load.py --profile 4c8g --duration 300 --output /tmp/run \
  --postgres-password-file pg.password --kelvo bin/kelvo --launcher bin/kelvo-landlock \
  --adapter bin/kelvo-adapter-go --cluster-test bin/cluster.test --loadgen bin/loadgen --latproxy bin/latproxy \
  --set warm_pool.max_workers=20
python3 scripts/pooled_read_load_summary.py /tmp/run/report.json
```

`--set key=value` overrides any scenario key (`benchmarks/pooled-read-load/scenarios/`). The report holds per-class latency, errors by
code, node CPU and RSS, workers, spawns, evictions, cache hit rate, source queries per dashboard refresh, resolver calls, the audit journal
use and a CPU profile (`node-cpu-top.txt`).

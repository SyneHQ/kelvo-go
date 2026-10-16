# Pooled read workers: production configuration and rollout

Status: **measured on the merged code** at `cargo` `ed1820f` to `2ba73e4`; this document is written against `cargo` **5a748f7**, where every product PR is merged. No patches (the harness's `patches/` directory is gone). The numbers come from
`scripts/pooled_read_load.py` (workflow `Pooled read load`) and `Pooled read profile`, both in this PR. The product fixes for the defects the first runs found are merged
(see [Product findings](#product-findings-and-their-status)). **Fixes merged after the measurements were not re-measured:** #234, #235, #238, #239, #241 and #237; see [Known issues](#known-issues).
Every number is from CI runners that share CPU with the load generator, PostgreSQL and the delay proxy; read them as measurements of this harness, not promises.

## What was measured

| | |
| --- | --- |
| Traffic | 50 virtual users for 5 minutes through the gateway's single-call read (`client.QueryStream`), a signed delegation grant per request |
| Mix | 20 explorer users (a page open is 42 requests at once on one connection, then 15-45 s think time); 2 dashboards x 10 viewers (10 panels each, refresh every 30 s from a random phase, 5% explicit refresh); 9 console users (one unique query every 5-15 s); 1 background analysis job (durable operations, one after another, ~13 s each) |
| Sources | PostgreSQL 17 with 20 databases and 20 roles, so 20 customers and 20 workers; 5 teams of 4 connections; TLS (`verify-full`) |
| Network | 80 ms round trip to the source (a delaying TCP proxy, 40 ms each way) |
| Credentials | the real `resolver.Handler` and the real lease checks against the gateway, plus 300 ms per resolve |
| Node | real gateway, node, contained read workers (the slim relational adapter), audit journals and a single-node TLS JetStream broker; the gateway and node run in one process inside a delegated systemd cgroup (`MemoryMax`, `CPUQuota`) |
| Node sizes | 2 vCPU / 1 GB (the 2026-10-12 node) and 4 vCPU / 8 GB |
| Runs | six matrix runs on the merged code: 38874114296, 38878205819, 38881519514, 38883939562, 38887246821, 38900878104 (this PR's workflow, one job per configuration; the last two repeat the pool-12 entries three times each; run 38900878104 is on head 3cf6e4b, and its workflow conclusion was `failure` only because the entries that fail their gate by design were then job-level failures, which the workflow no longer reports: the numbers are the per-job artifacts). Profile: 38878205691, 38881519648, 38883939553 (three runs); baseline before the fixes: 38784590203 (matrix v4) and 38793567524 / 38795023128 (profile) |

What the harness does **not** model: the tunnel to the source (only added round trip), a
dedicated node (it shares the CI runner with the generator, PostgreSQL and the proxy), several nodes, customer databases
with real latency variance, and db-api's own behaviour. The resolver and lease client run in the
node's process here; in production they are db-api's cost, so node CPU is slightly overstated.

## Results on the merged code

Run 38900878104 (the last). `w4` is `max_workers: 4`, `w20` is 20 (one per customer); "pool 4" and "pool 12" are `read_workers.pool_size`, the source connections per worker.
Failed/sent counts include deadline expiry at 30 s and the load generator's own cap of 256 requests in flight (client-admission refusals), listed separately.

| | 2 vCPU w4 | 2 vCPU w20 pool 4 | 2 vCPU w20 **pool 12** | 4 vCPU w4 | 4 vCPU w20 pool 4 | 4 vCPU w20 **pool 12** |
| --- | --- | --- | --- | --- | --- | --- |
| explorer reads failed / sent | 264 / 2496 | 1831 / 2384 | **0 / 2896** | 314 / 2448 | 2 / 2848 | **0 / 3088** |
| explorer inspections failed / sent | 518 / 4056 | 2968 / 3874 | **0 / 4706** | 601 / 3978 | 2 / 4628 | **0 / 5018** |
| dashboard panels failed / sent | 142 / 1640 | 1183 / 1580 | **0 / 1920** | 165 / 1700 | 0 / 1860 | **0 / 1970** |
| console failed / sent | 0 / 173 | 100 / 146 | 0 / 230 | 1 / 183 | 0 / 233 | 0 / 245 |
| page open (42 requests) p50 / p95 / p99 | 8.5 / 13.0 / 13.8 s | 4.8 / 30 / 30 s | 2.9 / 4.5 / 5.0 s | 8.3 / 12.8 / 17.4 s | 3.2 / 5.8 / 7.0 s | **1.4 / 2.9 / 3.3 s** |
| explorer read p50 / p95 / p99 | 6.4 / 11.5 / 12.8 s | 4.4 / 8.4 / 9.4 s | 1.9 / 4.0 / 4.7 s | 6.3 / 11.4 / 12.7 s | 1.9 / 4.4 / 5.7 s | **0.8 / 2.1 / 2.8 s** |
| dashboard panel p50 / p95 / p99 | 5.1 / 9.6 / 10.8 s | 1.5 / 6.6 / 7.3 s | 1.0 / 3.0 / 5.2 s | 4.8 / 9.9 / 13.1 s | 0.8 / 3.0 / 3.7 s | **0.3 / 1.1 / 1.9 s** |
| dashboard refresh (10 panels) p95 | 10.6 s | 7.1 s | 3.1 s | 10.2 s | 3.1 s | 1.1 s |
| console p50 / p95 / p99 | 5.6 / 9.1 / 10.7 s | 2.9 / 7.2 / 8.2 s | 1.3 / 3.4 / 4.1 s | 5.6 / 9.9 / 10.4 s | 0.6 / 3.5 / 3.9 s | 0.6 / 1.3 / 2.0 s |
| errors by code | client cap 829, `UNAVAILABLE` 94, capacity 1 | client cap 4040, deadline 2031, capacity 9, `UNAVAILABLE` 17 | none | client cap 983, `UNAVAILABLE` 98 | client cap 4 | none |
| in flight, mean / peak | 153 / 267 | 210 / 286 | 62 / 229 | 152 / 262 | 62 / 257 | 31 / 210 |
| node CPU, mean cores / of limit | 0.66 / 33% | 0.33 / 17% | 0.56 / 28% | 0.65 / 16% | 0.54 / 14% | 0.51 / 13% |
| CPU-throttled periods | 2 | 0 | 3 | 0 | 0 | 0 |
| cgroup memory peak | 477 MiB | 467 MiB | 608 MiB | 488 MiB | 733 MiB | 784 MiB |
| RSS: gateway+node / all workers, peak (shared pages counted per worker) | 345 / 455 MiB | 347 / 369 MiB | 344 / 380 MiB | 342 / 351 MiB | 344 / 557 MiB | 344 / 610 MiB |
| workers spawned / one-shot fallbacks | 329 / 0 | 49 / 0 | 190 / 0 | 315 / 0 | 19 / 0 | 20 / 0 |
| resolver calls | 613 | 140 | 632 | 603 | 622 | 659 |
| dashboard: source queries per viewer refresh (10 = no cache) | 2.87 | 0.63 | 2.68 | 2.22 | 2.63 | 3.11 |
| source connections, max | 17 | 38 | 131 | 14 | 74 | 219 |

### The same, before the fixes (v4, integration build with a measurement patch)

4 vCPU / 8 GB, 20 workers, pool 4: 43 of 2704 explorer reads failed, page open 5.8 / 9.3 / 11.1 s (p50 / p95 / p99), explorer read p50 3.5 s and p95 7.4 s, node CPU 1.52 cores (38% of the limit) and
52 ms of CPU per request; 2 vCPU: 78-95% of the CPU limit, 2000-3000 throttled periods, collapse. The pool-16 experiment of that run is superseded by the pool-12 column above.

### Repeats of the two target configurations

Single runs vary on a shared runner (the generator's 256-request cap is close to the peak in flight), so the pool-12 entries were run again. **Server-side capacity errors were 0 in every run listed.**

| | runs | clean (0 failed requests) | the others | p50 / p95 / p99 over all runs | node CPU, mean cores | cgroup memory peak |
| --- | --- | --- | --- | --- | --- | --- |
| 4 vCPU / 8 GB, 20 workers, pool 12 | 10 | **9** | 1 run: 38 generator-cap refusals of 10,010 requests (0.4%) | 744-827 ms / 1.88-2.28 s / 2.69-3.66 s | 0.26-0.52 | 779-816 MiB |
| 2 vCPU / 1 GB, 20 workers, pool 12 | 7 | **5** | 1 run: 53 generator-cap refusals; 1 run stalled: 758 requests hit the 30 s deadline, 1,539 hit the generator cap, with the node at 0.51 cores (cause not found) | 1.87-2.27 s / 3.94-4.52 s / 4.73-5.63 s | 0.35-0.60 | 579-624 MiB |

Reading the results:

* **The merged code carries 50 users on both sizes once `pool_size` is 12; the source connection pool is the limit, not CPU.** With pool 4, 20 workers fail on the 2 vCPU node (page-open p95 30 s in this run, the generator cap in every one) and
  are marginal on 4 vCPU (client-cap refusals in 5 of 6 runs, 4-59 of about 9,600 requests; 4 failed requests of 2,848 reads in this one). A worker with 4 source connections serves at most 4 / (3 x 80 ms) = 16.7 reads/s (see [Source connections per worker](#source-connections-per-worker-pool_size)); a 42-read page open on one connection then takes about 3 s before anything queues behind it.
* **CPU is no longer the limit.** The node used 0.5 cores at 50 users on 4 vCPU (v4: 1.52) and 0.35-0.6 cores on 2 vCPU (v4: 1.9, throttled thousands of periods). The cost per read fell from 40 ms to 6-9 ms of node CPU
  ([profile-plain-read.md](profile-plain-read.md)).
* **`max_workers: 4` collapsed with 20 customers (measured before #237).** The pool is full of other keys' workers: 300+ spawns, a miss rate of 17-19%, and a 503 `Worker unavailable` for 94-98 requests. Eviction worked (v4 ran 6535 one-shot fallbacks; then 0), but a pool smaller than the active connections thrashes. **#237** (merged, unit-tested, **not re-measured here**) evicts a worker for another key only after it has idled `evict_min_idle` (default 1 s), makes a read that finds no worker wait up to `queue_wait`, and then refuses it as **429 + `Retry-After`** (only for the capacity marker; limit errors stay as before), so this case is now a bounded refusal, not a 503. Most reads of 20 hot keys on 4 workers are still refused by design.
  **Never run `max_workers` below the number of connections active within `idle_ttl`.**
* **2 vCPU / 1 GB carries the mixed load in about 70% of runs** (5 of 7), at 2x the latency of the 4 vCPU node (page open p50 2.9 s against 1.4 s; read p95 4.0 s against 2.1 s), a peak memory of about 610 MiB of the 1 GiB, and without
  headroom (throttled periods in every run, one unexplained stall). It is a viable bridge for the current node, not a recommendation for the 50-user target.
* **At 80 ms round trip a warm read takes 243 ms at low load** (v4: 388 ms); the 42-read page open on 4 vCPU is 1.4 s p50 (v4: 5.8 s).
* Dashboards: ten viewers on one dashboard cost 2.1-3.1 source queries per refresh (10 without the cache) at a hit rate of 0.69-0.79, not 1.0, because viewers refresh from random phases and the TTL is 10 s.
* The generator's own cap: 256 requests in flight. Peak in flight reaches 210-290 even on the faster node because a page open is 42 requests at once; client-cap refusals are not server errors but are failures a user would see, so they are counted.

## Recommended production configuration

For a 4 vCPU / 8 GB node serving about 50 concurrent users over about 20 hot connections. Where a value was not varied in the measurements it is the code default and says so. Capacity of the read workers is set in
**`read_workers`** (node setting, restart to change); `operations.warm_pool` holds `enabled`, the cache, caps and the queue depth, and **must not set the same capacity fields** (a configuration error). The file is the scenario
`benchmarks/pooled-read-load/scenarios/4c8g.yaml`, validated by `TestPooledReadLoadScenariosAreValid` with the node's own checks.

```yaml
policy:                      # cluster metadata: changing these means re-initialising the cluster
  max_queries: 256           # queued + running jobs per tenant; measured peak in flight 290 with the cap at 256
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
  baseline_mb: 500           # gateway + node process measured at ~345 MiB RSS
  overhead_mb: 64            # must be >= limits.memory_mb + containment native + parent overhead, and >= max_in_flight x stream_buffer_mb
  scratch_mb: 1024
  separate_classes: true     # analysis jobs cannot take the interactive share
  query_reserve_slots: 28    # the interactive share: read workers and native reads; must exceed max_workers by one
  query_reserve_memory_mb: 5500   # must fit max_workers x (worker_memory_mb + overhead_mb + max_in_flight x stream_buffer_mb) + one result download
  query_reserve_scratch_mb: 768

containment:
  max_groups: 128            # >= max_workers + one-shot fallbacks; peak seen: 21 cgroups
containment_budget: {native_overhead_mb: 16, parent_overhead_mb: 16, max_processes: 64}

read_workers:                # capacity of the read workers (#222, #225, #228)
  max_workers: 20            # 1-64 (the read_workers maximum); >= connections active within idle_ttl. 4 refused or thrashed with 20 customers; <= resources.max_concurrent
  max_per_key: 1             # measured with 1; the default is 2 (a burst may scale out to a second worker)
  max_in_flight: 16          # default; reads in flight per worker
  pool_size: 12              # source connections per worker, 1-32. Unset sends no flag and the adapter keeps its default of 8 (4 before #237). pool_size x max_per_key <= max_source_connections. See "pool_size" below
  max_source_connections: 64 # default 64, at most 1024: the budget of ONE worker key (tenant, team, saved connection, database, schema, credential revision)
  idle_ttl: 30m              # default
  queue_wait: 10s            # default 2s; bounded wait for a worker slot before a retryable refusal (429 + Retry-After); must be < 30s (the launch timeout)
  evict_min_idle: 1s         # default min(1s, queue_wait/2); a worker must have idled this long before another connection may evict it; an explicit value must be below queue_wait
  stream_buffer_mb: 4        # default 8; max_in_flight x stream_buffer_mb must fit resources.overhead_mb
  relational_adapter:        # the slim adapter (kelvo-adapter-relational), pinned like operations.adapter
    binary: /opt/kelvo/releases/<release>/kelvo-adapter-relational
    sha256: <digest from the build>

operations:
  max_concurrent: 2          # background analysis jobs; must be <= resources.max_concurrent and <= audit.max_pending
  max_downloads: 2
  max_result_bytes: 16777216 # 16 MiB is the maximum
  adapter: {binary: ..., sha256: ...}   # the full adapter, for operations
  warm_pool:                 # the rest of the pool's settings; capacity fields are not set here
    enabled: true
    max_per_tenant: 32       # default 64; reads in flight per team
    max_per_connection: 16   # default max_per_key x max_in_flight
    queue_depth: 64          # default; reads waiting for a full worker
    worker_memory_mb: 64     # default; a cap. Measured per worker: 12 MiB anonymous at pool 4, cgroup 13 MiB with the slim adapter
    reauthorize_every: 60s   # signed off by the owner: a revoked source is refused within 60 s
    cache_disabled: false
    cache_ttl: 10s           # signed off; an explicit refresh bypasses it (BypassCache)
    cache_max_entry_bytes: 4194304
    cache_max_total_bytes: 67108864

audit:                       # gateway and node, each its own journal directory
  max_entries: 524288        # see "Audit journal sizing"
  max_pending: 512           # the node refuses to start below: gateway >= gateway.max_http_requests; node >= policy.workers[node] + operations.max_concurrent (+ exports.max_concurrent + exports.max_downloads). See below
  retention: 1h
  write_timeout: 2s          # at least 52 ms (#230's group commit)

gateway:
  max_http_requests: 512     # measured peak in flight 290

connection_resolvers:
  <issuer>:
    timeout: 15s
    max_concurrent: 16       # one resolve costs the resolver ~300 ms: 16 slots allow ~50 resolves/s
```

(The harness file puts the pool's fixed fields under `warm_pool:` at the top level of the scenario; in a node config they sit under `operations.warm_pool`.)

db-api's client: `MaxConcurrent: 256` (the maximum) and `MaxControlConcurrent: 128`. The Go client refuses
immediately (`RESOURCE_EXHAUSTED`) when its own limit is full, which looks like a node capacity
error and is not one; the harness reports it as `CLIENT_ADMISSION_LIMIT`. A page open of 42 requests plus dashboards reaches 210-290 in flight at the design load, so the cap is in reach in about 1 run of 10 on the 4 vCPU node.

The two states are separate on purpose: `separate_classes` and `query_reserve_*` split the admission pool, `read_workers` shapes the read workers inside the interactive share.

**CPU weights** (cgroup v2 `cpu.weight`, #234): a read worker 100 (the kernel default), a one-shot read child 50, a background operation 20 (`containment.CPUWeightReadWorker`, `CPUWeightOneShot`, `CPUWeightBackground`). Under CPU contention the long analysis operations yield to reads
without a quota; with spare CPU they use it. The load matrix above ran before this weighting merged, so the CPU numbers for the analysis job are not comparable to a release build.

## Node sizing for 50 concurrent users

Derived from the 4 vCPU / 8 GB, 20 worker, pool 12 runs (about 10,000 single-call requests in 306 s, about 33 per second):

| Resource | Measured | Rule |
| --- | --- | --- |
| CPU | 0.26-0.52 cores mean, about 15 ms of node CPU per request including the share of the gateway (CPU of the NATS broker and PostgreSQL excluded; the load generator, proxy and PostgreSQL used another 0.1-0.3 cores of the runner) | cores >= 4 x (requests per second x 0.015); about 2 cores at 30 requests per second keeps the node below 30% busy. The 2 vCPU node ran at 28% busy but with 3-11 throttled periods in every run, so plan **4 vCPU** for headroom |
| Memory | gateway + node 345 MiB; each read worker on the slim adapter 12 MiB anonymous at pool 4 and about 19 MiB at pool 12 (RSS 30 MiB with shared binary pages counted per worker); cgroup peak 780-816 MiB with 20 workers at pool 12, which also holds the page cache of the audit journals | 0.4 GiB + 20 MiB x (hot connections within `idle_ttl`) + 20 MiB. 20 workers: about 0.8 GiB; 64 workers (the maximum): about 1.7 GiB |
| Source connections | `pool_size` per worker: 219 backends at 20 workers x 12 | the node bounds **`pool_size x max_per_key` per worker key** by `max_source_connections` (default 64); it does not bound the total across databases or, for keys that share one database (different team, schema or credential revision, or the old and new revision during a rotation), the total per database: budget each source database's `max_connections` for active keys x `pool_size x max_per_key` |
| Disk (audit) | 2 KiB per audit slot, preallocated | see below |

**Recommended node: 4 vCPU / 8 GB** with 20 workers and `pool_size` 12. Memory is not what limits a node: 20 workers fit in 0.8 GiB and the maximum of 64 in about 1.7 GiB. The 2 vCPU / 1 GB node carried the same configuration in 5 of 7 runs at about twice the latency; use it as a bridge only (see the upgrade plan).
Take `max_workers` from the number of distinct connections active within `idle_ttl`, not from users.

### Workers per GB

A worker's own memory is about 12 MiB (pool 4) to 19 MiB (pool 12) of anonymous pages with the slim adapter; the binary's resident pages (16.6 MB file, 11 MiB resident) are shared by all workers and
charged once ([profile-plain-read.md](profile-plain-read.md)); gateway and node are 345 MiB. With a quarter of the cgroup kept free and 24 MiB per worker (conservative: above the measured 19 MiB at pool 12):

| cgroup memory | Workers that fit in memory (24 MiB each) |
| --- | --- |
| 1 GB | 16 |
| 2 GB | 48 |
| 4 GB | 64 (memory would allow 112) |
| 8 GB | 64 (memory would allow 240) |

(workers = min(64, (0.75 x memory - 382 MiB) / 24 MiB); **`read_workers.max_workers` is at most 64** and at most `resources.max_concurrent`, so larger memory sizes do not admit more workers.) Measured: 20 workers at pool 12 used a cgroup peak of 784-816 MiB on 4 vCPU and 580-624 MiB on 2 vCPU/1 GB, including the journals' page cache. **The admission pool, not memory, is the limit that bites first:** each worker
reserves `limits.memory_mb + overhead_mb + max_in_flight x stream_buffer_mb` of the interactive share (32 + 64 + 64 = 160 MiB logical with the values here, several times what it uses; node validation checks that all `max_workers` fit with a result download), and the interactive share
admits `min(query_reserve_slots, query_reserve_memory_mb / 160)` of them. Raise `query_reserve_slots` and
`query_reserve_memory_mb` together to admit more, and `containment.max_groups` above the worker count
plus one-shot fallbacks (peak 21 cgroups seen). `read_workers.max_workers` may not exceed `resources.max_concurrent`.

### Source connections per worker (`pool_size`)

A source connection stays busy for **3 round trips per warm read**: 2 on the critical path (the client waits for them) and the background `ROLLBACK`, which #229 took off the critical path but which still has to finish before the connection serves the next read. (Before #229 it was 4.) So one worker with a pool of P connections to a source at round trip R
serves at most **P / (3 x R)** reads per second, and a burst of B reads on one connection takes about B x 3 x R / P:

| RTT to the source | Warm read, client waits (2 turns) | Pool 4: ceiling / 42-read page open | Pool 8 | Pool 12 | Pool 16 |
| --- | --- | --- | --- | --- | --- |
| 20 ms | 40 ms | 67/s, 0.6 s | 133/s, 0.3 s | 200/s, 0.2 s | 267/s, 0.2 s |
| 40 ms | 80 ms | 33/s, 1.3 s | 67/s, 0.6 s | 100/s, 0.4 s | 133/s, 0.3 s |
| 80 ms (measured) | 160 ms | 16.7/s, 2.5 s | 33/s, 1.3 s | 50/s, 0.8 s | 67/s, 0.6 s |
| 100 ms | 200 ms | 13/s, 3.2 s | 27/s, 1.6 s | 40/s, 1.1 s | 53/s, 0.8 s |
| 160 ms | 320 ms | 8.3/s, 5.0 s | 17/s, 2.5 s | 25/s, 1.7 s | 33/s, 1.3 s |

Measured at 80 ms with pool 4: **16.1 reads/s** at 50 concurrent on one connection (profile, three runs: 16.0-16.1), against the 16.7/s ceiling; a warm read is 243 ms at low load. In the mixed run, pool 4 against pool 12 on the 4 vCPU node: page-open p50 3.2 s against 1.4 s, p95 5.8 s against 2.9 s, explorer read p95 4.4 s against 2.1 s, and failed requests 4 against 0. On the 2 vCPU node the pool is the difference between failing and (mostly) passing.

**Default after #237:** an unset `pool_size` leaves the adapter's default of **8** (it was 4 when this was measured); the per-worker ceiling at 80 ms is then 33 reads/s and a 42-read page open takes about 1.3 s idle. The pool-12 numbers above are with the value set explicitly; the pool-4 numbers with 4 set explicitly; **8 was not measured**.

Recommendation: **`pool_size` 12 at 80-100 ms round trip** (the measured value, 1.4 s page open); `pool = ceil(42 x 3 x R / 1.5 s)` gives 7 at 80 ms, 9 at 100 ms and 14 at 160 ms for a 1.5 s page open, and the measured 12 leaves room for bursts of several users on
one connection. **Through the Tailscale exit node the RTT is the sum of both legs**: Kelvo to the exit node plus the exit node to the source. An 80 ms exit-to-source leg plus 20 ms to the exit node is the 100 ms row: pool 12 (page open about 1.1 s idle). Do not go past the customer database's connection allowance. **The node's budget is per worker key** (#237): `pool_size x max_per_key <= read_workers.max_source_connections` (default 64, at most 1024; unset `pool_size` counts as 8; 20 workers x pool 16 is accepted, `max_per_key` 8 with `pool_size` 32 needs `max_source_connections` 256). It is not a per-database or per-node total: keys that differ only in team, schema or credential revision reach the same database and each hold up to that many connections, and a credential rotation briefly runs the old and new key's workers together. Run `kelvo node --check-config` before restarting; a configuration that was valid before can be refused.

### Audit journal sizing

Each journal is preallocated at 16 KiB + 2 KiB x `max_entries` (262,144 entries was a 512 MiB file) and **never
overwrites an unexpired entry**. Measured at about 10,000 single-call requests plus 22 analysis operations in 306 s: the **gateway journal used 19,935 entries (1.99 per request) and the node
journal 9,980 (1.0 per request)**. Size each journal at `requests per second x receipts per request x retention seconds`:

| Request rate | Gateway, retention 1 h | Node, retention 1 h |
| --- | --- | --- |
| 30/s | 215,000 entries (440 MiB) | 108,000 entries (220 MiB) |
| 100/s | 716,000 entries (1.4 GiB) | 360,000 entries (0.7 GiB) |
| the maximum | 1,048,576 entries (2 GiB), about 146 requests/s for 1 h | |

**A full journal and a full pending queue are different.** A full journal stops all traffic of that service. A full *pending queue* (`audit.max_pending`, admitted work waiting for its receipt) used to answer 503 `Audit storage unavailable`, which looked like an outage;
**it is now capacity, a 429 (#236), on every audit path (#239, #241)**. In the first run on the first-release config (`max_pending` 64, before #236) 345 requests got the 503 at a peak of 103 in flight; with 256 the 503 did not recur.
**The node enforces the minimum at load** (`audit.max_pending must be at least the work the service admits at once`; `docs/cluster.md`): the same `audit.max_pending` value is used by both services, so it must satisfy both rules:

| Service | Minimum `audit.max_pending` | This runbook's values |
| --- | --- | --- |
| Gateway | `gateway.max_http_requests` | 512 (target), 64 (first release) |
| Node | `policy.workers[<this node>]` + `operations.max_concurrent` (+ `exports.max_concurrent` + `exports.max_downloads` when exports are configured) | 64 + 2 = 66 (target), 16 + 1 = 17 (first release) |

Use at least the larger of the two (512 and 256 here). An existing node with exports and a smaller value must raise it before upgrading. Alert on usage (see below) because a full journal is a hard stop.

## Deploy order and mixed versions

Deploy: **node (and its adapters), then the gateway, then db-api.** The step-by-step plan, with probes and rollback, is [pooled-read-workers-upgrade.md](pooled-read-workers-upgrade.md).

* Node and adapter are a pair. The node pins the adapter binary by SHA-256 (`operations.adapter.sha256`, and `read_workers.relational_adapter.sha256` for the slim one), and frame
  protocol v2 (multiplexed streams) fails closed: a v1 adapter's Hello is refused with a version mismatch, so no
  read worker starts. Native reads then fall back to the one-shot path, and metadata inspections (which have no one-shot path) fail
  `UNAVAILABLE`. Ship the node binary, the launcher and the adapters built from the same revision. A slim adapter that is configured but unusable falls back to the full adapter.
* db-api after the node. db-api's single-call read, `client.QueryStream`, needs `POST /v1/queries/stream`. Against a node without it the client returns
  `ErrStreamUnsupported` before anything is sent, and db-api falls back to submit, wait and fetch. The `inspect` request field
  has no such fallback: send it only to nodes that accept it.
* db-api sends TLS (`sslmode=verify-full&connect_timeout=5`) DSNs from the resolver
  (the delegated query path rejects `sslmode=disable`; the operation path allows it). Workers are keyed by the saved connection since #221 (the request alias no longer matters; the shared-alias evidence run has 0 errors).
* **Untested:** mixed gateway/node versions from this stack. `docs/rolling-upgrades.md` only authorizes tested pairs. Roll all gateways and
  nodes of a tenant as one step; do not run a new gateway against old nodes.
* A node restart ends its workers; credentials and cache are in memory. After a restart the first read per connection resolves again
  (300 ms resolve in the test, 2 s on 2026-10-12).

Verify after each step: `kelvo read failed` log lines (below), `GET /resources`, and a probe read through db-api per tenant.

## Rollback

1. **db-api first**: stop sending `inspect` requests and fall back to its previous metadata path. A node with the warm pool disabled
   (or an older node) answers inspections `UNAVAILABLE`, because read workers are the only way to run them.
2. Node, without a binary change: set `operations.warm_pool.enabled: false` **and remove the whole `read_workers` block**, then restart (reads return to one-shot children). `enabled: false` alone makes the new node refuse to start once
   `read_workers` is present (`read_workers requires operations.warm_pool enabled, and capacity set only in read_workers`). Or deploy the previous node *and* adapter together (the binary rollback of the upgrade plan, which must be done before any migration step that the old binary cannot read).
3. **Audit journal `max_pending`.** From #222 the journal header no longer binds `max_pending`, but a journal records the
   value in force **when it was created**. An older binary compares the header with its configuration (`reflect.DeepEqual`) and
   treats a difference as corruption, so after you changed `audit.max_pending` on the new version, a rolled-back binary will refuse
   to open the old journal. Record each journal's creation value (`max_pending` in the header, or the config of the first start)
   before you ever change it; on rollback set `audit.max_pending` back to that value, or start the old binary on a new, empty
   directory and keep the old one for reconciliation. **Keep the old journal directory (both gateway and node) until the rollback window closes**: a rollback needs it, and a journal is never recreated from the new one. Never delete a journal to get past the error.
4. **Before any apply, back up** the node and gateway configs, every audit journal directory (and its recorded creation `max_pending`), the containment state directory and the NATS state (JetStream store, consumer state and the metadata record), into a location outside the release directory. A rollback or a recovery after a failed worker execution restores from it, not from memory.
5. `policy.workers` and `policy.limits` are pinned in cluster metadata. Do not change them as part of rollback; if you must,
   the metadata record has to be re-initialised.
6. Keep the previous security configuration current: rolling back a binary must not restore revoked keys or an older source policy.

## Metrics and alerts

Available now (node `GET /metrics` and `GET /resources`, gateway mTLS; log lines from the node). Thresholds are starting points chosen from these runs, not tuned values:

| Signal | Source | Alert (starting point from the measurements) |
| --- | --- | --- |
| Failed read, by code, class and stage | node log `kelvo read failed: code= class= stage= duration_ms=` | any `class=capacity`; `stage=resolve` or `validation` rate above 1% of reads |
| Job duration | `kelvo_job_duration_seconds` | p95 above the db-api timeout budget. `kelvo_job_queue_wait_seconds` is **not** a useful signal: it measures the node permit and was under 1 ms for nearly all jobs even in collapsed runs, because the waiting happens in the gateway and the pool |
| Rejections | `kelvo_jobs_rejected_total` | any increase |
| Credential resolve time | `kelvo_worker_resolve_seconds` | p95 over 1 s (300 ms was the test's resolve) |
| Admission pool | `GET /resources` | interactive share above 80% for 5 minutes |
| CPU throttling | cgroup `cpu.stat` `nr_throttled` on the node's unit | **any sustained increase**: 3-11 periods per 5 minutes were seen on the healthy 2 vCPU runs, 0 on 4 vCPU; the collapsed v4 runs had 2000-3000 |
| Memory | cgroup `memory.current` / `memory.peak` against `MemoryMax` | above 80% |
| Audit journal | no metric; compute from the request rate (about 2 gateway receipts and 1 node receipt per request) | projected fill above 70% of `max_entries` within `retention` |

**Not exported today, and needed:** warm pool counters (workers, spawns, hits, misses, one-shot fallbacks, evictions by
cause, authorizations, queue wait) and result cache hits. They exist in process (`WarmPoolMetrics`) and the load harness reads
them there; production cannot. Until they are exported, one-shot fallbacks and worker thrash are only visible as CPU, latency and 429 capacity refusals (503 `Worker unavailable` before #237).
Alert on fallbacks above 1% of reads and on `workers == max_workers` with misses once they exist. The slim adapter has `kelvo_read_adapter_slim_active` and `kelvo_read_adapter_slim_fallbacks_total`.

## The hosting change

Plan: run Kelvo next to db-api and keep the Oracle server only as a Tailscale exit node, so sources see one stable egress IP.

What it changes (reasoning from the measurements; the tunnel itself was not measured):

* **db-api to Kelvo becomes local.** On 2026-10-12 a read took 4-5 s, of which about 2 s was the credential resolve across the
  Mumbai-US-East link. The resolve and its two custody checks per read are now local calls, and with reauthorization
  every 60 s a warm connection pays them once a minute, not per read.
* **Kelvo to the source gains the exit node.** Every source packet takes the extra path, so the source round trip is
  RTT(Kelvo, exit node) + RTT(exit node, source). The test's 80 ms models that sum. A warm read keeps a source connection for 3 round
  trips (2 the client waits for; measured idle p50 0.24 s at 80 ms), a connection open several more, and a worker with `pool_size` P
  serves at most P / (3 x RTT) reads per second (50 per second at pool 12 and 80 ms; 40 at 100 ms). Put the exit node close to the customers' databases and set `pool_size` from the table above
  before the move: if source round trips dominate, doubling the round trip roughly doubles a page open (an estimate, not measured).
* **Bandwidth.** All result bytes cross the exit node: the 50-user mix moved about 0.3 MB/s mean (2.6 Mbit/s) in earlier runs. Console pages
  and analysis exports dominate. Size the exit node's uplink for the largest simultaneous export, not the mean.
* Tailscale adds per-packet encryption on both ends; at this rate that is small next to the node's 15 ms of CPU per request, but
  it was not measured.
* The Oracle node stops running Kelvo, so its 951 MB limit no longer applies to workers; the new node is sized as above. **Never delete the Oracle Kelvo install without a verified backup** (config, journals, containment and NATS state; see Rollback).
* **Lockout risk of `tailscale set --exit-node`:** routing all traffic through the exit node can cut the SSH or console session that ran the command. Keep console or out-of-band access, use `--exit-node-allow-lan-access=true`, and test the return paths (SSH and the monitoring and deploy paths) from a second session before leaving the change in place.

## Observations that are not defects of this stack

* **The delegated query path accepts only `sslmode=verify-full&connect_timeout=5`** PostgreSQL DSNs, with the CA passed as the
  `tls_ca_pem` source option. Operations accept `sslmode=disable`.
* **Result cache TTL and viewers.** A 10 s TTL does not turn ten viewers into one query unless they refresh within the same window (2.1-3.1 source queries per refresh with ten viewers).
* **Source turns per read are 3, not 2.** The third is the background `ROLLBACK` after the read; it is off the critical path, but its connection is not free until it finishes. Sizing uses 3.

## Product findings and their status

Evidence is in the artifacts of the runs above. "Fixed" means merged and, unless a row says "not re-measured", verified on the merged code by the runs listed.

| # | Finding (from the first runs) | Status |
| --- | --- | --- |
| 1 | Every read-worker read failed the client's receipt check (`PROTOCOL_ERROR`, `Stats.Batches` never set) | **Fixed** (the phase 2 and 3 stack). 0 `PROTOCOL_ERROR` on reads in every run; 2-3 on analysis operations in overload runs of the 6-user first-release entry only (not explained: the operation path under 429s) |
| 2 | `X-Refresh` / `bypass_cache` did not reach the cache | **Fixed** (7447f28: one `BypassCache` field). The probe confirmed that an explicit refresh reaches the source in 52 of 63 runs; the 11 misses (in all configurations, no pattern) were an explicit-refresh flake. **Fixed in #234** (a refresh never joins a running read), merged after the runs: **not re-measured** |
| 3 | `read_workers` settings were inert; the adapter pool was fixed at 4 | **Fixed** (#222, #225): `pool_size` reaches the adapter (`serve --pool-size`); measured in the pool-12 runs |
| 4 | Idle workers were not evicted for a new key | **Fixed**: 0 one-shot fallbacks in every run. A pool smaller than the active connections thrashed and answered 503 `Worker unavailable`; **#237 (merged, 5a748f7)** adds victim-age eviction (`evict_min_idle`), a bounded wait and a 429 + `Retry-After` refusal for capacity only: **not re-measured** |
| 5 | Waiting single-call reads polled every 50 ms and re-verified the grant | **Fixed** (#230): Ed25519 verifications per read 30 -> 4; node CPU per read 40 ms -> 6-9 ms |
| 6 | Workers keyed by the request's connection alias | **Fixed** (#221, 65bd44c): the shared-alias run has 0 errors and 5 spawns |
| 7 | Unconstrained `NUMERIC` results failed | **Fixed** (#226 exact decimal text; #227 bounded source codes) |
| 8 | A pending-queue-full audit journal answered 503 `Audit storage unavailable` | **Fixed**: #236 (429), #239 (capacity versus fault on every audit path, the node's `max_pending` minimum), #238 and #241 (their tests). The matrix ran before #239 |
| 9 | Adapter allocations per read rose from 89 KB to 152 KB (mallocs unchanged) | **Fixed in #235** (pools the 64 KiB result buffer), merged after the runs: the 152 KB figure is pre-#235 and **not re-measured** |
| 10 | A run on the 2 vCPU node stalled: 758 requests hit the 30 s deadline with the node at 0.5 cores and no server-side capacity error (1 of 7 runs of that configuration; a second run had 53 generator-cap refusals) | **Open, unexplained**: [issue 240](https://github.com/SyneHQ/kelvo-go/issues/240). The 4 vCPU node had no stall in 10 runs. The harness can capture goroutines and worker state when a run stalls (07f2e75 on this branch) |

Before rollout: the metrics gap above closed, the mixed-version step tested, and the matrix rerun on the release build with 0 server capacity errors.

## Known issues

* **2 vCPU / 1 GB stall** ([issue 240](https://github.com/SyneHQ/kelvo-go/issues/240)): 1 of 7 runs of 20 workers at `pool_size` 12 had every request fail from about t = 225 s with the node at 0.5 cores; cause unknown. Do not size the first release on the 5-of-7 clean runs.
* **Flaky test** ([issue 242](https://github.com/SyneHQ/kelvo-go/issues/242)): `TestPrivateRuntimeDataOpenUsesOperationContextAndConfiguredBudget` fails intermittently in `validate` under `-race` on a loaded runner. Unrelated to this stack; re-run, do not ignore a second failure.
* **Measured before the fixes that merged later.** All load and profile numbers here are from `cargo` at `ed1820f` to `2ba73e4`. They predate #234 (explicit refresh; CPU weights), #235 (the adapter result buffer: the 152 KB per read figure is pre-fix), #238, #239 and #241 (audit capacity on every path) and #237 (per-source connection budget, adapter pool default 8, victim-age eviction, 429 for capacity). A rerun on the release build is the release gate.
* **The gate is a release-candidate check, not a proof:** a pull request runs three entries once each. The 4 vCPU target had 1 run in 10 with 38 generator-cap refusals (0.4%), which the strict gate counts as a failure.
* **Unverified here:** the exit-node path (modelled as added delay only), mixed gateway and node versions, a multi-node cluster, and db-api's own behaviour.

## Running the test

Workflow `Pooled read load` (pull requests that touch the harness, or manually). Locally on Linux, as a non-root user with `sudo -n systemd-run`:

```sh
python3 scripts/pooled_read_load.py --profile 4c8g --duration 300 --output /tmp/run \
  --postgres-password-file pg.password --kelvo bin/kelvo --launcher bin/kelvo-landlock \
  --adapter bin/kelvo-adapter-go --relational-adapter bin/kelvo-adapter-relational \
  --cluster-test bin/cluster.test --loadgen bin/loadgen --latproxy bin/latproxy \
  --set read_workers.pool_size=12
python3 scripts/pooled_read_load_summary.py /tmp/run/report.json
```

Install the one Python dependency with `pip install pyyaml==6.0.2`. `--set key=value` overrides any scenario key (`benchmarks/pooled-read-load/scenarios/`). The report holds per-class latency, errors by
code, node CPU and RSS, workers, spawns, evictions, cache hit rate, source queries per dashboard refresh, resolver calls, the audit journal
use and a CPU profile (`node-cpu-top.txt`). The matrix is `benchmarks/pooled-read-load/matrix.json`. A pull request runs only the gated entries (`gated: true`: the 4 vCPU target, the three-user first-release load and the shared-alias evidence run); a manual dispatch runs all 14, including the undersized comparisons and the repeats, whose reports are published without failing the workflow. A gated entry fails on any failed request, any `DEADLINE_EXCEEDED` or `UNAVAILABLE`, any capacity error and on the scenario's latency ceilings (`gate:` in the scenario).

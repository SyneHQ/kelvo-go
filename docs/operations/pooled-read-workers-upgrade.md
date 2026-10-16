# Pooled read workers: production release and upgrade plan

Status: **plan only.** Nothing here has been run against production, and writing it touched no host, Terraform or Hakopod.
Facts about production (the Oracle VM, `/opt/kelvo/releases/<date>-<name>`, hash-pinned binaries, the root-owned
`terraform_data.upgrade` plan and apply with verifier receipts, db-api on Hakopod as `syne/db-api`) come from the request that
asked for this plan, not from this repository; steps that depend on them are marked. Marks used below:

* **[root]** needs root on the host or the infrastructure repo's root-owned apply.
* **[owner]** needs a decision by the service owner.
* **[no access]** depends on something this session cannot see or reach; the executor must supply or verify it.

Numbers come from [pooled-read-workers.md](pooled-read-workers.md) (load matrix) and [profile-plain-read.md](profile-plain-read.md), measured on the merged `cargo` code
(`ed1820f` to `2ba73e4`) with no patch, on CI runners. The plan is written against `cargo` **5a748f7**, where every product PR is merged. **Every threshold below must be re-validated on the release build** (section 4.1).

## 0. Where the code is today (2026-10-16; refresh before every use)

**Merged to `cargo`** (head `5a748f7` at the time of writing; the release SHA is whatever `cargo` is when the release is cut):

| PR | Commit(s) | What |
| --- | --- | --- |
| #212 | 1357e62 | design |
| #215 | 6a2d609 | phase 1: frame v2, multiplexed warm sessions |
| #217 | 0c41b44 | phase 2: in-flight multiplexing, routed reads and inspections |
| #218 | 7447f28 (with 2c) | admission classes; result cache; one `BypassCache` field |
| #220 | 77dec5f | phase 0 + 5: failure log, resolve metric, single-call read (`POST /v1/queries/stream`) |
| #221 | fa37ab8 (phase 3) | credentials once per connection, re-authorized every 60 s; workers keyed by saved connection (65bd44c) |
| #222 | dae3305 | `read_workers` settings; audit journal header no longer binds `max_pending` |
| #225 | 9410c79 | `read_workers` wired into the warm pool; `serve --pool-size` |
| #226, #227 | 22e34fa, 537cf3e | exact `NUMERIC` text; bounded source error codes |
| #228 | 164d0fd | slim relational adapter (`kelvo-adapter-relational`), `read_workers.relational_adapter` |
| #229 | ed1820f | fewer source round trips (2 on the critical path, 3 total with the background `ROLLBACK`) |
| #230 | 20aefc3 | control-plane diet: store, verified-grant cache, audit group commit |
| #231, #232 | 9db56ce, cf0b016 | tests |
| #234 | fa90c64, 4598412, 80f236a | an explicit refresh always executes; background CPU weighted below read workers (`cpu.weight` 100 read worker, 50 one-shot, 20 background) |
| #235 | eae8975 | pools the adapter's 64 KiB result buffer |
| #236 | 2ba73e4 | a full audit pending queue is capacity (429), not an outage (503); `max_pending` must cover admitted work |
| #238, #241 | 09ec963, 03af5a8 | audit tests made deterministic / pinning the gateway-side fixes |
| #237 | 1e2634d, cae5fdb, b0a35cd, 9ebb2e4, 7e80ef7, e21c61d, 6ffd117, 5a748f7 (head) | per-source connection budget (`pool_size x max_per_key <= max_source_connections`, default 64); the adapter's pool default is 8; a worker is evicted for another key only after idling `evict_min_idle`; capacity pressure is a 429 + `Retry-After`, not a 503 |
| #239 | cee0d3e, e2505e9 | audit capacity versus fault on every audit path; the node refuses a `max_pending` below the work it admits |

`git log --oneline --first-parent <previous release SHA>..RELEASE_SHA` on `cargo` lists the exact commits for the release record (rebase merges: each PR is its own commits). The harness (#224) is test-only and is not released.

**Merged after the measurements, not re-measured:** #234, #235, #238, #239, #241 and #237 (per-source connection budget, adapter pool default 8, victim-age eviction, 429 for capacity). The release gate is a rerun on the release build.

**Verified fixed on the merged code** by the matrix with `benchmarks/pooled-read-load/patches/` removed: every read passing the client receipt check, cache bypass wired, `read_workers` and `pool_size` effective, idle-worker eviction, no polling re-verification (Ed25519 verifications per read 30 -> 4), workers keyed by saved connection.
**Open:** a stall of the 2 vCPU / 1 GB node in 1 of 7 runs at the target load ([issue 240](https://github.com/SyneHQ/kelvo-go/issues/240); runbook finding 10). It does not affect the 4 vCPU target (0 stalls in 10 runs). See Known issues at the end of this document.

## 1. What ships

### 1.1 The release set

Four executables, all built from one commit `RELEASE_SHA` and pinned by SHA-256. They are a set: a node binary with another release's adapter or launcher is a mixed-version deployment (section 2).

| Artifact | Built from | Where it is used | Pinned in |
| --- | --- | --- | --- |
| `kelvo` (gateway and node) | `go build -trimpath -tags duckdb_arrow -ldflags "-s -w -X main.version=$RELEASE_SHA" ./cmd/kelvo`; with `DUCKBRIDGE=1` the bridge variant in the Dockerfile `build` stage | the node and gateway services | the release directory manifest |
| `kelvo-landlock` (launcher) | `cc -O2 -Wall -Wextra -Werror -std=c11 -o kelvo-landlock sandbox/launcher.c` | node `sandbox_path`; **must be this release's**: from #225 it accepts `serve --pool-size N` and an older one refuses the argument | the release directory manifest |
| `kelvo-adapter-go` (full adapter) | Dockerfile stage `adapter-build`: `go work` over the checkout and `adapters/go`, then `go build -mod=readonly -p 2 -tags duckdb_arrow -trimpath -ldflags "-s -w" ./cmd/kelvo-adapter-go` | durable operations; read workers when `read_workers.adapter: full` | `operations.adapter.sha256` |
| `kelvo-adapter-relational` (slim adapter, #228) | same source, `CGO_ENABLED=0 go build -mod=readonly -p 2 -tags slim -trimpath -ldflags "-s -w"`; 15.8 MiB against 147 MiB, 5.1 ms against 15.1 ms to Hello, no DuckDB, MongoDB, AWS SDK, go-ora, ClickHouse or gocql | read workers (default `read_workers.adapter: relational`) | `read_workers.relational_adapter.sha256` |

**[owner]** Which `kelvo` variant (`DUCKBRIDGE` 0 or 1) production runs now: **[no access]** read it from the running release's manifest, and build the same one. Do not change it in this release.

Commit list: `git log --oneline --first-parent <previous release SHA>..RELEASE_SHA` on `cargo`, attached to the release record with the PR numbers. Because the repository closes
pull requests by rebase merge, each PR appears as its own commits.

### 1.2 Build and pin

1. Qualification build first, on a `qual/**` branch, so nothing is built on the production host. Create `qual/build-<short sha>` from `RELEASE_SHA` with the matrix of `.github/workflows/qualification-build.yml`
   edited to `sha: <RELEASE_SHA>` for the targets `adapter-build` (adapters) and `build` (`kelvo`, launcher), for each `DUCKBRIDGE` value in use, as
   was done for `qual/build-a45556e` (#214). The workflow runs `docker buildx build --target ... --build-arg VERSION=<full sha>` from the repository Dockerfile
   and uploads the artifact `kelvo-native-<name>` containing the binaries, `SHA256SUMS` and `SOURCE` (the full commit). It publishes nothing.
2. Download the artifact on a clean machine; check `SOURCE` equals `RELEASE_SHA` (full 40 characters) and `sha256sum -c SHA256SUMS`.
3. Record the four digests in the release manifest. The slim digest is also in `/usr/share/kelvo/kelvo-adapter-relational.sha256` of the `worker` image.
   **Never derive a pin by hashing a file on the production host**; the pin comes from the build, and the node verifies the descriptor it executes.
4. Keep the artifact and manifest with the release record. **[no access]** where the existing process stores receipts.

### 1.3 Gates before anything is staged

All on `RELEASE_SHA`:

* CI green: `validate`, `warm-adapter` (which runs the read-worker gates on the slim adapter), `federation`, `package`, `go-adapters`, `jdbc-adapter`, `public-sdk`.
* The load matrix and the profile with **no patch** (there is no patch directory any more): workflows `Pooled read load` and `Pooled read profile`, run on the release branch; pass criteria in section 4.1.
* `scripts/release_upgrade_acceptance.py` for snapshot compatibility if acceleration is used (`docs/release-upgrades.md`).
* **Not covered anywhere yet:** a mixed-version rolling upgrade of this stack (`docs/rolling-upgrades.md` authorizes only tested pairs). Treat the whole tenant, node and adapter, as one step.
* **[owner]** accept that the load test models the exit-node RTT as added delay only (no Tailscale), and measured one node.

## 2. Deploy order and mixed versions

Order: **node with its adapters and launcher, then the gateway, then db-api.**

| Boundary | Behaviour | Consequence for the order |
| --- | --- | --- |
| Node to adapter: frame protocol v2 | Hello carries version 2; a v1 adapter's Hello is refused with a version mismatch (`frame.ErrVersion`), so the worker never serves. The node also pins the adapter digest. **Fails closed.** | Adapter and node ship in one release directory and are switched together |
| Launcher: `serve --pool-size N` (#225) | An older launcher refuses the argument; an older adapter exits on it. The worker never answers Hello, the launch fails, reads run one-shot. An unset `pool_size` sends no flag, so the old pair keeps working (and the adapter's own default of 8 applies from #237, 4 before it) | Deploy the new launcher and adapters with the node **before** setting `pool_size`; the node cannot detect support |
| Gateway to node | single-call read uses the existing internal results route | Roll gateways and nodes of a tenant together; no mixed-version claim exists |
| db-api to gateway: `POST /v1/queries/stream` | Against a gateway without it the client returns `ErrStreamUnsupported` before sending anything, and db-api falls back to submit, wait and fetch (a 404, 405 or 501 without the `Kelvo-Stream: 1` marker means "no such endpoint") | db-api after the node |
| db-api `inspect` requests | Metadata inspections run only on read workers; a node without them (or with the pool disabled) answers `UNAVAILABLE`. **There is no fallback** | db-api sends `inspect` only to nodes known to accept it; on rollback db-api stops first |
| Query alias and DSN | workers are keyed by the saved connection since #221 (no alias requirement; verified by the shared-alias run) | db-api sends `sslmode=verify-full&connect_timeout=5` DSNs from the resolver; the delegated query path refuses `sslmode=disable` |

### 2.1 Configuration migration (#222, #225, #228)

* A node **without** `read_workers` keeps working: capacity comes from `operations.warm_pool`, the adapter keeps its pool of 4, and (#228) reads use the full adapter. Do not migrate in the same step as the binary switch.
* Step A, with the new binaries (config unchanged), observe. Step B, migrate: move `idle_ttl`, `max_workers`, `max_per_key`, `max_in_flight`, `queue_timeout` (now `read_workers.queue_wait`) and `stream_buffer_mb`
  from `operations.warm_pool` into `read_workers`; keep `enabled`, `queue_depth`, `worker_memory_mb`, the caps, the cache and `reauthorize_every` in `warm_pool`. Setting a capacity field in both places is a configuration error.
  Run `kelvo node --config ... --check-config` (static messages, prints no values) before every restart.
* Step C: set `read_workers.relational_adapter` (`binary`, `sha256`) to enable the slim adapter; a configured but unusable slim binary logs one line at start and reads fall back to the full adapter
  (`kelvo_read_adapter_slim_active` and `kelvo_read_adapter_slim_fallbacks_total` on `/metrics`).
* Step D: set `pool_size` (1-32, with `pool_size x max_per_key` at most `read_workers.max_source_connections`, default 64, at most 1024; an unset `pool_size` counts as 8) only after steps A-C are healthy. The budget is per worker key, not per source database: keys that share a database (team, schema, credential revision) each hold that many. Also settable: `evict_min_idle` (default `min(1s, queue_wait/2)`; an explicit value must be below `queue_wait`; `queue_wait` must be below 30 s). `read_workers.max_workers` is 1-64 and at most `resources.max_concurrent`. Run `--check-config`: a configuration valid before #237 (for example `max_per_key` 8 with `pool_size` 32) may be refused.
* `policy.workers`, `policy.limits` and `policy.max_queries` stay pinned in cluster metadata; this release changes none of them. `read_workers` does not replace `policy.workers`.

### 2.2 Audit journal (#222, #230)

* From #222 the journal header does not bind `audit.max_pending`. A journal **records the value in force when it was created.** Record that value for every existing journal (gateway and node) **before** the upgrade; it is needed for rollback (section 5).
* #230 adds group commit: `audit.write_timeout` must be at least 52 ms; check the current value (production's is **[no access]**).
* Set `audit.max_pending` to at least the larger of the two minimums the node enforces at load (#239): gateway `max_http_requests`; node `policy.workers[<node>] + operations.max_concurrent` (+ `exports.max_concurrent` + `exports.max_downloads` if exports are on). A smaller value refuses to start (`audit.max_pending must be at least the work the service admits at once`); an existing node with exports needs it raised **before** the upgrade.
* Size the journals for the new rate before the switch (runbook: 2 KiB per slot, about 2 gateway receipts and 1 node receipt per read). Resizing needs a new journal directory: **[owner]** retention of the old journal.

## 3. Configuration

Validity: both configs below are the scenario files `benchmarks/pooled-read-load/scenarios/2c1g-first-release.yaml` and `4c8g.yaml`, which `TestPooledReadLoadScenariosAreValid` checks with the node's own
validation (resources, containment, operations, `read_workers`, warm pool). They already use the post-#225 keys: capacity is in `read_workers`, the rest of the pool in `operations.warm_pool` (the harness file shows the pool's fixed fields at the top level).
A node that still has the pre-migration config uses `operations.warm_pool` capacity keys until step B of section 2.1.

### 3.1 First release on the current node: 2 vCPU / 1 GB, a few users

**What the measurements allow.** On the merged code the current node is no longer CPU-bound: 20 workers at `pool_size` 12 carried the full 50-user mix in 5 of 7 runs (p50 2.0-2.3 s, p95 3.9-4.5 s per read, node 0.35-0.6 cores, 3-11 throttled periods,
cgroup memory peak 580-624 MiB of the 1 GiB), with one stall (758 deadline expiries) and one run with 53 generator-cap refusals. That is not a margin to promise, so the first release **sheds load at the door** and targets a handful of users, the load the 3-user entry
(the matrix entry `2c1g-first-release-3users`, which runs the `2c1g-first-release.yaml` scenario with a light loadgen mix: one explorer opening a page every 3-5 s, one 10-panel dashboard viewer refreshing every 10 s, one console user) carried in 4 of 4 runs with 0 errors (explorer reads p50 1.2 s, p95 4.0 s; dashboard panels p95 2.0 s; console p95 2.0 s; node 0.4 cores, 0 throttled periods, 335-350 MiB).
At **6 explorers, 6 viewers and 2 consoles** on the same config (8 workers, `max_http_requests` 64, `max_queries` 32) it sheds as designed: 72-619 capacity refusals, 95-195 503 `Worker unavailable` (8 workers against more than 8 active connections: thrash; measured before #237, which turns this into a bounded 429 + `Retry-After` and is not re-measured) and p95 6-7 s; it is not an error-free configuration at that load.
Memory is not the limit: 345 MiB for gateway and node, 12-19 MiB per worker, 11 MiB shared. So this config is a **bridge**: it turns the feature on for a handful of users while the node is replaced; it is not the 50-user target.
**[owner]** If the bridge must carry more than a handful of users, the measured alternative is 20 workers at `pool_size` 12 on this node (about 0.6 GiB, 5 of 7 clean runs, twice the latency of the target).

Settings (the scenario file; capacity is `read_workers`):

| Setting | Value | Why |
| --- | --- | --- |
| `policy.limits.max_bytes` | 67108864 | routing needs 64 MiB or less; the default 256 MiB silently disables read workers |
| `policy.workers` (a1) | 16 | node job permits for the query path; sheds load at the node instead of queueing without bound (**changing it re-initializes cluster metadata: [owner]**; keep the current value if it is higher and shed at the gateway) |
| `read_workers.max_workers` | 8 | at least the number of hot connections (more than 8 active connections thrash and answer 503 `Worker unavailable`: **never run `max_workers` below the active connections**); 8 workers are about 0.2 GiB |
| `read_workers.pool_size` | 4 (measured); 8-12 for bursts | measured at 4 with the 3-user load; 12 is the value measured at the target; unset means the adapter's default of 8 (not measured); `pool_size x max_per_key` at most `max_source_connections` (64) |
| `read_workers.idle_ttl`, `max_per_key`, `max_in_flight`, `queue_wait`, `stream_buffer_mb` | 30m, 1, 16, 10s, 4 | measured values; `max_per_key` 2 is the default (burst scale-out) |
| `read_workers.relational_adapter` | the slim adapter (step C) | 13 MiB per worker against 23 MiB; fallback to the full adapter is automatic |
| `resources.*` | `max_concurrent` 12, `memory_mb` 2100, `baseline_mb` 250, `overhead_mb` 64, `separate_classes: true`, `query_reserve_slots` 10, `query_reserve_memory_mb` 1700 | the interactive share admits `min(slots, memory / 160)` reservations of 160 MiB (32 + 64 + 16 x 4 stream buffers): 8 workers and 2 one-shot fallbacks. Memory here is **accounting**, larger than the VM on purpose (real use is about a tenth) |
| `operations.max_concurrent` | 1 | one analysis job at a time on this size |
| `gateway.max_http_requests` | 64 | load is shed at the front door |
| `audit.max_entries`, `max_pending` | 262144, 256 | 512 MiB per journal; the gateway journal (about 2 receipts per read) lasts 1 h at about 37 reads/s. **`max_pending` 64 failed: 345 requests got 503 `Audit storage unavailable` at a peak of 103 in flight;** the node refuses to start below the work a service admits: gateway `max_pending` >= `gateway.max_http_requests` (64 here), node >= `policy.workers[node]` + `operations.max_concurrent` (16 + 1 here); one value serves both, so 256 |
| db-api client | `MaxConcurrent` 32, `MaxControlConcurrent` 32 | an overload becomes fast refusals, not a collapse |
| Cgroup (systemd unit) | `MemoryMax=1G`, `MemorySwapMax=0`, `CPUQuota=200%`, `Delegate=yes` | as in `process-containment.md`; **[root] [no access]** the existing unit's values |

Acceptance for this config (canary tenant only, section 4): no `class=capacity` failure, no `PROTOCOL_ERROR`, `nr_throttled` flat, memory below 80% of `MemoryMax`, p95 of single reads under the db-api budget at 3 users. At the first
sign of CPU throttling above a few periods per 5 minutes, or any `Worker unavailable`, add no capacity; move to 3.2.

### 3.2 Target: 4 vCPU / 8 GB co-located with db-api

The `4c8g.yaml` scenario is the tested point: 20 workers, `pool_size` 12, the slim adapter. Over 10 runs it had **0 server capacity errors in all of them, 0 failed requests in 9, and 38 generator-cap refusals (0.4%) in one**; per read p50 0.74-0.83 s, p95 1.9-2.3 s, p99 2.7-3.7 s; page open of 42 requests p50 1.4 s, p95 2.9 s;
node 0.26-0.52 cores (13% of 4 vCPU), 0 throttled periods; cgroup memory peak 779-816 MiB. The whole matrix must be rerun on the release build (section 4.1).

| Area | Setting | Value |
| --- | --- | --- |
| Pool | `read_workers.max_workers` | at least the hot connections within `idle_ttl` (20 tested; 8 GB fits about 240 by memory, but the interactive admission share must be raised with it: `query_reserve_slots` above `max_workers` and `query_reserve_memory_mb` at least 160 MiB per worker plus a result download) |
| | `max_per_key` | 1 (measured); the default 2 allows a burst to scale out to a second worker |
| | `max_in_flight`, `queue_wait`, `idle_ttl`, `stream_buffer_mb` | 16, 10s, 30m, 4 |
| | `pool_size` | **12** at 80-100 ms RTT (measured at 80 ms: page open 1.4 s p50 against 3.2 s at 4); formula `ceil(42 x 3 x RTT / 1.5 s)` (a warm read holds a connection for 3 round trips): 7 at 80 ms, 9 at 100 ms, 14 at 160 ms; at most 32; `pool_size x max_per_key` at most `max_source_connections` (default 64; 12 x 1 here). The budget is per worker key; budget each source database's `max_connections` for its active keys x that product. Through the exit node RTT is the sum of both legs |
| Adapter | `read_workers.relational_adapter` | the slim adapter, pinned by digest |
| Cache | `cache_ttl` 10s, `reauthorize_every` 60s | signed off by the owner |
| Admission | `separate_classes` true, `query_reserve_slots` 28, `query_reserve_memory_mb` 5500, `containment.max_groups` 128 | see the runbook |
| Audit | `max_entries` 524288, `max_pending` 512, `retention` 1h | 1 GiB per journal; the gateway journal lasts 1 h at about 74 reads/s |
| Gateway | `max_http_requests` 512 | measured peak in flight 290 |
| Cgroup | `MemoryMax=8G`, `CPUQuota=400%` | |
| db-api client | `MaxConcurrent` 256, `MaxControlConcurrent` 128 | the generator's 256 cap is reached at the design load in about 1 run in 10; spread db-api over more than one client or raise nothing |

After the host move the Oracle VM is the Tailscale exit node only (section 6); the round trip to a source is the sum of both legs.

## 4. Step-by-step upgrade

Mirrors the existing process: stage a release directory, verify its hashes, plan, apply, verify, observe. The mechanics of the root-owned `terraform_data.upgrade` plan and apply and its verifier receipts
**[no access]** are summarized from the request; where this document says "plan" and "apply" it means that mechanism, and the receipts below are what the verifier should record in it.

### 4.1 Before the change window

1. **Qualification** (section 1.2) and gates (1.3) complete; the load matrix on the release build meets, with no patch, on the 4 vCPU / 8 GB 20-worker pool-12 entry (run it three times, all must pass): 0 server capacity errors, 0 `PROTOCOL_ERROR` on reads, 0 failed requests apart from generator-cap refusals (at most 0.5% of requests),
   page-open p95 at most 3.5 s, explorer read p95 at most 2.6 s and p99 at most 4 s, node CPU at most 0.8 cores and 0 throttled periods; and the plain-read profile at 50 concurrent: 0 failures, node CPU at most 10 ms per read, at most 6 Ed25519 verifications per read, source round trips 3.0 per read.
   The thresholds are the measured spread plus margin (runs 38874114296 to 38900878104, profile 38878205691 to 38883939553); the baseline before the fixes was 43 of 2704 failed reads, page-open p95 9.3 s and 40 ms of node CPU per read (runs 38784590203 and 38795023128). **[owner]** the final thresholds.
2. **[no access]** Read the running release: its directory under `/opt/kelvo/releases/`, its manifest and hashes, `kelvo version`, the node and gateway configs, the unit files, the previous upgrade's receipt.
3. **[root]** Record: `systemctl show kelvo-node -p MemoryMax -p CPUQuotaPerSecUSec -p Delegate -p MemorySwapMax`; the NATS metadata record and consumer state (unchanged by this release, recorded for the receipt);
   each audit journal's creation `max_pending` (section 2.2) and free space for the new journal size; the egress IP seen by a source (`curl https://ifconfig.me` as the `kelvo` user, via the same path).
   **Back up before the apply [root] [no access]**: the node and gateway configs, every audit journal directory with its creation `max_pending`, the containment state directory and the NATS state (JetStream store, consumer state, metadata record), to a location outside the release directory, and record how to restore each. Keep the old journal directories until the rollback window closes.
4. Previous failure: **[no access]** an earlier upgrade failed at worker execution and needed recovery. Obtain that incident's notes and add its cause as a pre-check. This plan adds the checks below, which cover the usual causes
   (adapter digest, launcher pairing, containment placement, group limits), but the actual cause is not in this repository.
5. Draft the config for the first step (A) and the release manifest; plan review by a second person **[owner]**.

### 4.2 Stage the release **[root]**

1. Create `/opt/kelvo/releases/<date>-pooled-read-workers/` with the four binaries, `SHA256SUMS`, `SOURCE`, the new config files and the manifest; owner root, binaries `0555`.
2. `sha256sum -c SHA256SUMS`; compare every digest with the record from the qualification build (not with a hash taken on this host).
3. Static checks without starting anything: `kelvo version` shows `RELEASE_SHA`; `kelvo node --config <new node.yml> --check-config`; the adapters are not executed on the host before the switch beyond what the node does.
4. Confirm the host: Landlock ABI 3+, cgroup v2 with `cpu memory pids` available, unit `Delegate=yes`, the jobs directory and `scratch_directory`/`containment.state_directory` owned by the service user with mode `0700`.

### 4.3 Plan and apply **[root]**

1. **Plan**: the infra plan must show only: the release symlink or `ExecStart` path change, the config files, and (step D) nothing else. Reject a plan that touches the broker, TLS identities, policy metadata or unit limits.
2. Window: choose low traffic **[owner]**. Tell db-api's owners; freeze db-api deploys.
3. Drain: SIGTERM with `--drain-timeout` long enough for in-flight reads and the longest analysis operation (default 30 s; set larger if analysis jobs run) **[owner]**. Admission stops and readiness fails; assigned work finishes within grace; grace expiry cancels work without replay.
4. **Apply**: switch to the new release, start the node unit, then the gateway. Order within a host: node before gateway.
5. The node starts inside the service cgroup per `process-containment.md` (the supervisor and jobs groups are created by the unit's bootstrap); no manual cgroup edits.

### 4.4 Health checks after the switch (exact probes)

Run them from the host as the service user; certificates are the deployment's own **[no access]**.

| # | Probe | Pass |
| --- | --- | --- |
| 1 | `systemctl is-active kelvo-node kelvo-gateway`; `journalctl -u kelvo-node -n 200` | active; startup lines show the release SHA, no containment, audit or adapter-digest error |
| 2 | `curl --cacert $CA https://127.0.0.1:$GATEWAY_PORT/health` and `/ready` | 200; `/ready` needs a ready worker per tenant |
| 3 | `curl --cert $GW_CRT --key $GW_KEY --cacert $CA https://$NODE/resources` | admission snapshot present; interactive share shows the configured reservations and no drain flag |
| 4 | same client certificate, `GET /metrics` | counters present; `kelvo_read_adapter_slim_active 1` when the slim adapter is pinned; no `slim_fallbacks` increase |
| 5 | `cat /sys/fs/cgroup/system.slice/kelvo-node.service/{memory.current,memory.peak,cpu.stat}`; `ls .../jobs | wc -l` | baseline recorded; job groups appear as workers spawn |
| 6 | **Worker execution smoke**, the step that failed last time: through db-api (or a signed grant from a test principal) on the canary connection: one native `SELECT 1`, one 50-row preview, one schema inspection, one analysis operation | all succeed; `journalctl -u kelvo-node | grep 'kelvo read failed'` empty; a worker cgroup appears under `jobs/` and stays |
| 7 | the same read a second time within 60 s | faster than the first (no resolve); one worker still present |
| 8 | the same read after 70 s | re-authorizes once (`resolver` call count +1), still succeeds |
| 9 | an explicit refresh through db-api (`X-Refresh`) | reaches the source (source-side statement count +1); the harness probe missed in 11 of 63 runs before #234; #234 is merged (a refresh never joins a running read) but not re-measured, so treat a miss on the release build as a defect to report, and a rollback condition only together with another failure |
| 10 | egress IP check as in 4.1 | unchanged |

Any failure in 1-8 is a rollback condition (section 5) unless the cause is found and fixed within the window **[owner]**.

### 4.5 Enable steps, then observe

1. db-api: deploy the single-call read behind its existing fallback, for one canary tenant first **[owner]** (Hakopod deploy **[no access]**, not performed by this plan).
2. Observation window: 2 hours canary, then 24 hours with all tenants. Watch, every minute for the first 30 minutes and every 5 minutes after:

| Signal | Source | Rollback trigger (proposed, **[owner]**) |
| --- | --- | --- |
| Read failure rate by code/class/stage | `journalctl ... 'kelvo read failed: code='` | any `class=capacity` above 0.5% of reads in 5 minutes; `stage=resolve` above 5% |
| Client-visible protocol errors | db-api logs `PROTOCOL_ERROR` | any |
| CPU throttling | cgroup `cpu.stat` `nr_throttled` | increasing by more than 100 per minute for 3 minutes |
| Memory | cgroup `memory.current`, `memory.events` `oom_kill` | above 80% of `MemoryMax` for 5 minutes; any `oom_kill` |
| Warm pool health | `/resources`, job cgroup count, `slim_fallbacks`, 503 `Worker unavailable` | `workers == max_workers` with fallbacks, `slim_fallbacks` increasing, any `Worker unavailable` (a 503, which #237 removes for capacity; a 429 with `Retry-After` from the pool is a pool smaller than the active connections: raise `max_workers`) |
| Audit | gateway answers 503 `Audit storage unavailable` (a full journal; a full pending queue is a 429 since #236) | any 503; any 429 from audit pressure above 0.5% of requests; projected journal fill above 70% within `retention` |
| Latency | db-api per-read p95 | above the agreed budget for 10 minutes **[owner]** |
| Resolver | `kelvo_worker_resolve_seconds` | p95 above 1 s |

3. Step B (config migration), then C, then D (section 2.1), each with a 30-minute observation and its own receipt. Do not combine them.
4. Close: record final hashes, config, journal creation values, metrics at the end of the window and the egress IP in the release receipt.

## 5. Rollback

**Drain order for a rollback (and for any restart):** db-api first; then the **node before the gateway**: the gateway keeps serving status, cancel and result reads and waits its drain grace because result handles are shared; admission stops and readiness fails on SIGTERM on either service. While the node is down, queued reads wait until a node assigns them, the job expires (`policy.job_ttl`, 2 m in the scenarios) or the client disconnects, and they occupy the gateway's parked-waiter pool of `max_http_requests`; past it the gateway answers 429. Start the old node before the old gateway and confirm `/ready` before db-api resumes (`docs/cluster.md`, `docs/operations.md` Maintenance; **rehearse it** on a canary: that the waiters survive a node restart is documented, not measured here).

**Conditions:** any trigger in 4.5; any failure of probes 1-8; a worker-execution failure that is not understood in 30 minutes; a security finding. Rollback is cheap before step B and must stay cheap: **do not migrate the config and the binaries in one change.**

Order (reverse of the deploy):

1. **db-api first.** Stop sending single-call reads and `inspect` requests (a node with the pool disabled, or an older one, answers inspections `UNAVAILABLE`); db-api goes back to its previous read and metadata paths. **[no access]** Hakopod rollback.
2. **Gateway and node, together.** **[root]** Drain as in 4.3, point the unit back to the previous release directory (previous `kelvo`, **its** launcher and **its** adapter; all four digests of the old manifest), restore the previous config files. Adapter and node are one pair: a v2 node with a
   v1 adapter, or the reverse, fails closed and reads run one-shot or fail.
3. **Faster partial rollback without a binary change:** set `operations.warm_pool.enabled: false` **and remove the whole `read_workers` block**, then restart. `enabled: false` alone is not enough: once `read_workers` is present the node refuses to start (`read_workers requires operations.warm_pool enabled, and capacity set only in read_workers`). Reads return to the one-shot path; inspections fail, so do step 1 first. After migration step B this is the only no-binary rollback; before step B, restoring the previous config file is enough.
4. **Audit journal `max_pending`.** An older binary opens a journal only if its configuration equals the journal header **including `max_pending`**, and the header holds the value from when the journal was created. If `audit.max_pending` was changed after the
   upgrade, set it back to the recorded creation value (section 2.2) before starting the old binary, or start the old binary on a new, empty journal directory and keep the old one for reconciliation. **Never delete a journal to get past the error.**
   #230's group commit changes no header, but `write_timeout` below 52 ms is a configuration error on the new binary only.
5. **Metadata and policy.** This release changes none of `policy.workers`, `policy.limits`, `policy.max_queries` or the NATS record; rollback does not touch them. If anyone changed them, restore the recorded values first. Keep current security
   revisions: rolling back a binary must not restore revoked keys or an older source policy.
6. Re-run probes 1-8 on the old release; record the rollback receipt.
7. **Recovery from a failed worker execution** (as in the earlier incident **[no access]**): stop the unit, confirm no processes remain in `/sys/fs/cgroup/system.slice/kelvo-node.service/jobs`, preserve the containment state directory for inspection, then
   start on a fresh state directory (`process-containment.md`: unknown groups, corrupt records or stale root identity fail closed; recovery is by operator repair, not automatic).

## 6. Host move: Kelvo next to db-api, Oracle VM as the exit node only

**[owner]** Decide first: where the NATS broker runs after the move, and whether the move is a same-identity cutover (downtime of one drain) or a new worker identity (changes `policy.workers` and needs a re-initialization and
drain per `docs/operations.md`). The steps below assume a **same-identity cutover** of the node and gateway; the broker and its TLS are unchanged and reachable from the new host.

1. **Provision [root] [no access]** (infrastructure repo; not run here): a VM in db-api's region and network, 4 vCPU / 8 GB, Linux with Landlock ABI 3+, cgroup v2, the `kelvo` user, the unit file with `Delegate=yes`, `MemoryMax=8G`, `CPUQuota=400%`,
   directories as in 4.2. Same release directory layout; stage the **same** release that is running (do not move host and version together).
2. **Tailscale.** Install and join the tailnet with a tag for the node. Make the Oracle VM the exit node: advertise it (`tailscale up --advertise-exit-node` on the Oracle VM, approve it in the admin console **[owner]**), then on the new host set it as the exit node with local-network access so
   the traffic that must stay local stays local: `tailscale set --exit-node=<oracle> --exit-node-allow-lan-access=true`. **All other outbound traffic then leaves through the Oracle VM** (and **can lock you out**: the command can cut the SSH or console session that ran it. Keep console or out-of-band access open, keep `--exit-node-allow-lan-access=true`, and from a second session test the return paths for SSH, monitoring and deploy before leaving it in place; revert with `tailscale set --exit-node=`), so the broker, the resolver (db-api) and the gateway's clients must be on the local network or the tailnet, not
   on the public internet. **[owner]** whether to route only source networks instead (subnet routes and ACLs), which changes the egress IP guarantee. ACL: the new host may reach the Oracle VM as an exit node and nothing more; customer databases' allowlists keep the Oracle VM's public IP.
3. **Verify before cutover.** As the `kelvo` user on the new host: egress IP equals the Oracle VM's; a TLS connection to one canary source with `sslmode=verify-full` succeeds; RTT to the source (`ping`/connect time) measured and compared with the model:
   the new `pool_size` follows `ceil(42 x turns x RTT / 1.5 s)` with RTT = host to exit node + exit node to source (runbook table). Bandwidth: the exit node carries all result bytes; the test mix was about 0.3 MB/s mean; check the uplink for the largest export.
4. **Dry run.** Start the new node and gateway on the new host with **a separate test tenant/identity** if one exists, or with the production identity **stopped on the old host** (one owner per worker id, never both). Run the probes of 4.4 and the load profile there (`Pooled read profile` against this host shape is the comparison).
5. **Cutover [root] [owner]:** drain the old node and gateway (4.3 step 3); copy config and journals' policy as needed (**journals stay with their service identity**: a new host starts new journals; keep the old directory read-only for reconciliation); start the node then the gateway on the new host; probes 1-10.
6. **db-api [no access]:** change the Kelvo endpoint (Hakopod environment) to the new host's name; the gateway's client-facing certificate needs a SAN for the new name; the worker and gateway mTLS identities (`spiffe://kelvo/gateway`, `spiffe://kelvo/tenant/<t>/worker/<id>`) are unchanged if the certificates move with the identity; the resolver URL
   in the principal policy (`access.principals.<p>.delegated_resolver.url`) is **frozen in policy metadata**: keep the resolver at the same URL (a changed URL is a policy change and a drain/reprovision). DNS: lower the TTL a day ahead; switch the record; the old name stays valid until the rollback window closes.
7. **Observation:** the window of 4.5 on the new host; the baseline for CPU per read and latency is the profile on this host shape, not the Oracle VM's.
8. **Rollback path:** keep the Oracle VM's Kelvo stopped but intact for 7 days **[owner]**, with its release directory, config and journals. Rollback: drain the new host (node before gateway, see section 5), start the old node and gateway on the Oracle VM (same identity), point db-api's endpoint back, confirm the egress IP and probes. The exit-node setup does not need
   to change because the Oracle VM's IP is the same in both directions. Do not run both hosts with one identity.
9. After the window: remove Kelvo from the Oracle VM (**[owner]**), leaving only Tailscale and the exit-node configuration; its 951 MB limit then no longer applies to workers. **Never delete the Oracle Kelvo install without a verified backup** (config, journals, containment and NATS state, release directories): the rollback path of step 8 needs them for 7 days, and the journals are the audit record.

## 7. Receipts to keep

Release SHA and commit list; the four digests and the artifact's `SOURCE`; the config diff per step; `--check-config` output; the journal creation values; probes 1-10 with timestamps; observation-window metrics; the egress IP before and after; the rollback receipt if used.

## 8. Known issues

* **2 vCPU / 1 GB stall** ([issue 240](https://github.com/SyneHQ/kelvo-go/issues/240)): 1 of 7 target-load runs on the current node failed every request from about t = 225 s with the node at 0.5 cores; unexplained. It is why the first release is a few users, not the 50-user mix. The harness captures goroutines and worker state if a run stalls (a manual dispatch of the matrix).
* **Flaky `validate` test** ([issue 242](https://github.com/SyneHQ/kelvo-go/issues/242)): `TestPrivateRuntimeDataOpenUsesOperationContextAndConfiguredBudget` fails intermittently under `-race`; re-run, do not ignore a repeat.
* **Pre-fix measurements.** Every number here predates #234, #235 (the 152 KB adapter allocations per read figure is pre-#235), #238, #239, #241 and #237. The thresholds in 4.1 are the measured spread plus margin on that code and **must be re-derived from the rerun on the release build**.
* **Not verified:** the exit-node path (modelled as added delay), mixed gateway and node versions, a multi-node cluster, node restart behaviour for waiting gateway requests, and the earlier worker-execution incident's cause.

# Pooled read workers: production release and upgrade plan

Status: **plan only.** Nothing here has been run against production, and writing it touched no host, Terraform or Hakopod.
Facts about production (the Oracle VM, `/opt/kelvo/releases/<date>-<name>`, hash-pinned binaries, the root-owned
`terraform_data.upgrade` plan and apply with verifier receipts, db-api on Hakopod as `syne/db-api`) come from the request that
asked for this plan, not from this repository; steps that depend on them are marked. Marks used below:

* **[root]** needs root on the host or the infrastructure repo's root-owned apply.
* **[owner]** needs a decision by the service owner.
* **[no access]** depends on something this session cannot see or reach; the executor must supply or verify it.

Numbers come from [pooled-read-workers.md](pooled-read-workers.md) (load matrix) and [profile-plain-read.md](profile-plain-read.md). They were
measured on the integration build with a measurement patch; **every threshold below must be re-validated on the release build** (section 4.1).

## 0. Where the code is today (2026-10-16; refresh before every use)

Merged to `cargo`: #212 design, #215 phase 1 (frame v2, multiplexed warm sessions), #226 exact `NUMERIC`, #227 bounded source codes, #231.
Open, and part of this release when merged: #217 routing, #218 admission classes, #219 result cache, #220 single-call read, #221 credentials once per
connection and re-authorization, #222 `read_workers` settings and the audit header change, #225 `read_workers` wiring and `serve --pool-size`, #228 slim relational adapter,
#229 fewer source round trips, #230 control-plane diet, plus the read-path fixes routed from the load test. The harness (#224) and the integration branch (#223) are test-only and are not released.

Known open defects at the time of writing (see the runbook): every read fails the client receipt check until #219's batch count is fixed; `bypass_cache` does not reach the
cache; idle workers are not evicted for a new key; reads that wait more than 5 s for a slot fail as expired credentials; waiting single-call reads re-verify the grant every
50 ms; workers are keyed by query alias. **Do not release until each is fixed and shown fixed by the matrix with `benchmarks/pooled-read-load/patches/` empty.**

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
* The load matrix and the profile with **no patch**: workflows `Pooled read load` and `Pooled read profile`, run on the release branch; pass criteria in section 4.1.
* `scripts/release_upgrade_acceptance.py` for snapshot compatibility if acceleration is used (`docs/release-upgrades.md`).
* **Not covered anywhere yet:** a mixed-version rolling upgrade of this stack (`docs/rolling-upgrades.md` authorizes only tested pairs). Treat the whole tenant, node and adapter, as one step.
* **[owner]** accept that the load test models the exit-node RTT as added delay only (no Tailscale), and measured one node.

## 2. Deploy order and mixed versions

Order: **node with its adapters and launcher, then the gateway, then db-api.**

| Boundary | Behaviour | Consequence for the order |
| --- | --- | --- |
| Node to adapter: frame protocol v2 | Hello carries version 2; a v1 adapter's Hello is refused with a version mismatch (`frame.ErrVersion`), so the worker never serves. The node also pins the adapter digest. **Fails closed.** | Adapter and node ship in one release directory and are switched together |
| Launcher: `serve --pool-size N` (#225) | An older launcher refuses the argument; an older adapter exits on it. The worker never answers Hello, the launch fails, reads run one-shot | Deploy the new launcher and adapters with the node **before** setting `pool_size`; the node cannot detect support |
| Gateway to node | single-call read uses the existing internal results route | Roll gateways and nodes of a tenant together; no mixed-version claim exists |
| db-api to gateway: `POST /v1/queries/stream` | Against a gateway without it the client returns `ErrStreamUnsupported` before sending anything, and db-api falls back to submit, wait and fetch (a 404, 405 or 501 without the `Kelvo-Stream: 1` marker means "no such endpoint") | db-api after the node |
| db-api `inspect` requests | Metadata inspections run only on read workers; a node without them (or with the pool disabled) answers `UNAVAILABLE`. **There is no fallback** | db-api sends `inspect` only to nodes known to accept it; on rollback db-api stops first |
| Query alias | workers are keyed by the request alias until the read-path fix lands | db-api sends one alias per saved connection, and `sslmode=verify-full&connect_timeout=5` DSNs from the resolver |

### 2.1 Configuration migration (#222, #225, #228)

* A node **without** `read_workers` keeps working: capacity comes from `operations.warm_pool`, the adapter keeps its pool of 4, and (#228) reads use the full adapter. Do not migrate in the same step as the binary switch.
* Step A, with the new binaries (config unchanged), observe. Step B, migrate: move `idle_ttl`, `max_workers`, `max_per_key`, `max_in_flight`, `queue_timeout` (now `read_workers.queue_wait`) and `stream_buffer_mb`
  from `operations.warm_pool` into `read_workers`; keep `enabled`, `queue_depth`, `worker_memory_mb`, the caps, the cache and `reauthorize_every` in `warm_pool`. Setting a capacity field in both places is a configuration error.
  Run `kelvo node --config ... --check-config` (static messages, prints no values) before every restart.
* Step C: set `read_workers.relational_adapter` (`binary`, `sha256`) to enable the slim adapter; a configured but unusable slim binary logs one line at start and reads fall back to the full adapter
  (`kelvo_read_adapter_slim_active` and `kelvo_read_adapter_slim_fallbacks_total` on `/metrics`).
* Step D: set `pool_size` (1-32, with `max_workers x pool_size` at most 256) only after steps A-C are healthy.
* `policy.workers`, `policy.limits` and `policy.max_queries` stay pinned in cluster metadata; this release changes none of them. `read_workers` does not replace `policy.workers`.

### 2.2 Audit journal (#222, #230)

* From #222 the journal header does not bind `audit.max_pending`. A journal **records the value in force when it was created.** Record that value for every existing journal (gateway and node) **before** the upgrade; it is needed for rollback (section 5).
* #230 adds group commit: `audit.write_timeout` must be at least 52 ms; check the current value (production's is **[no access]**).
* Size the journals for the new rate before the switch (runbook: 2 KiB per slot, about 2 gateway receipts and 1 node receipt per read). Resizing needs a new journal directory: **[owner]** retention of the old journal.

## 3. Configuration

Validity: both configs below are the scenario files `benchmarks/pooled-read-load/scenarios/2c1g-first-release.yaml` and `4c8g.yaml`, which `TestPooledReadLoadScenariosAreValid` checks with the node's own
validation (resources, containment, operations, warm pool). They use the pre-#225 key names (`operations.warm_pool.*`); after step B the capacity keys move to `read_workers` as in section 2.1.

### 3.1 First release on the current node: 2 vCPU / 1 GB, a few users

**What the measurements allow.** On this size the load test collapsed at its design load and at half of it (10 explorers, 5 viewers per dashboard; 880 of 928 reads failed), with the node at 78-95% of its CPU
limit and throttled thousands of periods. The only 2 vCPU runs with no errors carried **about three active users**: one explorer opening a page every 3-5 s (42 requests), one viewer of a
10-panel dashboard refreshing every 10 s, one console user (at 80 ms to the source: explorer reads p95 5.5 s, dashboard panels p95 3 s, console p95 1.7 s; 4 workers, run `evidence-shared-alias`). Memory is not the limit: 340 MiB for gateway and node, 22 MiB per worker, 42 MiB shared.
8 workers are about 0.6 GiB. So this config is a **bridge**: it turns the feature on for a handful of users while the node is replaced; it is not the 50-user target.

Settings (differences from the load-test 2 vCPU file are in the right column):

| Setting | Value | Why |
| --- | --- | --- |
| `policy.limits.max_bytes` | 67108864 | routing needs 64 MiB or less; the default 256 MiB silently disables read workers |
| `policy.workers` (a1) | 16 | node job permits for the query path; sheds load at the node instead of queueing without bound (**changing it re-initializes cluster metadata: [owner]**; keep the current value if it is higher and shed at the gateway) |
| `warm_pool.max_workers` | 8 | at least the number of hot connections; 8 workers is about 0.6 GiB real memory |
| `warm_pool.idle_ttl` | 10m | bounds memory while idle workers are not evicted for new keys |
| `warm_pool.max_per_key`, `max_in_flight` | 1, 16 (after the burst fix: `read_workers.max_per_key` 2) | defaults |
| `resources.*` | `max_concurrent` 12, `memory_mb` 1400, `baseline_mb` 250, `separate_classes: true`, `query_reserve_slots` 10, `query_reserve_memory_mb` 1000 | the interactive share admits `min(slots, memory / 96)` = 10 reservations: 8 workers and 2 one-shot fallbacks. Memory here is **accounting** (96 MiB per reservation, 4x real use), larger than the VM on purpose |
| `operations.max_concurrent` | 1 | one analysis job at a time on this size |
| `gateway.max_http_requests` | 64 | load is shed at the front door |
| `audit.max_entries`, `max_pending` | 262144, 64 | 512 MiB per journal; the gateway journal (about 2 receipts per read) lasts 1 h at about 37 reads/s |
| db-api client | `MaxConcurrent` 32, `MaxControlConcurrent` 32 | an overload becomes fast refusals, not a collapse |
| Cgroup (systemd unit) | `MemoryMax=1G`, `MemorySwapMax=0`, `CPUQuota=200%`, `Delegate=yes` | as in `process-containment.md`; **[root] [no access]** the existing unit's values |

Acceptance for this config (canary tenant only, section 4): no `class=capacity` failure, no `PROTOCOL_ERROR`, `nr_throttled` flat, memory below 80% of `MemoryMax`, p95 of single reads under the db-api budget at 3 users. At the first
sign of CPU throttling add no capacity; move to 3.2.

### 3.2 Target: 4 vCPU / 8 GB co-located with db-api

The `4c8g.yaml` scenario with `max_workers` 20 is the tested point (0 server capacity errors, page open p95 9.3 s on a shared runner). Recommended production values, with the
caveat that the whole load matrix must be rerun on the release build:

| Area | Setting | Value |
| --- | --- | --- |
| Pool | `read_workers.max_workers` | at least the hot connections within `idle_ttl` (20 tested; 8 GB fits about 240 by memory, but the interactive admission share must be raised with it: slots and `query_reserve_memory_mb / 96`) |
| | `max_per_key` | 2 (burst scale-out) once the fix lands; 1 before |
| | `max_in_flight`, `queue_wait`, `idle_ttl` | 16, 2 s, 10m |
| | `pool_size` | `ceil(42 x turns x RTT / 1.5 s)`: 9 at 80 ms with 4 turns, 4-5 with #229's 2 turns; 8-16 until #229 is deployed; at most 32 |
| Cache | `cache_ttl` 10s, `reauthorize_every` 60s | signed off by the owner |
| Admission | `separate_classes` true, `query_reserve_slots` 28, `query_reserve_memory_mb` 5500, `containment.max_groups` 128 | see the runbook |
| Audit | `max_entries` 524288, `max_pending` 512, `retention` 1h | 1 GiB per journal; the gateway journal lasts 1 h at about 74 reads/s |
| Gateway | `max_http_requests` 512 | measured peak in flight 270 |
| Cgroup | `MemoryMax=8G`, `CPUQuota=400%` | |
| db-api client | `MaxConcurrent` 256, `MaxControlConcurrent` 128 | |

After the host move the Oracle VM is the Tailscale exit node only (section 6); the round trip to a source is the sum of both legs.

## 4. Step-by-step upgrade

Mirrors the existing process: stage a release directory, verify its hashes, plan, apply, verify, observe. The mechanics of the root-owned `terraform_data.upgrade` plan and apply and its verifier receipts
**[no access]** are summarized from the request; where this document says "plan" and "apply" it means that mechanism, and the receipts below are what the verifier should record in it.

### 4.1 Before the change window

1. **Qualification** (section 1.2) and gates (1.3) complete; the load matrix on the release build meets, with no patch: 0 server capacity errors, 0 `PROTOCOL_ERROR`, 4 vCPU / 8 GB page-open p95 and failures no worse than the recorded baseline
   (run 38784590203: 43 of 2704 explorer reads, p95 9.3 s), and CPU per read and allocations per read no worse than the profile baseline (run 38793567524) with the fixes' improvements recorded. **[owner]** the final thresholds.
2. **[no access]** Read the running release: its directory under `/opt/kelvo/releases/`, its manifest and hashes, `kelvo version`, the node and gateway configs, the unit files, the previous upgrade's receipt.
3. **[root]** Record: `systemctl show kelvo-node -p MemoryMax -p CPUQuotaPerSecUSec -p Delegate -p MemorySwapMax`; the NATS metadata record and consumer state (unchanged by this release, recorded for the receipt);
   each audit journal's creation `max_pending` (section 2.2) and free space for the new journal size; the egress IP seen by a source (`curl https://ifconfig.me` as the `kelvo` user, via the same path).
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
| 9 | an explicit refresh through db-api (`X-Refresh`) | reaches the source (source-side statement count +1); only meaningful once the cache bypass fix is in |
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
| Warm pool health | `/resources`, job cgroup count, `slim_fallbacks` | `workers == max_workers` with fallbacks, or `slim_fallbacks` increasing |
| Audit | gateway answers 503 `Audit storage unavailable` | any; and projected journal fill above 70% within `retention` |
| Latency | db-api per-read p95 | above the agreed budget for 10 minutes **[owner]** |
| Resolver | `kelvo_worker_resolve_seconds` | p95 above 1 s |

3. Step B (config migration), then C, then D (section 2.1), each with a 30-minute observation and its own receipt. Do not combine them.
4. Close: record final hashes, config, journal creation values, metrics at the end of the window and the egress IP in the release receipt.

## 5. Rollback

**Conditions:** any trigger in 4.5; any failure of probes 1-8; a worker-execution failure that is not understood in 30 minutes; a security finding. Rollback is cheap before step B and must stay cheap: **do not migrate the config and the binaries in one change.**

Order (reverse of the deploy):

1. **db-api first.** Stop sending single-call reads and `inspect` requests (a node with the pool disabled, or an older one, answers inspections `UNAVAILABLE`); db-api goes back to its previous read and metadata paths. **[no access]** Hakopod rollback.
2. **Gateway and node, together.** **[root]** Drain as in 4.3, point the unit back to the previous release directory (previous `kelvo`, **its** launcher and **its** adapter; all four digests of the old manifest), restore the previous config files. Adapter and node are one pair: a v2 node with a
   v1 adapter, or the reverse, fails closed and reads run one-shot or fail.
3. **Faster partial rollback without a binary change:** set `operations.warm_pool.enabled: false` and restart. Reads return to the one-shot path; inspections fail, so do step 1 first.
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
   the traffic that must stay local stays local: `tailscale set --exit-node=<oracle> --exit-node-allow-lan-access=true`. **All other outbound traffic then leaves through the Oracle VM**, so the broker, the resolver (db-api) and the gateway's clients must be on the local network or the tailnet, not
   on the public internet. **[owner]** whether to route only source networks instead (subnet routes and ACLs), which changes the egress IP guarantee. ACL: the new host may reach the Oracle VM as an exit node and nothing more; customer databases' allowlists keep the Oracle VM's public IP.
3. **Verify before cutover.** As the `kelvo` user on the new host: egress IP equals the Oracle VM's; a TLS connection to one canary source with `sslmode=verify-full` succeeds; RTT to the source (`ping`/connect time) measured and compared with the model:
   the new `pool_size` follows `ceil(42 x turns x RTT / 1.5 s)` with RTT = host to exit node + exit node to source (runbook table). Bandwidth: the exit node carries all result bytes; the test mix was about 0.3 MB/s mean; check the uplink for the largest export.
4. **Dry run.** Start the new node and gateway on the new host with **a separate test tenant/identity** if one exists, or with the production identity **stopped on the old host** (one owner per worker id, never both). Run the probes of 4.4 and the load profile there (`Pooled read profile` against this host shape is the comparison).
5. **Cutover [root] [owner]:** drain the old node and gateway (4.3 step 3); copy config and journals' policy as needed (**journals stay with their service identity**: a new host starts new journals; keep the old directory read-only for reconciliation); start the node then the gateway on the new host; probes 1-10.
6. **db-api [no access]:** change the Kelvo endpoint (Hakopod environment) to the new host's name; the gateway's client-facing certificate needs a SAN for the new name; the worker and gateway mTLS identities (`spiffe://kelvo/gateway`, `spiffe://kelvo/tenant/<t>/worker/<id>`) are unchanged if the certificates move with the identity; the resolver URL
   in the principal policy (`access.principals.<p>.delegated_resolver.url`) is **frozen in policy metadata**: keep the resolver at the same URL (a changed URL is a policy change and a drain/reprovision). DNS: lower the TTL a day ahead; switch the record; the old name stays valid until the rollback window closes.
7. **Observation:** the window of 4.5 on the new host; the baseline for CPU per read and latency is the profile on this host shape, not the Oracle VM's.
8. **Rollback path:** keep the Oracle VM's Kelvo stopped but intact for 7 days **[owner]**, with its release directory and config. Rollback: drain the new host, start the old node and gateway on the Oracle VM (same identity), point db-api's endpoint back, confirm the egress IP and probes. The exit-node setup does not need
   to change because the Oracle VM's IP is the same in both directions. Do not run both hosts with one identity.
9. After the window: remove Kelvo from the Oracle VM (**[owner]**), leaving only Tailscale and the exit-node configuration; its 951 MB limit then no longer applies to workers.

## 7. Receipts to keep

Release SHA and commit list; the four digests and the artifact's `SOURCE`; the config diff per step; `--check-config` output; the journal creation values; probes 1-10 with timestamps; observation-window metrics; the egress IP before and after; the rollback receipt if used.

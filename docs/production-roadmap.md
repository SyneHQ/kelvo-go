# Production roadmap: lessons from Spice OSS

Source review: **2 October 2026**. These are proposed changes, not implemented
features or new performance results.

Recommendation: keep Kelvo's Go coordinator, DuckDB execution, Arrow output,
Parquet snapshots and NATS dispatch. Adopt the dataset lifecycle, resource
accounting, diagnostics and recovery mechanisms that make those components
dependable. Adding another query engine is not a prerequisite.

## Review scope

The comparison used Kelvo
[`90ade2e`](https://github.com/SyneHQ/kelvo-go/tree/90ade2e2092a90a05a65e20628cd350079f36373),
Spice's latest stable release
[`v2.3.2`](https://github.com/spiceai/spiceai/releases/tag/v2.3.2)
at `e4da550460ef69b67a93310c5c67810f79cbb2c5`, and development revision
[`a6e0b05`](https://github.com/spiceai/spiceai/commit/a6e0b05acdf39b023c7e0cc134d410909ecf70a3).
Online upstream source was checked rather than treating the locally rebranded
Rust reference checkout as current upstream. Implementation references below
pin the stable commit unless marked development-only. Release criteria and
license text are pinned to the reviewed development revision.

Areas inspected include acceleration, schemas, snapshots, cache invalidation,
CDC acknowledgement, query tracking, readiness, shutdown, memory accounting,
secret providers, retries, federation policies, Flight SQL, durable jobs and
test infrastructure. Source presence is not a claim about every released
binary, connector, feature flag, commercial edition or production workload.

No runtime changes, builds, deployments or new benchmarks were performed for
this review. Both [existing CI jobs](https://github.com/SyneHQ/kelvo-go/actions/runs/36974733994)
passed for the reviewed Kelvo revision;
the [validation record](validation.md) retains the live-provider and capacity
boundaries of those results.

## What Kelvo already provides

Preserve these mechanisms while extending the system:

- Tenant-bound workers, separate NATS accounts, authenticated tenant selection
  and mTLS worker identities.
- Durable compare-and-swap query assignment, ownership leases, cancellation and
  explicit failure rather than automatic replay of an assigned query.
- Disposable sandboxed query processes, source/table allowlists, selected-secret
  forwarding and explicit row, byte, time, thread and temporary-data limits.
- Exact supported Arrow types, required-filter enforcement and local evaluation
  of predicates that cannot safely run at a source.
- Immutable snapshot publication, writer fencing, fingerprints for configuration
  and authorization, pinned readers and previous-generation preservation.
- Real cross-source correctness tests, micro-VM measurements and fault-handling
  evidence. The recent additions still have live Oracle/warehouse validation gaps.

The important current limits are visible in the implementation:

| Area | Current boundary | Consequence |
| --- | --- | --- |
| Resource admission | [Node dispatch](../internal/cluster/node.go) counts jobs; [query limits](../internal/query/types.go) apply individually | Several individually permitted queries plus refresh work can exceed a node's actual memory or scratch capacity. |
| Health | [Gateway routes](../internal/cluster/gateway.go) take an HTTP permit before `/health` and `/ready`; readiness checks tenant reconciliation | Saturation can return 429 to probes. Source health, usable snapshots and sufficient worker capacity are separate, currently incomplete signals. |
| Diagnostics | Per-query `Stats` exist; no first-class metrics/trace/history service | Expired query handles cannot explain recurring queue delays, source errors, pressure or failed refreshes. |
| Shutdown | [Node close](../internal/cluster/node.go) and [cluster shutdown](../cmd/kelvo/cluster.go) cancel active work | Safe cancellation exists; an explicit stop-admission/finish-in-flight deployment mode does not. |
| Acceleration | [RefreshWriter](../internal/acceleration/backend.go) models one file; [catalog validation](../internal/catalog/acceleration.go) caps object snapshot `max_bytes` at 4 GiB | Every refresh extracts a complete result. Large datasets need bounded parts and selective replacement, not a larger monolithic buffer. |
| Schema and storage | [Parquet sink](../internal/acceleration/parquet.go) validates each refresh; manifests do not record a cross-generation schema contract. Remote [Prune](../internal/acceleration/object_store.go) intentionally never deletes | A valid but changed source schema lacks an evolution policy. Remote storage reclamation needs reader protection before deletion is safe. |
| Delivery | Cluster results are tied to a single consumer; no persistent result catalog/cache | A client disconnect can waste expensive work. Repeated dashboards still execute the same analysis. |
| Federation | Eight adapters expose projected scans and integer/Boolean predicates | Other predicates and all joins/aggregates remain local; warehouse paths currently decode JSON pages. |

These are source-observed boundaries, not claims that every listed failure has
been reproduced. DuckDB still completes execution before Arrow delivery through
the pinned Go driver; changing the transport alone does not remove that memory
boundary.

## Priority matrix

P0 means a foundation for a dependable production offering. P1 improves scale,
recovery or workload cost after those foundations. P2 depends on demonstrated
customer workloads. Effort is relative engineering scope, not a delivery promise.

| Priority | Feature to adopt or adapt | Benefit for Kelvo | Effort | Upstream reference |
| --- | --- | --- | --- | --- |
| P0 | Resource-aware query and refresh admission | Prevent a valid set of jobs from collectively exhausting a worker | Medium | [Memory reservation and tracked pools][memory] |
| P0 | Metrics, optional tracing and bounded query/refresh history | Explain latency, source load, resource pressure and failed work | Medium | [Query tracking][tracking], [task history][history] |
| P0 | Independent probes, dataset status and phased drain | Survive overload and routine rolling maintenance predictably | Medium | [Readiness][ready], [health monitor][health], [shutdown][shutdown] |
| P0 | Cross-generation schema contracts and verified restore | Avoid surprising dashboard breakage and make recovery deliberate | Medium | [Schema policies][schema], [snapshot management][restore] |
| P0 | Source quotas, classified retries and credential rotation | Protect source databases and recover without retry storms or broad secret access | Medium | [Refresh retries][retry], [secret providers][secrets] |
| P0 | Recurring conformance, upgrade and recovery gates | Catch wrong results and regressions before release | Medium; mostly test infrastructure | [Validation framework][validation], [release criteria][criteria] |
| P1 | Partitioned Parquet manifests, incremental replacement and safe GC | Refresh/read less data and grow beyond the single-file snapshot model | Large | [Partition pruning][partitions], [append overlap][append], [compaction][compaction] |
| P1 | Durable asynchronous exports | Re-download completed results without re-running expensive source queries | Medium/large | [Object-backed query jobs][jobs] |
| P1 | Opt-in result cache and bounded freshness policy | Reuse identical dashboard results while making data age explicit | Medium | [Cache namespaces][cache], [invalidation][invalidation] |
| P1/P2 | Inspectable federation plans, richer proven predicates and columnar source paths | Reduce transferred data and JSON conversion where measurements justify it | Medium; automatic subplans are large | [Federation policies][federation], [ADBC policy][adbc] |
| P2 | Standard read-only Flight SQL server and authorized catalog browsing | Let compatible notebooks and SQL clients connect through a standard interface | Medium | [Flight SQL metadata][flight], [prepared statements][prepared] |
| P2 | PostgreSQL/MySQL CDC and external change-feed adapters | Keep accelerated datasets current without repeated full extraction | Large | [Durability before source ACK][cdc] |

## First delivery: visibility, bounded execution and maintenance

Implement a small lifecycle event interface and resource-reservation interface
before adding more execution paths. Instrument existing states rather than
creating a second query state machine. Separate queue wait, connection/setup,
source execution/fetch, local computation and result transfer where measurable;
label unknown or overlapping timings rather than deriving misleading totals.

Expose bounded counters/histograms for query outcomes, queue wait, first batch,
source/result bytes, cancellations, lost leases, refresh age, retries, memory and
scratch pressure. Optional trace export and tenant-scoped history should use
bounded buffers and retention. Do not put SQL literals, parameters, DSNs, result
previews or unbounded query IDs into metric labels. Exporter failure must not
block queries. Detailed history should be opt-in on constrained hosts.

Admission must reserve capacity for **all concurrent query and refresh work**,
including native allocations, Arrow buffers, baseline runtime memory and
temporary storage. Initially use conservative configured budgets; SQL cardinality
estimates are not memory enforcement. Separate interactive, export and refresh
budgets so background work cannot consume every slot. Do not increase concurrency
merely because compression reduces network bytes. Keep real container quotas;
per-query cgroups can provide additional containment on suitably delegated Linux
hosts, but are a Kelvo design proposal, not a port of Spice's scheduler.

Spice development code adds a [spill-headroom memory pool][headroom] that reserves
1/16 of its DataFusion pool for other operators. That file is absent from the
reviewed stable release. Its lesson is to leave measured headroom; its algorithm
and fraction should not be transplanted into DuckDB or treated as an RSS cap.

Separate liveness from downstream readiness and query permits. Add authenticated
per-tenant/source/dataset diagnostics, passive health from actual query outcomes,
and optional cheap, rate-limited probes for idle sources. Spice's inspected
source monitor excludes accelerated datasets and certain providers; it is not
universal health coverage. An optional source outage must not remove a shared
gateway serving unrelated tenants. Required dataset or worker-capacity gates
should be operator-selected.

Add a `running -> draining -> stopped` lifecycle. Stop new query/refresh admission,
continue existing result/status/cancel requests and lease renewal, wait a bounded
grace period, then use existing cancellation and process cleanup. Keep diagnostics
available until cleanup completes. Versioned catalog/policy rollout is a separate
contract; draining alone does not make conflicting policies compatible.

**Release gate:** under mixed concurrent exports, joins and refreshes, oversize
work queues or fails explicitly without node OOM, leaked permits or partial
success. Probe liveness during saturation. Roll gateways/workers while queries
run; jobs within the grace window finish, overdue jobs cancel, and no source SQL
is silently replayed. Record throughput, queue p95/p99, process RSS, charged cgroup
memory, OOM events, scratch use and instrumentation overhead on the same fixtures.
An initial telemetry target of at most 3% throughput regression is a proposed
acceptance budget, not an achieved result.

## Dataset safety before incremental acceleration

Persist an Arrow schema contract with each generation. Default to blocking an
incompatible replacement while keeping the last valid generation. Additive
nullable columns and provably lossless widening can be explicit policies.
Decimal scale, timestamp zones/precision, dropped columns, renames and nullability
need distinct decisions; never coerce them merely to make refresh succeed.

Store durable refresh attempts with IDs, sanitized failure categories, last
success and observed source watermark where available. Existing JetStream retries
should remain; improve their decisions. Invalid credentials, incompatible schemas
and bad configuration should not receive the same repeated retry policy as a
temporary network failure. Add bounded backoff/jitter, Retry-After support, source
concurrency budgets and an operator-visible exhausted/permanent state. A limiter
inside each disposable process is insufficient for a source shared by many nodes.

Provide generation inventory, verification and explicit restore under operator
authorization. Check schema, checksum, configuration and authorization epoch
before switching the manifest. Keep the original data timestamp: restoring an
old generation must not relabel it fresh. Source-grant changes must still
invalidate retained copies. Add a backup/restore procedure for both manifests
and their referenced data, with measured RTO/RPO.

**Release gate:** change a source between two individually supported schemas,
revoke authorization, corrupt a current object, restart during publication, and
race restore against refresh. The outcome must be a verified complete generation
or explicit unavailability; the old authorized reader's generation remains
consistent. Permanent refresh errors stop, transient retries remain bounded and
recovery is visible without leaking source diagnostics.

## Scale acceleration with bounded parts and selective work

Evolve the single-file backend into a versioned manifest listing immutable
Parquet parts with schema hash, byte/row counts, content hashes and partition
bounds. A query pins one complete manifest. First support bounded multipart
full refresh and partition replacement; then reuse unchanged parts and add
append-only refresh for appropriate event/history sources.

Watermarks require a stable source identity, tie-breaker/key and explicit late
arrival policy. `updated_at > last_timestamp` alone misses equal timestamps,
late data and some updates; append is not CDC and does not handle deletes by
itself. Keep unsupported partition filters as local residual predicates.

Add storage accounting for active, retained, pinned, staging and orphan bytes.
Remote GC must protect active readers with durable leases or a rigorously bounded
lifetime/quarantine design. Today's no-op remote pruning is intentional safety;
deleting old-looking objects without that contract would be a regression. Keep
row retention, snapshot retention and unreachable-object deletion separate.
Budget small-file compaction independently from interactive queries. Spice's
compaction is useful inspiration; it is not evidence that its object store has
the exact distributed reader-GC contract Kelvo requires.

**Release gate:** query a dataset exceeding 4 GiB while individual parts and
buffers stay bounded; test selective reads across many partitions, late/equal
timestamps, duplicates, interrupted manifest CAS, concurrent readers and GC,
quota/full-disk failures and compaction. Prove reduced source/object bytes and
unchanged exact results; measure the cost of metadata and extra object requests.

## Durable exports and result caching are different features

Keep direct Arrow delivery as the low-overhead default. An opt-in export mode
can write bounded complete Arrow/Parquet chunks to existing object-store
providers and publish a success manifest after all required bytes are durable.
Include owner, authorization epoch, schema, chunk identity/hash, expiry and
source/snapshot identity. A disconnected downloader should retrieve the committed
result again without re-executing its SQL. Resume only at supported complete
chunk/framing boundaries, not arbitrary Arrow stream bytes. Cancellation of
compute and cancellation of one download need separate semantics.

Use a separate result-cache contract for repeated dashboards. Start only with
queries over immutable accelerated generations. Keys must include exact SQL,
typed parameters, tenant, effective authorization, selected generations and
relevant engine/configuration version. Reject incomplete results and unmodeled
volatile/session-dependent queries. Bound entry count, encoded and decoded
bytes, total memory/disk and simultaneous fills. Refresh or revocation during a
fill must prevent stale publication. Live-source caching needs its own TTL or
source-version contract.

Keep fail-closed freshness as the default. An opt-in stale-if-transient-error
window must never bypass schema incompatibility or revoked access. Expose actual
data age and observed source lag separately from refresh completion time.
Do not copy an empty-result fallback blindly: zero rows can be the correct
answer. Any live fallback needs separate authorization and source budgets.

**Release gate:** kill the client/gateway/worker at each commit boundary; only
complete validated exports become available. Repeated reads do not rerun SQL.
Verify expiry, revocation, tenant isolation, storage failure and wide-row memory.
For caching, race refresh/revocation against fill, vary parameter types and test
highly compressible oversize results; measure avoided execution and real latency.

## Federation and client interoperability

Extend the public adapter SDK with an optional, versioned capability contract
and a structured explain result derived from the real bound execution plan.
Show source versus local operators, residual reasons, supported types and
estimates versus measured rows/bytes. Keep credentials and sensitive literals
out of diagnostics. Missing capabilities must conservatively retain local
execution; wrappers must explicitly forward capabilities rather than silently
losing policy through defaults.

Then prove selected decimal/date/timestamp predicates per dialect, preserving
precision, collation, timezone and NULL behavior. Prefer explicit operator-owned
source views for expensive remote pre-aggregations first. Automatic same-source
join/aggregate pushdown requires a bound DuckDB planner integration, compatible
authorization/connection contexts, statistics and dialect conformance. Do not
build a general optimizer through Go string rewriting or apply an early LIMIT
to a relation still participating in a local join.

Spice's full-subplan federation is a useful goal, but it is materially more
complex than adding another scan adapter. Selective runtime join filters also
need cost and size limits; false negatives would change results. This work
should follow explain/conformance, with pushdown-off parity tests.

The existing outgoing Flight SQL connector can gain federation through the
public adapter interface. Separately, optional warehouse Arrow/ADBC retrieval
can reduce JSON decoding for high-volume results. Measure provider permissions,
worker CPU/RSS, first batch, total transfer and exact output against the existing
path. ADBC is a driver interface, not a universal database engine; driver
packaging, licenses and native runtime requirements differ.

A later read-only Flight SQL server can reuse Kelvo's authorized coordinator
for catalog browsing, typed prepared statements and query execution by compatible
ADBC/SQL clients. It must not advertise unsupported DML/transactions. Bound
sessions and tickets, recheck authorization, restrict metadata, and test actual
client versions. This improves interoperability; neither Flight nor QUIC
automatically makes DuckDB execution faster.

## CDC and enterprise operations

Implement CDC after partition manifests, schema policy, storage maintenance and
recovery. Start with PostgreSQL logical replication and MySQL binlog or a public
external change-envelope adapter. Specify primary keys, inserts/updates/deletes,
transaction order, schema versions and snapshot-to-stream handoff. Commit applied
data and its source checkpoint in the same manifest/version, or through an
explicitly recoverable write-ahead protocol, before acknowledging the cursor.
Restart must never advance the cursor past durably published data. Replays must
be idempotent; an expired WAL/binlog position must explicitly require bootstrap.
JetStream refresh acknowledgement is not a database replication checkpoint.

Spice's acknowledgement fence is the transferable idea. Its Cayenne memory
tier is not required. Test every data/checkpoint/ACK crash boundary, duplicate
events, key changes, source failover, deleted history and concurrent writes.
Avoid an unqualified exactly-once claim.

Add optional secret providers with bounded TTL and coalesced fetches in the
trusted parent. Resolve only selected source credentials into each new worker;
never pass secret-manager master credentials into query processes. Overlapping
tenant key IDs, revocation and atomic TLS reload are useful Kelvo additions.
The inspected Spice providers establish a reference for fetching/caching,
not proof that every live connector or certificate rotates transparently.

Before enterprise shared deployments, specify per-user/service authorization,
key revocation and audit retention. Current tenant isolation does not implement
per-user row/column policies. Do not replace it with a simpler upstream API-key
scheme or treat a source's read-only routing hint as a permission boundary.
Catalog/policy versioning, worker enrollment and backup recovery need explicit
rollout contracts; a Kubernetes operator is not required to define them.

## Release gates and delivery order

Make existing live acceptance and public-SDK examples recurring release/upgrade
checks. Use deterministic tests on PRs and bounded real-provider suites for
release candidates. Keep exact Arrow schema, integer, decimal and timestamp
checks alongside logical SQL comparisons. Spice's engine-parity test framework
can intentionally tolerate representations; that is not a reason to weaken
Kelvo's transport fidelity.

Add source-native and independent reference comparisons, required ORDER BY
checks, empty-result review, failure-inclusive benchmark accounting, multi-hour
mixed-load soaks, worker/gateway/broker loss, storage exhaustion, credential
rotation and backup restore. Every connector should distinguish implemented,
protocol-tested, live-validated and sustained-load-validated states. Upstream
release criteria are process inspiration, not evidence that Kelvo meets them.

Recommended sequence:

1. **Operational foundation:** bounded lifecycle telemetry, independent probes,
   resource reservations, phased drain and recurring conformance gates.
2. **Dataset safety:** schema contracts, durable refresh status, classified
   retries/source quotas, verified inventory/restore and rotation contracts.
3. **Efficient scale:** partition manifests, safe GC/compaction and selective
   refresh; durable exports and generation-scoped result caching.
4. **Broader workloads:** measured columnar source paths, safe richer pushdown,
   standard Flight SQL clients, then CDC using the same durability contracts.

Keep optional history, cache, durable exports and new transports independently
configurable. Re-run the small-memory profile after each runtime addition;
features must not quietly make the micro-VM footprint a stale claim. Larger
deployments scale independent jobs across workers, with explicit per-tenant and
per-source budgets. Single-query distribution remains a separate product and
architecture decision.

Defer model/GPU inference, additional accelerator engines, operational-database
writeback and a new DataFusion/Drill/distributed-query runtime. Search can remain
a later focused capability without requiring those runtimes. The current priority
is predictable resource use, data correctness and recoverable operation.

## Source reuse

Spice's repository is [Apache-2.0 licensed][license]. Directly copied or ported
source must preserve applicable attribution/license notices, identify modified
files and carry applicable NOTICE content. Review dependencies individually.
Reusing an idea does not require rebranding or maintaining Spice's entire runtime.
Kelvo remains an independent project by SYNEHQ.

[memory]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime/src/datafusion/builder.rs#L1586-L1636
[headroom]: https://github.com/spiceai/spiceai/blob/a6e0b05acdf39b023c7e0cc134d410909ecf70a3/crates/runtime/src/datafusion/query_memory_pool.rs#L17-L51
[tracking]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime/src/datafusion/query/tracker.rs#L30-L49
[history]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime/src/task_history/mod.rs#L46-L139
[ready]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime/src/http/v1/ready.rs#L138-L193
[health]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime/src/datasets_health_monitor.rs#L330-L360
[shutdown]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime/src/lib.rs#L2070-L2177
[schema]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/spicepod/src/component/dataset.rs#L97-L143
[restore]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime-acceleration/src/snapshot/mod.rs#L2859-L2938
[retry]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime-table/src/accelerated/refresh_task.rs#L597-L659
[secrets]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime-secrets/src/stores/aws_secrets_manager.rs#L169-L193
[validation]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/test-framework/src/queries/validation/mod.rs#L57-L115
[criteria]: https://github.com/spiceai/spiceai/blob/a6e0b05acdf39b023c7e0cc134d410909ecf70a3/docs/criteria/connectors/stable.md
[partitions]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime-table-partition/src/provider.rs#L343-L460
[append]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime-table/src/accelerated/refresh_task.rs#L2039-L2095
[compaction]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/cayenne/src/provider/compaction.rs#L17-L80
[jobs]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime/src/jobs/store.rs#L424-L497
[cache]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime/src/datafusion/query/cache.rs#L505-L548
[invalidation]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime-table/src/accelerated/refresh.rs#L1127-L1155
[federation]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/data_components/src/federation.rs#L68-L132
[adbc]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/data-connectors/connector-adbc/src/table_factory.rs#L42-L129
[flight]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime/src/flight/flightsql/get_tables.rs#L54-L137
[prepared]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime/src/flight/flightsql/prepared_statement_query.rs#L198-L259
[cdc]: https://github.com/spiceai/spiceai/blob/e4da550460ef69b67a93310c5c67810f79cbb2c5/crates/runtime-table/src/accelerated/refresh_task/changes.rs#L86-L107
[license]: https://github.com/spiceai/spiceai/blob/a6e0b05acdf39b023c7e0cc134d410909ecf70a3/LICENSE#L89-L128

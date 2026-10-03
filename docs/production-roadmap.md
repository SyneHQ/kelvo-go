# Production roadmap: industry standards

Source review: **2 October 2026**. The review below records the pre-implementation
baseline; proposals remain pending unless listed in delivery status.

## Delivery status

The first operational slice adds optional shared node query/refresh reservations,
bounded worker execution telemetry and protected resource diagnostics, independent
probes and phased cluster drain. See [operations](operations.md) for configuration,
semantics and limits. This is partial delivery of the operational foundation, not
completion of the P0 roadmap.

The second slice adds strict cross-generation schema checks, verified local
inventory/restore, durable classified refresh retries and operator reset,
distributed source quotas, optional source credential files, bounded execution
history and actual-scan diagnostics.

The third slice adds protected interactive slot/memory/scratch reserves against
background refreshes; authenticated dataset metadata diagnostics and opt-in
required-dataset readiness; and verified remote restore using a bounded retained
manifest catalog. Remote history retains at most 16 previous generations plus
current, subject to the manifest byte limit. It does not list or delete objects.
Current remote writes emit v4 at the first writer action, requiring
coordinated readers/writers and preventing rollback to older binaries afterward.
Required readiness checks cached metadata, not payload integrity or source health.

Optional [lifecycle tracing](tracing.md) is also available, disabled by default,
with sampled local query/refresh spans, bounded export queues and fixed attributes
that exclude SQL, parameters, source/tenant identities and raw errors. It does not
provide distributed trace continuity or full queue/stage timings. Focused tracing,
credential-rotation and protocol-fixture checks are development correctness gates,
not capacity measurements or provider-wide validation.

An additional implementation slice adds opt-in [multipart acceleration](multipart-acceleration.md)
for local and remote storage: bounded immutable Parquet parts, one atomic
generation pointer, exact-file or isolated-range DuckDB consumption and schema/
recovery integration. Local single-file manifests remain v1; local multipart uses
v2. Remote readers accept v2/v3/v4, and every writer action emits v4, including the
first lease claim before extraction. Coordinated upgrades are required; disabling
multipart does not enable rollback to older remote readers.

The local ordinary/bridge, crash-recovery, sandbox and over-4-GiB development gates
passed; linked evidence states their scope. Remote multipart also passed its
[20-check TLS fixture matrix](evidence/object-multipart-acceptance.json), legacy
object regressions and [over-4-GiB component gate](evidence/object-multipart-large-dataset.json),
including real DuckDB/HTTPFS reads. Remote multipart stages/uploads one part at a time and publishes
a bounded descriptor/root reference; it adds no incremental refresh or remote GC.
These checks do not establish actual cloud-provider acceptance or general
production capacity.

Opt-in [schema evolution](schema-evolution.md) now permits append-only nullable
fields and a conservative numeric widening allowlist through independent flags.
Strict remains default; within-generation integrity and restore remain exact.
Versioned policy fingerprints fence configuration changes, and actual drift errors
now stop durable retries as permanent schema failures. The eight-case sandboxed
CLI acceptance and four storage/layout publication paths passed development checks.

The [storage release runner](storage-conformance.md) passed its complete opt-in
matrix: named correctness checks, both larger-than-4-GiB datasets and 56 TLS
protocol checks, with stable source-input hashes and completed cleanup.
[Native error classification](native-error-classification.md) now preserves typed
PostgreSQL/MySQL access failures and trusted sink errors; isolated real TLS tests
confirmed that authentication and revoked-grant refresh failures preserve prior
snapshots. Unsupported native result types stop as permanent configuration
failures. These additions do not establish live coverage for every adapter.

Further correctness work rejects late source-quota cancellation before worker
success and classifies typed DuckDB analytical memory failures as permanent
resource failures. Sandboxed worker/refresh tests cover prior-snapshot
preservation, admission cleanup and reset recovery for both local layouts.
Single-file verification now checks actual Parquet footer rows and schema in
addition to payload hashes, matching multipart integrity requirements; remote
verification requires range-read support. These are development correctness
checks, with injected quota loss and in-memory retry-state fixtures.

The pinned native federation bridge now lowers bounded optional sparse integer
IN hints to exact equality trees while retaining DuckDB residual evaluation.
Typed NULL/OR semantics, mandatory filters and aggregate plan budgets are covered
by real planner tests. Opt-in [CSV buffers](csv-memory.md) also let operators
reduce scanner allocation with an explicit supported line-size policy. Both
features preserve the existing execution and authorization boundaries.

[Verified local snapshot backup/recovery](snapshot-backup.md) now copies one
current dataset to a new private root, preserving its authorization fingerprint,
exact payloads and original age. Atomic no-replace publication, source leases and
explicit post-publication uncertainty protect recovery while refreshes continue.
The sandboxed recovery runner deletes only its generated source and live store,
then compares every recovered Arrow value, type and NULL. This is a local
correctness gate; cross-host replication and service failover remain separate.

Provider-wide conformance/rotation coverage, full queue/stage timing,
export-specific admission, safe remote GC,
remote backup recovery and sustained fault/capacity gates remain pending. The validation
records in [operations](operations.md) identify their tested runtime commits;
implementation status does not extend those results to later unvalidated changes.
No new throughput results are claimed.

Recommendation: keep Kelvo's Go coordinator, DuckDB execution, Arrow output,
Parquet snapshots and NATS dispatch. Adopt the dataset lifecycle, resource
accounting, diagnostics and recovery mechanisms that make those components
dependable. Adding another query engine is not a prerequisite.

## Review scope

The review covers Kelvo
[`90ade2e`](https://github.com/SyneHQ/kelvo-go/tree/90ade2e2092a90a05a65e20628cd350079f36373)
and applies industry standards and established operational practices to its
production roadmap. References cover Arrow, OpenTelemetry, HTTP caching,
database durability, resource management and reliability testing. They inform
the proposed design; they do not certify Kelvo's implementation or prescribe
every application-specific policy below.

Areas inspected include acceleration, schemas, snapshots, cache invalidation,
CDC acknowledgement, query tracking, readiness, shutdown, memory accounting,
secret providers, retries, federation policies, Flight SQL, durable jobs and
test infrastructure. Planned capabilities require their own implementation,
conformance checks and workload-specific validation before release.

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

The table below records the **reviewed baseline at `90ade2e`**, before the
implementation slices listed in [Delivery status](#delivery-status). It is not a
statement of current feature availability; consult [operations](operations.md)
for the implemented behavior and remaining limits.

| Area | Reviewed baseline boundary | Consequence at review |
| --- | --- | --- |
| Resource admission | [Node dispatch](../internal/cluster/node.go) counts jobs; [query limits](../internal/query/types.go) apply individually | Several individually permitted queries plus refresh work can exceed a node's actual memory or scratch capacity. |
| Health | [Gateway routes](../internal/cluster/gateway.go) take an HTTP permit before `/health` and `/ready`; readiness checks tenant reconciliation | Saturation can return 429 to probes. Source health, usable snapshots and sufficient worker capacity are separate, currently incomplete signals. |
| Diagnostics | Per-query `Stats` exist; no first-class metrics/trace/history service | Expired query handles cannot explain recurring queue delays, source errors, pressure or failed refreshes. |
| Shutdown | [Node close](../internal/cluster/node.go) and [cluster shutdown](../cmd/kelvo/cluster.go) cancel active work | Safe cancellation exists; an explicit stop-admission/finish-in-flight deployment mode does not. |
| Acceleration | [RefreshWriter](../internal/acceleration/backend.go) models one file; [catalog validation](../internal/catalog/acceleration.go) caps object snapshot `max_bytes` at 4 GiB | Every refresh extracts a complete result. Large datasets need bounded parts and selective replacement, not a larger monolithic buffer. |
| Schema and storage | [Parquet sink](../internal/acceleration/parquet.go) validates each refresh; manifests do not record a cross-generation schema contract. Remote [Prune](../internal/acceleration/object_store.go) intentionally never deletes | A valid but changed source schema lacks an evolution policy. Remote storage reclamation needs reader protection before deletion is safe. |
| Delivery | Cluster results are tied to a single consumer; no persistent result catalog/cache | A client disconnect can waste expensive work. Repeated dashboards still execute the same analysis. |
| Federation | Eight adapters expose projected scans and integer/Boolean predicates | Other predicates and all joins/aggregates remain local; warehouse paths currently decode JSON pages. |

These are source-observed boundaries at the reviewed revision, not claims that
every listed failure was reproduced. DuckDB still completes execution before Arrow delivery through
the pinned Go driver; changing the transport alone does not remove that memory
boundary.

## Priority matrix

P0 means a foundation for a dependable production offering. P1 improves scale,
recovery or workload cost after those foundations. P2 depends on demonstrated
customer workloads. Effort is relative engineering scope, not a delivery promise.

| Priority | Feature to adopt or adapt | Benefit for Kelvo | Effort | Standards and practice references |
| --- | --- | --- | --- | --- |
| P0 | Resource-aware query and refresh admission | Prevent a valid set of jobs from collectively exhausting a worker | Medium | [Workload and memory management][memory] |
| P0 | Metrics, optional tracing and bounded query/refresh history | Explain latency, source load, resource pressure and failed work | Medium | [Metrics][tracking], [log records][history] |
| P0 | Independent probes, dataset status and phased drain | Survive overload and routine rolling maintenance predictably | Medium | [Probe semantics][ready], [HTTP shutdown][shutdown] |
| P0 | Cross-generation schema contracts and verified restore | Avoid surprising dashboard breakage and make recovery deliberate | Medium | [Arrow schemas][schema], [conditional publication][restore] |
| P0 | Source quotas, classified retries and credential rotation | Protect source databases and recover without retry storms or broad secret access | Medium | [Refresh retries][retry], [secret providers][secrets] |
| P0 | Recurring conformance, upgrade and recovery gates | Catch wrong results and regressions before release | Medium; mostly test infrastructure | [SQL conformance tests][validation], [reliability testing][criteria] |
| P1 | Partitioned Parquet manifests, incremental replacement and safe GC | Refresh/read less data and grow beyond the single-file snapshot model | Large | [Partition pruning][partitions], [incremental models][append], [file maintenance][compaction] |
| P1 | Durable asynchronous exports | Re-download completed results without re-running expensive source queries | Medium/large | [Arrow IPC files][jobs], [conditional publication][restore] |
| P1 | Opt-in result cache and bounded freshness policy | Reuse identical dashboard results while making data age explicit | Medium | [Caching semantics][cache] |
| P1/P2 | Inspectable federation plans, richer proven predicates and columnar source paths | Reduce transferred data and JSON conversion where measurements justify it | Medium; automatic subplans are large | [Pushdown][federation], [ADBC][adbc] |
| P2 | Standard read-only Flight SQL server and authorized catalog browsing | Let compatible notebooks and SQL clients connect through a standard interface | Medium | [Flight SQL protocol][flight] |
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
hosts; this remains a proposed extension to Kelvo's current container controls.

Leave measured headroom for readers, Arrow buffers and other allocations outside
DuckDB's managed memory. Choose that reserve from workload measurements rather
than treating a fixed fraction or an engine memory setting as a process RSS cap.

Separate liveness from downstream readiness and query permits. Add authenticated
per-tenant/source/dataset diagnostics, passive health from actual query outcomes,
and optional cheap, rate-limited probes for idle sources. Source reachability,
snapshot freshness and query-serving capacity need separate checks.
An optional source outage must not remove a shared
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
Budget small-file compaction independently from interactive queries. Validate
the distributed reader-GC contract against Kelvo's actual object publication
and query lifetime guarantees before enabling deletion.

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
An empty result must not automatically trigger source fallback: zero rows can
be the correct answer. Any live fallback needs separate authorization and source budgets.

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

Full-subplan federation is materially more complex than adding another scan
adapter. Selective runtime join filters also
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

Test every data/checkpoint/ACK crash boundary, duplicate
events, key changes, source failover, deleted history and concurrent writes.
Avoid an unqualified exactly-once claim.

Add optional secret providers with bounded TTL and coalesced fetches in the
trusted parent. Resolve only selected source credentials into each new worker;
never pass secret-manager master credentials into query processes. Overlapping
tenant key IDs, revocation and atomic TLS reload are useful Kelvo additions.
Secret caching and live credential rotation are separate contracts. Verify
rotation for each connector, token and certificate lifecycle explicitly.

Before enterprise shared deployments, specify per-user/service authorization,
key revocation and audit retention. Current tenant isolation does not implement
per-user row/column policies. Do not replace it with a simpler shared API-key
scheme or treat a source's read-only routing hint as a permission boundary.
Catalog/policy versioning, worker enrollment and backup recovery need explicit
rollout contracts; a Kubernetes operator is not required to define them.

## Release gates and delivery order

Make existing live acceptance and public-SDK examples recurring release/upgrade
checks. Use deterministic tests on PRs and bounded real-provider suites for
release candidates. Keep exact Arrow schema, integer, decimal and timestamp
checks alongside logical SQL comparisons. Logical equivalence checks and
transport type fidelity are separate requirements; numeric tolerances must not
hide changed integer, decimal or timestamp values.

Add source-native and independent reference comparisons, required ORDER BY
checks, empty-result review, failure-inclusive benchmark accounting, multi-hour
mixed-load soaks, worker/gateway/broker loss, storage exhaustion, credential
rotation and backup restore. Every connector should distinguish implemented,
protocol-tested, live-validated and sustained-load-validated states. Industry
standards guide the release process; evidence must establish that Kelvo meets
its stated acceptance criteria.

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

## Licensing

Kelvo is an independent project by SYNEHQ under [Apache-2.0][license]. Preserve
applicable third-party attribution, license and modification notices when
adopting external source, and review dependencies individually.

[memory]: https://duckdb.org/docs/stable/guides/performance/how_to_tune_workloads.html
[tracking]: https://opentelemetry.io/docs/specs/otel/metrics/
[history]: https://opentelemetry.io/docs/specs/otel/logs/
[ready]: https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/
[shutdown]: https://pkg.go.dev/net/http#Server.Shutdown
[schema]: https://arrow.apache.org/docs/format/Columnar.html
[restore]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
[retry]: https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/
[secrets]: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
[validation]: https://duckdb.org/docs/stable/dev/sqllogictest/intro.html
[criteria]: https://sre.google/sre-book/testing-reliability/
[partitions]: https://duckdb.org/docs/stable/data/partitioning/hive_partitioning.html
[append]: https://docs.getdbt.com/docs/build/incremental-models
[compaction]: https://iceberg.apache.org/docs/latest/maintenance/
[jobs]: https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format
[cache]: https://www.rfc-editor.org/rfc/rfc9111
[federation]: https://trino.io/docs/current/optimizer/pushdown.html
[adbc]: https://arrow.apache.org/adbc/current/
[flight]: https://arrow.apache.org/docs/format/FlightSql.html
[cdc]: https://www.postgresql.org/docs/current/logicaldecoding-explanation.html
[license]: https://www.apache.org/licenses/LICENSE-2.0

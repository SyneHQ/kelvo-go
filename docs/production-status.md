# Production delivery checklist

This checklist separates implemented safeguards from release acceptance and
later product features. It supplements the [production roadmap](production-roadmap.md)
and [validation record](validation.md); it does not certify a deployment as
production ready. Source review: **3 October 2026**.

The current foundation includes tenant-bound workers, exact supported Arrow
values, immutable snapshots and resource controls. The remaining work is larger
than a documentation update: distributed storage maintenance, durable results,
CDC and per-user policy each require their own safety contracts and acceptance.
A checklist item is complete only when implementation, relevant correctness
checks and any required live release gate are complete. Track ownership, dependencies
and evidence on the [production board](https://github.com/orgs/SyneHQ/projects/3)
and [delivery tracker](https://github.com/SyneHQ/kelvo-go/issues/32); the
[delivery process](delivery-process.md) defines status and completion rules.

## Operational foundation

| Deliverable | Current implementation | Remaining acceptance or capability |
| --- | --- | --- |
| Aggregate query/refresh budgets | Shared [admission pool](../internal/admission/pool.go), node memory/scratch accounting and protected interactive reserves; held through cleanup/publication; [five-minute local mixed-load gate](operational-acceptance.md#recorded-validation) passed | Separate export class, per-query cgroup containment and workload-sized sustained capacity gates |
| Independent probes and maintenance | Gateway probes bypass permits; worker dataset readiness; bounded gateway/node drain and cancellation; saturation, recovery and accepted-query node-drain fixture passed; [gateway/worker/broker process-loss acceptance](process-loss-acceptance.md) passed on the recorded candidate | Rolling cluster upgrade campaigns, multi-hour fault/load evidence and explicit capacity-readiness policy if needed |
| Diagnostics and history | Fixed-cardinality lifecycle and reached local-stage metrics, optional bounded local history, actual federation scan diagnostics | Full distributed queue/source/local execution attribution, durable audit retention and measured instrumentation overhead |
| Passive source observations | Opt-in [native outcome diagnostics](source-health.md), bounded per-node registry, TTL and internal mTLS scope | Live-provider classification/rotation acceptance; active rate-limited probes remain absent |
| Source pressure and refresh recovery | Distributed [source quotas](../internal/cluster/source_quota.go), classified durable retry state and operator reset | Retry/credential lifecycle acceptance across every connector and provider; saturation/fault soaks |
| Credentials | Selected-secret forwarding, bounded [private source credential files](operations.md#file-based-source-credential-rotation) and [live gateway key rotation](gateway-key-rotation.md) with a passing two-gateway fixture; opt-in [atomic TLS identity rotation](tls-identity-rotation.md) | Cloud secret-provider integrations, global/persistent revocation coordination, CA/trust rotation, peer revocation and explicit enrollment/policy rollout |
| Release correctness | Scheduled CI, native bridge race/ownership gates, sandbox/cluster fixtures, storage release runner, 15 executed notebooks, repeatable operational acceptance and a [preview/candidate snapshot upgrade matrix](release-upgrades.md) | Recurrent real-provider suite, rolling cluster upgrade acceptance and failure-inclusive multi-hour mixed-load evidence |

Opt-in Linux [managed worker scratch](worker-scratch.md) reclaims complete owned
workspaces only after parent and child leases are released. It does not sweep
legacy temporary directories or provide independent hostile-process containment.

Already implemented controls are configurable; enabling them is not evidence of
safe capacity on a particular host. Admission reserves configured budgets rather
than enforcing process RSS. DuckDB materializes execution before Arrow delivery,
so output streaming does not bound native query memory. Keep real host/container
limits and leave measured headroom for Arrow, native libraries and runtime work.

## Dataset safety and scale

| Deliverable | Current implementation | Remaining work |
| --- | --- | --- |
| Immutable full refresh | Local/remote snapshots, fenced publication, selected authorization fingerprints, pinned readers and previous-generation preservation | Live acceleration acceptance for sources without linked evidence |
| Schema policy | Exact cross-generation default; opt-in append-only nullable fields and conservative numeric widening | Wider evolution policies only with separate correctness proof; no silent type coercion |
| Multipart snapshots | Bounded immutable parts and atomic generation pointer; local/remote reads and over-4-GiB development gates | Partition-aware selective replacement, reuse, append-only refresh and explicit source watermarks/late-data policy |
| Inventory and restore | Verified local and bounded remote history restore; exact checksums/footer schema/rows and original age | Deployment recovery procedures and provider-specific corruption/CAS/restore acceptance |
| Independent backup | [Local backup/recovery](snapshot-backup.md) and explicit remote-to-local migration, verified no-replace publication, preserved data age and matching source policy; signed TLS migration fixtures passed | Recovery into remote destinations, cross-host service cutover, cold-cache evidence and workload-specific RTO/RPO |
| Storage reclamation | Local generation pruning with pinned-reader protection; remote prune deliberately does not delete | Durable remote reader protection, orphan accounting, quarantine/GC and independently budgeted compaction |

Remote deletion must not be enabled by merely listing old objects. The
[remote backend](../internal/acceleration/object_store.go) has no distributed
reader-deletion contract. A new collector must protect current, retained, pinned,
staging and orphan data through crashes and concurrent publication. Existing
remote v4 manifests also require coordinated writer/reader upgrades.

## Results and interoperability

| Capability | Status | Required contract before completion |
| --- | --- | --- |
| Direct Arrow delivery and opt-in LZ4 | Implemented | Continue exact-value and failure-inclusive measurements; compression does not increase decoded limits |
| Durable asynchronous export | Not implemented | Bounded committed parts, hashes/schema, owner/auth epoch, expiry, redownload without SQL replay and independent download cancellation |
| Result cache | Not implemented | Immutable-generation keys including SQL/typed parameters/tenant/effective authorization/configuration; bounded fills/storage and refresh/revocation fencing |
| Eight native federation adapters | Implemented with supported types, projected scans, integer/Boolean pushdown, required filters and local residuals | Live Oracle/warehouse gates; measured richer predicates and columnar retrieval where justified |
| Automatic remote joins/aggregates | Not implemented | Bound-plan and authorization-compatible pushdown with pushdown-off parity; string SQL rewriting is insufficient |
| Flight SQL client connector | Implemented | Live provider/client coverage for intended deployments |
| Read-only Flight SQL server | Not implemented | Bounded sessions/tickets/prepared statements, authorization rechecks and restricted metadata; actual client conformance |
| PostgreSQL/MySQL CDC | Not implemented | Keys, transaction order, source checkpoint/publication atomicity, replay idempotence and snapshot-to-stream handoff |
| Per-user row/column policy | Not implemented | Explicit user/service authorization, revocation and audit retention; current tenant isolation is a different boundary |
| Single-query distribution | Outside the chosen design | Kelvo distributes independent queries across workers; one DuckDB query runs on one worker |

## Gates that can run on the dedicated test hosts

These gates require existing build tools, fixtures and explicit resource limits.
Run builds and package downloads on the designated Linux test VM, as required by
[contributor instructions](../AGENTS.md).

- Complete ordinary and native-bridge tests, race checks, vet and the public
  adapter SDK example using the pinned dependency versions.
- Repeat the storage release matrix, schema evolution, worker failure and local
  backup/source-loss acceptance against the actual release revision.
- Run concurrent interactive joins, native exports and scheduled refreshes with
  slow readers and cancellation; retain failures and report queue tails, process
  RSS, charged cgroup memory, OOM events, scratch usage and exact results.
- Terminate gateways/workers/brokers during admitted jobs and snapshot publication;
  verify lease cleanup, bounded cancellation, surviving authorized readers and
  no silent SQL replay. Process fault tests must be isolated from other services.
- Compare telemetry disabled/enabled on the same workload, and rerun the Oracle
  micro profile after runtime changes. Previous throughput numbers belong to
  their recorded binaries; passing new unit tests does not refresh them.

Entry points include [CI](../.github/workflows/ci.yml),
[storage conformance](storage-conformance.md),
[cluster acceptance](../scripts/cluster_acceptance.py),
[worker failures](worker-failures.md), [backup recovery](snapshot-backup.md),
[analytical workflows](analytics-workflow-benchmarks.md) and
[micro-VM capacity](oracle-micro-capacity.md).

## Gates requiring real provider access or deployment decisions

- Oracle TCPS, Snowflake, BigQuery and Databricks live federation still need
  provider-issued credentials, intended grants, TLS and cancellation checks.
  Verified protocol fixtures cannot substitute for these accounts.
- Real S3, R2, Azure Blob and GCS acceptance needs dedicated buckets/prefixes,
  scoped writer/reader identities and permission to create/remove owned fixtures.
  TLS protocol matrices cover behavior, not each provider's live deployment.
- Every native/compatible engine needs its own acceleration, credential rotation,
  revoked-grant, supported-type and sustained-load evidence. A protocol-family
  alias does not establish product-specific validation. Consult
  [source coverage](source-coverage.md) before making a support claim.
- Shared enterprise rollout needs declared tenant/user policy, key/certificate
  rotation, recovery objectives, topology, storage retention and capacity budgets.
  These are operator/product contracts, not values a test runner can infer.

## Completion rule

Keep each release record tied to the actual source revision, binary and fixture
hashes, with named passed, failed, skipped and unrun gates. Preserve explicit
limits and failed measurements. Developer correctness, provider acceptance and
sustained production capacity are separate evidence levels. Complete the
operational release gates before advertising a broadly production-validated
multi-tenant service; implement later feature families without weakening the
existing authorization, exact-value or durability boundaries.

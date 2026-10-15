# Control-plane diet for single-call reads

Status: store, grant cache, audit group commit and the node's result write are
built and measured here. The remaining distance to the owner's targets is a
protocol change, described at the end and **not built**.

Starting point: PR 224's profile (`docs/operations/profile-plain-read.md`): at 50
concurrent, 94% of about 19 ms CPU per plain read is the control plane around the
read (job store on JetStream over TLS, grants, audit), 1.93 MB allocated per read,
about 21 broker round trips plus polls, 6 audit fsyncs, 9-20 Ed25519 verifications.

## What was cut, and why it is safe

| Change | Before | After | Guarantee kept |
| --- | --- | --- | --- |
| `NATSStore.CompareAndSwap` no longer reads the record first | get + update per transition | update only | The update still fails on any newer revision. A snapshot carries the record it was read at and the checks run against a private decode of it, so a caller mutating a pointer inside it (trace, authority, parameters) still cannot change what is compared (`TestClusterTraceDurableAdmissionAndImmutableCAS`). A snapshot not produced by the store is compared against a fresh read. |
| `Submit` probes from a rotating slot and reuses a slot this process saw finish, one update | a create and a get per occupied slot in the strict pass: **209 gateway messages per read** with every slot recently used | 1 | Same admission rule (`max_queries`), same preference for aged slots over fresh terminal ones; only the order of the probe changed. |
| Single-call jobs are marked `Transient`; a **completed** one is reusable at once | slot held for the lease window | free | The caller reads the outcome from the response (receipt trailer), never polls it. A transient job that ended before any claim is free at once (the node runs only Claimed jobs, so nothing can be executing, and holding it would let a retry storm of refused-then-cancelled reads fill `max_queries`). One cancelled or failed after a claim keeps the lease window, in the saturated pass too: its node may still be executing until its next lease tick (up to about a second), and early reuse would let that work run outside `max_queries` (`TestNATSStoreTransientSlotAfterCancelOrFailureIsHeldOnlyIfClaimed`, all four combinations). Confirmable (three-call) terminal jobs keep the window; a completed transient slot is taken first (`TestNATSStoreCompletedTransientSlotIsReusedBeforeAConfirmableOne`). |
| A job just submitted is enqueued without reading it back | get + publish | publish | It is Queued by construction; the message ID makes a repeat harmless. |
| Node publishes the result from its own last write | get + get + update | update (falls back to a read on a conflict; at most one wasted update per call, since the hint is used once) | A newer revision (cancellation, claim) conflicts and the attempt repeats from a read. |
| Verified-grant cache in `internal/delegation` (`VerifyCached`, `VerifyClaimsCached`) | 65 us, 14 KB, 351 allocations per verification, about six per read on the gateway and again on the node | once per token per process; a hit is 1.1 us, 1.2 KB | Keyed by token digest, trust (issuer, audience, tenant, principal, key) and, when bound, the exact request. The claims' two clock checks run on every hit, so a grant is never accepted past its own expiry; failures are not cached. The principal's current authority, the request's key authorization and the job's stored authority are still checked on every read. A grant has no per-token revocation to miss: it ends by expiry, or when its principal or trust changes (a different key here). |
| Audit group commit (`internal/audit`) | one fsync per begin and per finish: 6 per read | operations that arrive together share one; `write_timeout` must now be at least 52 ms (the window plus 50 ms for an fsync) | No operation completes and no receipt escapes before the sync covering its writes returns; a failed sync fails every operation that wrote with `ErrUncertain` and faults the journal. Header and `Config` untouched (the 2 ms window is a constant). A lone operation is synced at once, even right after a group (`TestAuditLoneOperationAfterAGroupIsNotDelayed`). |
| Result relay copy buffer pooled | 32 KiB per read | 0 | none involved |

## Measured here: a store-call replay, not an end-to-end measurement

Local JetStream (nats 2.10, TLS), 64 slots all holding recently finished jobs, 16
concurrent replays of the store calls one single-call read makes
(`TestMeasureTransientReadStoreCost`, `KELVO_MEASURE_READS=640`). The replay is
hand-written in the order the gateway and node perform the calls. It leaves out
the node's lease renewals, the resolver's lease reads (each custody check is
several gets), the gateway's result-wait polls and re-reads, audit, and the
result stream, so its totals are lower bounds of what a read costs. It now fails
if the store's cost regresses. Real per-read CPU, allocations and broker messages
need the profile's harness on the integration branch.

| | Before | After |
| --- | --- | --- |
| Broker messages per read, gateway (replay) | 208.9 | 6.0 |
| Broker messages per read, node (replay) | 11.0 | 7.0 |
| **Broker messages per read, total (replay, excluding polls, renewals, resolver reads)** | **219.9** | **13.0** |
| Allocations per read (store-call replay) | 806 KB, 10,103 objects | 45 KB, 600 objects |
| CPU per read (store-call replay, includes the NATS client) | 10.4 ms | 0.71 ms |

Ed25519 verification (`BenchmarkVerifyUncachedVersusCached`): 65.4 us, 14.2 KB, 351
allocations uncached; 1.09 us, 1.2 KB, 20 allocations cached. Verifications per
read: gateway 1, node-side cluster code 1 (in one process they share the cache).
`internal/worker/worker.go` calls the uncached `delegation.Verify` twice more per
read (`delegatedExecution`, lines 205 and 224); it is owned by the read-path
session and should switch to `delegation.VerifyCached`.

Audit fsyncs (`TestAuditFsyncsPerOperationByLoad`, real fsync, 6 operations per read):

| Arrival pattern | fsyncs per operation | per read |
| --- | --- | --- |
| 1 caller back to back | 1.00 | 6.00 |
| 4 callers, about 100 operations a second | 0.69 | 4.12 |
| 8 callers back to back | 0.12 | 0.75 |
| 50 callers, 5,000 operations a second | 0.04 | 0.23 |
| 50 callers back to back | 0.02 | 0.12 |

The gateway handler alone, with a fake store and worker (`BenchmarkStreamQueryHandler`):
41.8 KB and 175 allocations per read.

## What is not met, and which guarantee blocks it

**Broker round trips, target 5; now 13 plus polls.** The remaining operations are
the job's durable lifecycle: submit (1), queue publish (1), a poll or two (about 2),
claim (1), commit read and update (2) on the gateway; fetch, read, assign, ack,
start read and update, result update (7) on the node. Every one is a transition
of the durable job record that the **credential resolver's custody check** reads:
`connectionLease` (gateway) answers the resolver only if the durable record says
this worker owns this running job. Removing the record (ephemeral, in-memory job
state) would leave nothing for that check to read, and the resolver's lease calls
reach whichever gateway instance db-api talks to, not necessarily the one holding
the request. That is a change to the custody protocol, not to the store.

**Audit fsyncs, target at most 1 per read amortised.** Met when operations arrive
quickly (0.12 per read at 50 back-to-back callers) and not at the profiled rate:
at about 100 operations a second a 2 ms window rarely holds a second operation
(4.12 per read). Amortising there would need a longer window, or acknowledging a
record before it is durable; the owner's rule forbids the second and bounds the
first. The gateway writes four of the six operations per read (submit and results,
each begin and finish); a single audit record per single-call read would cut that
to two, but it adds an audit kind, which is a journal format change.

**CPU 6 ms and 300 KB, and Ed25519 at most 2 per read, at 50 concurrent.** The
components are measured above, the end-to-end figure needs the same harness as the
profile (run 38791531066 and 38793567524).

## Plan for 5 round trips (not built; needs a decision)

Keep a durable job record, but write it only where a check reads it, and push the
job to the node instead of queueing it:

1. Gateway picks a node from its configured workers (round robin, next on a
   capacity refusal) and creates the job record directly as `Claimed` with that
   `WorkerID` and a gateway-minted claim, in one create or one update of a freed
   slot. **1 round trip.**
2. Gateway posts the request to a new node endpoint over the existing mTLS
   gateway identity, carrying the job ID and claim. The node admits it under its
   own capacity with a bounded wait and returns a retryable refusal otherwise.
3. Node publishes its owner into the record (one update, revision known from the
   response, **1**), runs, publishes `ResultReady` (**1**).
4. Gateway commits `Succeeded` (read and update, **2**).

Total 5, no queue message, no fetch, no ack, no polls. The resolver's custody check
still finds a durable record. It changes: the state machine (a `Claimed` start for
transient jobs), who chooses the node (the gateway, losing the pull model's natural
balancing), the gateway/node protocol (one new endpoint), and the per-gateway
bound on queued transient reads (today enforced by the shared slot count; here
each gateway also needs an in-flight limit). It needs the owner's decision and a
security review of the new endpoint; it is not in this PR.

## What ephemeral job state would take, if the end-to-end numbers miss

If the profile harness shows CPU above 6 ms, allocations above 300 KB or more than
5 broker round trips per read, the lever is the durable record itself, and the plan
above is the smallest version that keeps what the resolver reads. A fully
in-memory job would additionally need:

1. A custody proof the resolver can check without the record: for example the
   gateway signing a short-lived statement "this node runs this read" that
   db-api verifies, replacing `connectionLease`'s read of the record. That changes
   the resolver protocol on both sides (db-api and the SDK's lease validator).
2. A single owner for a read's state, since the resolver's lease calls reach any
   gateway instance: sticky routing of lease calls by job ID, or shared state
   again.
3. A per-gateway in-flight bound standing in for `max_queries`, and the loss of
   the queue's global fairness across nodes.
4. Cancellation and loss handling without `Reconcile`: the node's lease tick and
   the gateway's request context become the only fences.

Each of these is a security-reviewed protocol change; none is in this PR.

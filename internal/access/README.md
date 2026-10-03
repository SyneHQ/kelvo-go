# Row and column enforcement

This package accepts a trusted, selected-source policy from the worker envelope.
It is not a public SQL or query-request option. The authenticated principal's
whole-source grants and durable policy version remain authoritative.

Call `WithPolicy` to validate and freeze a detached policy. Call `ValidateRequest`
before resolving acceleration snapshots, credentials, or choosing an executor
in both parent and child. An omitted context preserves existing whole-source
authorization; an empty policy is invalid. `PolicyFromContext` and `Lookup`
return copies, so callers cannot mutate active authority.

Every restricted table requires an explicit column list and exactly one of a
typed row predicate or `all_rows: true`. Unlisted tables in a restricted source
are unavailable and never undergo schema discovery. Restrictions cannot be
replaced by a public query predicate.

## Enforcement boundary

The first implementation accepts callback federation sources only. A restricted
query selecting any raw database attachment, CSV, Parquet, SQLite, DuckDB file,
object range, or accelerated snapshot fails closed. Restricted native SQL is
also unsupported. Those surfaces need their own sound pre-query boundary.

The source's complete schema remains private. Only allowed flat columns are
registered with DuckDB, with schema and field metadata removed. Each scan adds
the policy's hidden columns to its private source projection, applies the row
predicate in Go, and removes those columns before returning Arrow batches.
Joins, aggregates, windows, and residual expressions run after this step.

DuckDB can remove predicates once it pushes them into a scan. The guard therefore
evaluates every such predicate locally in addition to the mandatory policy.
Restricted scans currently send **no filters** to the source adapter. No adapter
capability assertion or optimizer decision can omit the local checks. This can
increase source traffic; remote pushdown is a future optimization that must
preserve local verification and exact semantics.

Policy comparisons support exact Boolean and signed/unsigned integer widths,
UTF-8 byte comparisons, NULL checks, and bounded AND/OR expressions. Comparisons
against NULL do not authorize a row. String policies do not inherit database
collations. Decimal, floating-point, timestamp, and other unsupported predicate
semantics are rejected. Those flat data types can still be projected unchanged.
Nested, dictionary, and extension projections remain unsupported.

Each scan owns its reader, cancellation, selection buffers, and output batches.
The guard uses Arrow's selection kernel only on visible columns; borrowed input
batches remain owned by the source reader. Source scan limits count raw input
and fail on overflow instead of truncating the authorized relation.

The raw scan reader checks cumulative row and Arrow-byte budgets before handing
any batch to the guard, including when the policy would discard every row. For
a batch of `n` rows, guard work retains that bounded input, uses a Boolean mask
(one bit per row plus Arrow capacity/alignment), an index selection of at most
eight bytes per selected row plus alignment, and copies selected visible
column buffers. Builder capacity growth, field buffers, and Go object headers
add overhead. No complete result is retained, and cancellation is checked every
1,024 predicate rows. This is a per-batch allocation model, not a total RSS cap;
the worker's admission and OS containment limits remain necessary.

## Operational limits

Pre-policy scan counts, transport-byte counts, and scan diagnostics are withheld
for restricted queries. Timing, resource exhaustion, and source-side activity
are not a noninterference guarantee. Compiled adapters remain trusted code;
operators must not expose the same underlying data through an unrestricted
source alias or grant broader native access to bypass a restricted source.

Package tests cover ownership, NULLs, exact integer boundaries, byte-sensitive
strings, unsupported types, metadata, cancellation, and concurrent rescans.
Pinned-DuckDB tests additionally exercise CTEs, joins, aggregates, windows,
hidden-row expressions, hidden-table discovery, and direct-reader rejection.
These fixtures do not establish live-database policy acceptance or complete
row/column support for every Kelvo execution mode.

# Durable export storage foundation

`internal/exports` provides a local storage primitive for future durable query
results. It is an internal contributor API. **No current CLI, HTTP endpoint or
cluster job exposes durable exports through this package.** Dispatch, download
authorization, retention policy and client integration are separate deliverables.

The store writes immutable Apache Arrow IPC parts and a committed YAML manifest.
It preserves field/schema metadata, types, NULLs and values. A manifest records
the tenant, owner, effective authorization digest, creation/expiry times, writer
fence, canonical schema digest, ordered part digests and exact row/encoded/decoded
byte totals. Every part is a complete Arrow stream with its own schema and EOS.
An empty export still contains one schema-only part.

## Identity and publication contract

Each store serves one tenant under a private root. `Identity` carries an opaque
owner ID and a SHA-256 digest of effective authorization. **The package compares
the digest supplied by a trusted caller; it cannot discover global revocation.**
The gateway/job layer must obtain current authorization, include every applicable
policy/version in that digest, and cancel or invalidate outstanding fills when
access changes. The storage primitive does not implement user authentication,
row/column policies, key rotation or an authorization service.

`Begin` assigns an unpredictable export ID and a separate writer fence. It
publishes and flushes a full-budget initialization intent at the root before
creating the entry directory, lease, state or schema. The intent remains charged
and makes the entry unavailable until initialization finishes. The writer holds an
exclusive per-export lease and accepts borrowed Arrow batches synchronously;
callers may release a batch when `Write` returns. Each batch is currently one
part. An oversized batch is rejected; it is not implicitly split or coerced.
This keeps ownership simple and permits independent validation of each part,
at the cost of repeating schema metadata for each batch.

`Commit` verifies the completed parts, then takes the root metadata lock. It
rechecks the persisted fence, supplied current identity, expiry and active state.
It creates the immutable manifest without replacement, flushes it, and atomically
publishes ready state referring to that exact manifest digest. `Cancel` uses the
same lock to persist a cancelled state; a cancelled or replaced fence cannot
publish. Metadata is written and synced in an unnamed `O_TMPFILE` inode before
`linkat(AT_EMPTY_PATH)` gives it a name. New metadata is published without
replacement. A state replacement first links the complete file into the fixed
`.state.next.yml` slot, then renames it onto `state.yml`. Directory sync completes
durable publication. Under the metadata lock, restart recovery discards a
complete uncommitted slot only when it describes a valid transition of the exact
existing reservation. Malformed or conflicting slots remain untouched and fail
closed; arbitrary temporary-file prefixes do not authorize deletion.

A nonempty manifest returned with `ErrPublicationUncertain` means ready state
may have been published but its durability was not certified. Preserve the
entry and investigate the filesystem. Do not blindly delete, retry an overwrite
or treat this as ordinary success. A later successful read cannot prove that a
previous failed sync survived a crash. Before ready publication, incomplete
parts and manifests remain unavailable to readers.

`Close` on an unfinished writer cancels the fill and releases its lease. It does
not reclaim files or reservation capacity. Its cancellation has a five-second
lock-acquisition bound; an error must be retained by the caller. A terminated
process releases its operating-system lock but leaves its durable reservation.
There is no automatic SQL replay or automatic fill adoption after a crash.
If initialization fails after removing its intent but before root sync succeeds,
the fully initialized, unleased active entry stays unavailable and charged until
its original expiry (at most the configured TTL, capped at 30 days), or explicit
operator cancellation followed by cleanup. The caller receives the sync error,
not a writer or success. Investigate the filesystem before reclaiming it.

## Reading and cancellation

`Acquire` checks ready state, tenant/owner/authorization, expiry, schema and
manifest integrity and takes a shared export lease. It does **not** certify every
payload byte. `OpenPart` independently verifies the entire selected part's hash,
strict IPC framing/EOS, canonical schema, row count and decoded byte count before
returning readable bytes. It checks that state is still authorized and ready
after verification. Unknown versions, malformed metadata, missing parts, extra
streams and truncated EOS fail closed.

Each opened part owns its own shared lease. Closing its parent reader cannot
permit cleanup to delete that part. New acquisitions and part opens require the
current identity and reject cancellation. A part already handed to a caller
retains its admission: its reads enforce the supplied context and original
expiry, but this package cannot reauthenticate the caller or revoke bytes already
delivered. Higher layers must propagate download cancellation and current policy.
Expiry on an acquired handle uses an elapsed-time deadline; persisted expiry
across process restarts depends on an accurate host clock.

## Bounds and storage accounting

Operator configuration fixes the tenant, entry count, logical storage budget
and maximum TTL. Opening the same root with different limits is rejected.
All processes sharing a root serialize reservations/publication with POSIX
`flock`. They need the same effective UID and storage providing the documented
locking, `O_TMPFILE`, `linkat(AT_EMPTY_PATH)`, atomic rename and file/directory
`fsync` semantics. Unsupported filesystems fail explicitly; there is no weaker
named-temporary-file fallback. Linux is the initial supported platform; other
platforms return `ErrUnsupported`.

Per-export limits independently bound rows, encoded bytes, decoded bytes,
encoded/decoded bytes per part, part count and compression. `none` is the default;
`lz4_frame` is opt-in. Compression does not raise decoded limits. The hard format
bounds include at most 256 parts, 256 MiB per encoded or decoded part, a 1 MiB
schema and a 128 KiB manifest. The operator TTL cannot exceed 30 days; a root
cannot contain more than 4,096 charged export entries.

Each export conservatively reserves:

```text
max_encoded_bytes + 1 MiB schema + 128 KiB manifest + 3 × 8 KiB state/intent metadata
```

The root reserves another 8 KiB for its own metadata. Reservation checks include
active, committed, cancelled, expired and crashed fills. The complete reservation
remains charged until cleanup, even when a result is smaller than its upper bound.
This deliberately trades utilization for a straightforward crash-safe capacity
bound. Set realistic result budgets; later accounting optimization must preserve
the same cross-process guarantee. Staging parts and temporary publication files
are included in the bound. Logical file bytes do not account for filesystem block
rounding, journal/COW amplification or unrelated processes; use a filesystem or
container volume quota and provision headroom.

Schema serialization is bounded before Arrow constructs its FlatBuffer. The
preflight permits at most 4,096 type-node visits (including repeated references),
32 levels of nesting, 4,096 metadata pairs, 64 KiB per field name/timezone/metadata
string and 512 KiB of combined string data. It checks nested field counts before
copying child lists, then separately bounds the final schema-buffer allocation
and the 1 MiB encoded schema. These are format/admission limits, not coercions.
The pinned Arrow v18.5.1 implementation uses ordinary Go allocations for its
FlatBuffer builder and metadata vectors; `WithAllocator` only controls its later
copy. The structural preflight therefore matters even with a bounded output
writer. Dictionary, list/view, struct, map, union, run-end and extension schemas
remain supported within these limits.

Schema objects must remain immutable while the writer uses them. Custom Arrow
type and extension callbacks are trusted Go code and must be deterministic and
bounded. Their own execution/allocation, including `ExtensionType.Serialize`,
is outside the storage allocator. Returned extension strings count toward the
same preflight limits; the package does not sandbox registered Go callbacks.

The Arrow reader reuses Kelvo's allocation-critical IPC metadata validation and
adds bounded body/decompression allocation and decoded-size checks. Arrow schema
objects and the caller's borrowed batch have separate costs. Compression
explicitly uses one synchronous codec lane in the pinned Arrow implementation,
so a bounded-allocator rejection stays within the caller's recovery boundary;
the LZ4 subprocess regression must pass when Arrow is upgraded. Encoding and
decoding can temporarily hold multiple buffers. These controls are not a whole-process
RSS cap and do not make DuckDB execute a query incrementally.

## Retention and crash recovery

There is no background collector. `Cleanup` is an explicit operator operation
with a bounded removal count. It considers incomplete initialization intents
immediately, and otherwise cancelled or expired entries. It uses a nonblocking
exclusive lease. Active writers/readers keep their data and
remain charged; a leaked handle requires caller/process cleanup.

Before deletion, cleanup writes and flushes an immutable root-level deletion
marker naming the exact owned entry and reservation. It validates every remaining
file name, type, permissions and size before unlinking anything. It removes the
entry, syncs the root, then removes the marker and syncs again. A later cleanup
can resume an interrupted deletion from that marker, including when the entry's
state file or directory has already gone. Unknown files, malformed state or
conflicting markers stop cleanup without deleting unrecognized data. An
anonymous metadata write that dies before publication leaves no named root
debris. While the durable initialization intent remains, cleanup can recover
subsequent initialization cutpoints, including missing directories or state files. Errors may
leave a charged entry/marker for a later retry or operator investigation.

Store files and directories must belong to the effective user and have no
group/other permissions. Every ancestor must be owned by root or the effective
user and reject group/other write access, with a root-owned sticky temporary
directory such as `/tmp` as the explicit exception. These checks happen before
creating a child through an ancestor. Published payloads, schemas, manifests and
initialization/deletion markers are read-only. Descriptor-relative traversal rejects symlink components, non-regular
files and additional hard links. The effective UID and operating-system
administrator remain trusted; this is not encryption or a defense against an
administrator modifying the store outside its protocol.

## Contributor validation

Run only on the designated Linux test host, with existing dependencies:

```sh
GOMAXPROCS=2 go test -race -p 2 ./internal/exports
GOMAXPROCS=2 go vet -p 2 ./internal/exports
```

The package tests cover exact typed Arrow results, empty exports, LZ4, restarts,
independent part leases, owner/authorization rejection, cancellation/publication
races, row/byte/part limits, actual subprocess admission and reader leases,
process death at initialization/state-publication cutpoints, malformed recovery
slots, injected file/directory sync failures, publication uncertainty, interrupted
deletion, forged counts with recomputed metadata hashes, cancellation/expiry
during verification, private-file/ancestor safety, oversized schema metadata,
nesting/cycles/repeated type references, practical nested/extension schema
compatibility, YAML depth and strict IPC
completion. A subprocess forces a low allocator budget during LZ4 encoding and
requires ordinary failure without a process panic.

These are storage correctness tests. Runtime job integration, real HTTP download
tests, cluster loss acceptance and workload-sized capacity evidence remain
required before durable exports can be offered to users.

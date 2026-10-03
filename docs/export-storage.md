# Durable export storage foundation

Use `internal/exports` to store immutable Arrow IPC parts with a committed YAML manifest. **This internal package is not exposed by a current CLI, HTTP endpoint or cluster job**; dispatch, download authorization, retention policy and clients remain separate work.

Manifests bind tenant/owner/authorization, creation/expiry, writer fence, schema and ordered part digests, rows, and encoded/decoded totals. Each part is a complete Arrow stream with schema and EOS; an empty export has one schema-only part. Types, metadata, NULLs and values are preserved.

## Identity and publication contract

1. Open a private root for one tenant with fixed operator limits.
2. Call `Reserve` with a trusted owner and effective-authorization SHA-256 digest **before executing source SQL**. It durably reserves the full budget and creates unpredictable export/fence IDs without needing a schema.
3. Call `BindSchema` with the result schema and current trusted identity, then write borrowed batches synchronously. Binding succeeds exactly once; each batch becomes one part. Oversized schemas/batches fail instead of splitting or coercing. `Begin` remains a convenience wrapper for a schema already known before execution.
4. Call `Commit` with current identity, or `Cancel`. Retain all returned errors, including errors from unfinished-writer `Close`.

The package compares the supplied digest; **it cannot discover revocation**. Higher layers must include every applicable policy/version, obtain current authorization and invalidate fills after changes. Authentication, row/column policies and key rotation are outside this API.

`Reserve` durably records a root initialization intent before creating entry state or lease. The intent stays charged and unreadable until initialization completes. Writers hold exclusive export leases. Unbound `Write`/`Commit` return `ErrSchemaUnbound` without consuming part capacity; invalid schemas or identities can be corrected before binding.

`BindSchema` rechecks exact reservation, identity, expiry and cancellation under the root lock, syncs the immutable schema, then publishes active state. Rebinding is rejected, including an identical schema. Storage failures poison the writer; an orphan schema is never adopted. Binding does not make results readable.

`Commit` verifies parts and rechecks persisted fence, identity, expiry and active state under the root lock. It creates/syncs the immutable manifest, then atomically publishes ready state with that manifest's digest. Cancellation uses the same lock; cancelled or replaced fences cannot publish.

Metadata uses synced unnamed `O_TMPFILE` inodes and `linkat(AT_EMPTY_PATH)`. New files never replace existing names. State replacement links to `.state.next.yml`, then renames onto `state.yml` and syncs the directory. Recovery discards a pending slot only for a valid transition of the exact reservation; malformed/conflicting slots fail closed. Temporary-looking names do not authorize deletion.

**`ErrPublicationUncertain` means a metadata publication may have happened.** `Commit` also returns a manifest; `BindSchema` returns no result and the entry stays unreadable. Both release the writer without cancellation. Preserve the entry and investigate storage; do not overwrite, delete or report ordinary success. Readback cannot prove a failed sync survived a crash.

Unfinished `Close` cancels and releases the lease with a five-second lock bound; it reclaims neither files nor reservation. Process death releases OS locks but not durable reservations, and never causes SQL replay or fill adoption. After initialization removes its intent, an unleased reserved/active entry remains unavailable and charged until original expiry or operator cancellation plus cleanup, including root-sync failures or a crash during binding. Preserve storage errors and investigate first.

New entries use state version 2 (`reserved → active → ready`); existing version 1 entries remain readable without migration. Manifests and root configuration remain version 1. Upgrade **every process using a root before new writes**: older binaries reject version 2 entries, and mixed-version operation or automatic downgrade is unsupported. A pending schema-binding state slot is discarded only after validating its exact immutable schema and reservation; conflicting metadata remains untouched.

## Reading and cancellation

`Acquire` checks ready state, current identity, expiry, schema and manifest, then holds a shared lease. It does not hash all payloads. `OpenPart` verifies the selected part's full hash, strict IPC/EOS, schema, rows and decoded bytes, then rechecks authorization/state before returning bytes. Unknown versions, malformed metadata, missing parts and extra/truncated streams fail.

Each part holds an independent lease; closing its parent reader cannot allow deletion. New acquisitions/opens reject cancellation and require current identity. An already-admitted part enforces context and original expiry but cannot reauthenticate or revoke delivered bytes. Higher layers must propagate download cancellation. Live handles use elapsed-time expiry; restarts depend on an accurate host clock.

## Bounds and storage accounting

All root users need the same effective UID and limits. POSIX `flock`, `O_TMPFILE`, `linkat(AT_EMPTY_PATH)`, atomic rename and file/directory `fsync` are required. Unsupported filesystems fail without a weaker temporary-file fallback; non-Linux platforms return `ErrUnsupported`.

| Bound | Maximum |
| --- | --- |
| Parts / encoded or decoded bytes per part | 256 / 256 MiB |
| Schema / manifest | 1 MiB / 128 KiB |
| Operator TTL / charged entries | 30 days / 4,096 |
| Schema visits / nesting / metadata pairs | 4,096 including repeats / 32 / 4,096 |
| Each schema string / combined strings | 64 KiB / 512 KiB |

Per-export limits independently cap rows, encoded/decoded totals, part sizes/count and compression. `none` is default; opt-in `lz4_frame` does not raise decoded limits.

Each export reserves:

```text
max_encoded_bytes + 1 MiB schema + 128 KiB manifest + 3 × 8 KiB state/intent metadata
```

The root adds 8 KiB. Reserved, active, ready, cancelled, expired and crashed entries retain their **full** reservation until cleanup, including staging/publication files. Set realistic budgets and provision filesystem quota/headroom for block rounding, journals, COW and unrelated files.

Schema preflight runs before FlatBuffer construction, bounds child counts before copying and separately bounds final allocation/encoding. Dictionary, list/view, struct, map, union, run-end and extension schemas remain supported within limits. Arrow's FlatBuffer/metadata allocations are not fully controlled by `WithAllocator`.

Schemas must remain immutable. Custom type/extension callbacks are trusted deterministic, bounded Go code; their execution/allocation is not sandboxed. Returned strings still count against preflight limits.

IPC decoding validates allocation-critical metadata, body/decompression sizes and decoded totals. LZ4 uses one synchronous codec lane; retain its subprocess regression during Arrow upgrades. Caller batches, schemas and temporary encode/decode buffers add memory. These budgets neither cap RSS nor make DuckDB execution incremental.

## Retention and crash recovery

`Cleanup` is explicit and removal-count bounded; there is no background collector. It considers incomplete intents immediately, otherwise cancelled/expired entries, and needs a nonblocking exclusive lease. Live or leaked handles keep data charged.

Deletion first syncs an immutable root marker identifying the exact entry/reservation. It validates all remaining names, types, permissions and sizes, removes the entry, syncs root, removes the marker and syncs again. A later cleanup resumes interrupted deletion even after state/directory loss. Unknown files, malformed state or conflicting markers stop deletion. Failed initialization/recovery can remain charged for retry or investigation.

Files/directories must belong to the effective user with no group/other permissions. Ancestors must belong to root or that user and reject group/other writes; a root-owned sticky directory such as `/tmp` is the exception. Checks precede child creation. Payloads, schemas, manifests and intent/deletion markers are read-only. Descriptor-relative traversal rejects symlinks, nonregular files and extra hard links. The effective UID and administrator remain trusted; storage is not encrypted by this package.

## Contributor validation

Run on the designated Linux host with existing dependencies:

```sh
GOMAXPROCS=2 go test -race -p 2 ./internal/exports
GOMAXPROCS=2 go vet -p 2 ./internal/exports
```

[Reservation evidence](evidence/export-reservation-package.json) adds pre-schema admission, binding/crash recovery and actual v1/v2 binary compatibility checks. All shared-store processes must upgrade before v2 writes.

[Earlier package evidence](evidence/export-storage-package.json) covers exact Arrow data, LZ4, limits, independent leases, cross-process admission, cancellation/publication races, crash cutpoints, sync uncertainty, strict recovery, filesystem safety and schema/IPC bounds.

Runtime jobs, authenticated HTTP downloads, cluster loss and workload-sized capacity still need acceptance before durable exports are user-facing.

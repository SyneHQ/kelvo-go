# Restore a protected snapshot

Switch the active pointer to a retained generation without copying data or querying
the source. Restore preserves its original age, object versions, rows and schema.

## Run

1. Use a [protected namespace](protected-object-readers.md) and configure the
   dataset's [verification budget](protected-verification.md).
2. Find the target and current generation:

```sh
kelvo accelerate inventory --config kelvo.yml --dataset trips
```

3. Restore with the current generation as a precondition:

```sh
kelvo accelerate restore --config kelvo.yml --dataset trips \
  --generation RETAINED_GENERATION \
  --expected-generation CURRENT_GENERATION
```

Both generations must match the current catalog and have exactly equal schemas.
A stale target keeps its age; restore does not make it fresh. Missing or corrupt
current data requires a separate recovery process.

## What is checked

Kelvo claims a fresh writer lease, pins both generations, then verifies immutable
versions, checksums, Parquet row counts and schemas. One deadline and byte budget
cover writer admission, metadata reads, verification and publication reconciliation.
Leave room for both payloads, descriptors and repeated footer/root reads.

The final check fences the writer identity and both selected generations. Restore
issues one pointer swap; an uncertain response triggers a bounded reread, never a
blind retry. Restoring the current generation verifies it once and must confirm
writer release before succeeding.

No payload upload, new generation, registry staging/sealing or source query occurs.
Exact-owned writer cleanup may use bounded root reads after the verification budget
is exhausted; it cannot publish a snapshot or read generation data.

## If the command fails

The CLI withholds success YAML until manager and runtime cleanup finish. A failed
command can still have changed the pointer; check its outcome before retrying.

| Outcome | Meaning |
| --- | --- |
| `not_attempted` | No pointer swap was attempted; cleanup may still need attention |
| `not_published` | The pointer swap received a definite conflict |
| `verified_noop` | The target was already current and writer release was confirmed; later finalization failed |
| `target_observed` | The exact target pointer was observed; later work failed |
| `unknown` | Publication could not be established within the deadline |

An observation does not prove which writer published the pointer or that it is
still current. Inspect it again before another restore. Pending body, lease or
provider cleanup keeps its operation capacity until the owned work actually ends.

[Linux acceptance](evidence/protected-restore.json) passed all 13 stages on `9d8789e`,
including ordinary/race/native checks, four TLS restore workflows and independent
process cleanup. The first trial's cleanup-fixture failure and correction remain
recorded. Fault tests cover cancellation, conflicts and delayed cleanup.
These fixtures do not certify cloud IAM, distributed revocation or deletion safety.

[Verification](protected-verification.md) · [Backup and migration](snapshot-backup.md) · [Production status](production-status.md)

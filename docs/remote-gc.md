# Remote generation collection

`kelvo accelerate gc` accounts for every object under one dataset's remote
prefix and, only when explicitly asked, collects generations that no manifest
references. Deletion is off by default and requires a
[protected namespace](protected-object-readers.md). Legacy namespaces have no
durable reader leases, so they get the report only.

```sh
# Report only (default). No writes, no deletes.
bin/kelvo accelerate gc --config kelvo.yml --dataset sales_fast
# Quarantine unreferenced generations; delete them on a later run after --grace.
bin/kelvo accelerate gc --config kelvo.yml --dataset sales_fast --delete --grace 24h
```

## Classes

| Class | Meaning | Ever deleted |
| --- | --- | --- |
| `current` | Committed generation in `current.yaml` | Never |
| `retained` | Generation in the manifest history (restorable) | Never |
| `staging` | Any unreferenced generation while a writer lease is held or not provably expired (+1 minute margin), and the writer's own generation | Never |
| `orphan` | Unreferenced, no writer, no quarantine marker yet | Quarantined first |
| `quarantined` | Unreferenced with a `gc/<generation>.quarantine.yaml` marker | After grace and quiescence |
| `manifest`, `unknown` | `current.yaml` and keys that do not match a Kelvo name | Never |

The report lists bytes and object counts per class and up to 256 generations
with their action. Listing is paged at 1000 keys, capped at 1,048,576 objects
and 4096 generations. A partial listing is reported as `listing_complete:
false` and disables all collection for that run.

## Collection protocol

Each step is a separate durable write. A crash after any step leaves a store
that the next run resumes from.

1. List the prefix, then read the manifest. Every listed object existed before
   that read, so its writer is visible, published, or gone.
2. Skip the whole dataset if a writer lease is held.
3. Write the quarantine marker (create-only). Its time comes from the provider
   clock observed with the manifest.
4. Retire the generation's reader-registry document (CAS). Retirement
   permanently blocks new `Acquire` and `Seal`. Leases that already exist keep
   renewing, so an in-flight reader keeps reading.
   Steps 3 and 4 run only if a fresh manifest read, taken right before the
   marker write, still shows the generation unreferenced and no writer lease
   inside the margin.
5. On a later run, re-read the manifest: still unreferenced and no writer. A
   manifest without provider time keeps the generation (`kept: clock`); the
   host clock is never used.
6. Inspect the registry: every reader entry must be past `expires_at` plus the
   clock uncertainty, judged against the earliest possible provider time. A
   crashed reader's entry blocks until it expires. The first quiescent
   inspection records `quiescent_since` (provider time) in the marker with a
   conditional replace.
7. Deletion waits until `quiescent_since` is at least `--grace` old (minimum
   10 minutes, default 24 hours) and a fresh inspection is still quiescent.
   Grace runs from quiescence, not from quarantine, so a reader that renews
   for days still gets the full grace after it stops.
8. Delete each data object at its exact current version, then the registry
   document at the version just inspected, then the marker. A missing object
   counts as already deleted.

Any read, clock, decode, permission, capacity or provider error keeps the
generation and is reported as `kept: <reason>`. A generation without a
registry document is kept forever. Unknown keys are never touched.

## Provider permissions and limits

Deletion uses the writer identity for data objects and markers, and the
registry identity for registry documents. Both need list and delete grants
only when `--delete` is used. The reporting run needs list only.

Delete sends `If-Match` (S3/R2/Azure) or `x-goog-if-generation-match` (GCS).
MinIO `RELEASE.2026-10-23T20-06-51Z` deleted an object even with a stale
`If-Match`, so the S3 client first sends a conditional `HEAD`. That check is
not atomic with the delete. It is safe in practice only because generation
keys are never rewritten.

"Exact version" means the ETag (or GCS generation) matches. Kelvo never
sends an S3 `versionId`. On a versioned S3 bucket, a delete only adds a delete
marker: earlier versions remain and no space is freed until a bucket
lifecycle rule expires noncurrent versions. Configure that rule separately.

Known leaks, reported but never collected: `<generation>.yaml` and
`.stage-*` objects and late multipart parts that do not match a Kelvo
generation name show up as `unknown`, and parts of a generation with no
registry document are kept forever. Clean these by hand after checking the
report.

An expired registry entry still does not prove that a native child stopped
reading. Grace is the margin for that. Do not set it lower than your longest
query deadline plus process cleanup time.

## Validation

- Fixture tests (`internal/acceleration/gc_remote_test.go`, race-enabled):
  dry run writes nothing; quarantine, grace and idempotent delete; a concurrent
  reader blocks deletion and keeps reading; a crashed lease blocks until it
  expires; a crash at each delete step resumes; listing, write and delete
  failures and an active writer all fail closed; the current generation stays
  byte-identical.
- Registry tests (`internal/readerlease/retire_test.go`): retirement blocks
  acquire and seal, live leases keep renewing, clock skew never shortens
  quiescence.
- MinIO conformance (`TestS3ConformanceListPagesAndDeletesExactVersion`,
  `TestRemoteGCMinIOConformance`) runs against an HTTPS MinIO when
  `KELVO_TEST_S3_ENDPOINT`, `KELVO_TEST_S3_BUCKET`,
  `KELVO_SOURCE_TEST_S3_ID` and `KELVO_SOURCE_TEST_S3_SECRET` are set.

Not yet validated: AWS S3, R2, GCS and Azure live providers, and capacity.
Do not enable `--delete` in production until those gates pass.

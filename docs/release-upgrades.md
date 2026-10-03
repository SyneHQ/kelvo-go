# Release upgrades and snapshot compatibility

A binary rollback and a data rollback are separate operations. Retain the old
executable, matching configuration and a verified independent snapshot copy
before enabling a new writer. A readable manifest does not establish that a
mixed-version cluster, its authorization configuration or its job state is safe
to roll back.

## Current compatibility boundary

The immutable `v0.1.0-preview.1` package was built from
`73a8eb44b7b00f6416f7bbefcdd6434fa7dbce0e`. It already reads and writes the formats
below. The candidate release gate compares that exact packaged executable with
the supplied candidate binary; it refuses another executable presented as the
preview. The report records both hashes and the operator-supplied candidate
source revision. Build provenance must independently associate that revision
with the candidate hash.

| State | Preview writer | Current writer | Scope of the release gate |
| --- | --- | --- | --- |
| Single local snapshot | Local manifest v1 | Local manifest v1 | Both directions, exact Arrow values, verification and backup rollback |
| Multipart local snapshot | Local manifest v2 | Local manifest v2 | Both directions, multiple physical parts and empty datasets |
| Remote single/multipart snapshot | Remote manifest v4 | Remote manifest v4 | Both directions of signed remote verification, explicit local migration and exact queries against migrated data |
| Remote manifest v2/v3 | Reader support in the implementation | Reader support in the implementation | Covered by existing format tests; this release matrix does not execute a historical pre-v4 writer |
| Unknown manifest version | Reject | Reject | Both readers and refresh writers reject version 999 without overwriting the current manifest |
| `accelerate migrate-backup` | Unavailable | Explicit remote-to-local migration | Preview rejects the command before storage access; both binaries read candidate-created local copies |

**Do not deploy a pre-v4 reader after a writer starts publishing remote v4.**
Older format support in the new reader does not imply newer format support in an
old reader. Restore the matching old binary/configuration and a separately
verified compatible copy, or move forward with the new reader. Do not edit a
manifest's version to force acceptance: descriptors and retained history have
their own invariants. Fixture version mutation is only a rejection test inside
an isolated temporary store.

This matrix concerns standalone snapshot storage on Linux amd64. It does not
prove a rolling cluster upgrade, JetStream compatibility, key-file rollback,
native extension ABI compatibility, live-provider conformance or macOS/ARM64
behavior. Query execution may materialize data in DuckDB before Arrow delivery;
these small compatibility fixtures establish no memory or throughput limit.

## Operator sequence

1. Record the release/tag, executable SHA-256, build options, launcher and
   extension versions, catalogs and authorization versions. Preserve private
   configuration outside public evidence. Run the release gate for those exact
   executables on a representative test host.
2. Stop new refresh scheduling and drain admitted work using the
   [maintenance controls](operations.md). Keep all readers/writers on compatible
   catalogs. This document does not authorize mixing releases within a cluster.
3. Create and verify independent [local backups](snapshot-backup.md). For an
   object-backed dataset, the candidate can perform explicit
   [remote-to-local migration](snapshot-backup.md#migrate-a-current-object-snapshot-to-local-storage)
   into a new private root with a matching destination catalog. That is a
   deliberate storage change, not an object-backend backup or an automatic
   service cutover. Preserve every dataset needed by the service separately.
4. Upgrade readers before allowing an incompatible writer format, then run
   health/readiness checks, `accelerate verify` and known exact-result queries.
   Enable refreshes only when all readers and the rollback plan are compatible.
5. On failure, stop the new writer, retain the failure evidence and verify the
   rollback copy with the intended old executable. Recover into a new root and
   switch the service catalog/mounts deliberately. Never overwrite the live
   store or the only backup. Restore key revision floors and authorization
   policy from the approved current security configuration; a data rollback
   must not reinstate revoked access.
6. Query the restored copy before admitting traffic. Its original generation,
   fingerprint and refresh time must remain intact. An old copy remains stale;
   do not increase freshness or reset the timestamp to make a rollback pass.
   Measure recovery objectives separately for the real topology and data size.

The scripted rollback deliberately restores the original rows after a subsequent
refresh adds seven rows. Those seven rows are absent after recovery. This is a
visible backup boundary, not a measured source-transaction recovery-point
objective. Independent snapshots are not a cross-database transaction.

## Reproduce the release gate

Use a dedicated Linux amd64 host with the two executables, sandbox launcher,
Python, PyArrow, PyYAML and OpenSSL already available. The runner downloads or
builds nothing, starts no database/broker and changes no system certificate
trust. It creates signed loopback HTTPS fixtures using a process-scoped CA;
synthetic credentials live only in the process environment.

```sh
python3 -m unittest discover -s scripts -p test_release_upgrade_acceptance.py -v

python3 scripts/release_upgrade_acceptance.py \
  --previous-binary /opt/kelvo-preview/kelvo \
  --candidate-binary bin/kelvo \
  --candidate-revision FULL_40_CHARACTER_SOURCE_REVISION \
  --launcher bin/kelvo-landlock \
  --rows 100000 \
  --output artifacts/release-upgrades-new.json
```

The output path must be new. The fixed matrix requires all **20 scenarios**:
single and multipart, each with a nonempty and an empty counterpart, for local
storage plus S3, R2, GCS and Azure Blob protocol fixtures. Local fixtures use
100,000 rows by default; `--rows` accepts 100,000–1,000,000 for those fixtures.
Remote fixtures use 40,000 rows to remain below the shared TLS fixture's 8 MiB
single-request limit. The nonempty multipart scenarios must contain multiple
physical Parquet objects/files, not just a multipart configuration flag.

Every snapshot comparison preserves the generation, authorization fingerprint,
schema fingerprint, payload digest, row/byte accounting and `refreshed_at`.
Explicit migration preserves generation/schema/rows/bytes/time and checks both
source provenance hashes; it deliberately derives a different local fingerprint
and may derive a different multipart aggregate digest. Exact queries compare
integer widths, unsigned values, decimals, timestamps, strings and NULLs.

The gate also verifies failed refresh preservation, unchanged reader metadata,
unknown-format rejection without overwrite, unavailable-command rejection,
old-reader access to the new writer's snapshots, and independent backup rollback
with freshness/authorization enforcement. Remote verification/migration must
use only reader credentials and GET/HEAD operations. **Remote direct DuckDB
queries are not exercised**: exact value checks run against the migrated local
copies, which avoids conflating format compatibility with extension/network
integration.

The sanitized JSON records passed cases, failure stage/code, binary/launcher and
helper-script hashes, platform, command count and cleanup. Crashes are never
counted as expected rejection. Each command has a deadline and bounded captured
output; descendants and fixture servers belong to this run. An interrupted or
failed gate cannot report success. Existing reports are never overwritten.

Keep failures alongside the eventual passing record. Repeat the gate whenever
runtime, format, migration or relevant dependency changes make an earlier
binary's result inapplicable. See the [production checklist](production-status.md)
for the provider, mixed-load and deployment work outside this gate.

## Recorded validation

The [recorded Linux amd64 run](evidence/release-upgrades.json) passed all
20 scenarios and **573 executable commands**, with no forced process kills and
successful private fixture cleanup. It compares the exact public preview with
the `kelvo-production-v3` candidate executable associated with source
`7ac9bc3052de6a8fa1f71a221c24c2f0fbc478e5`; the report contains both binary hashes,
the launcher hash and every imported fixture-script hash. Nine control tests
passed, covering false/missing evidence, changed identity/provenance, illegal
storage operations, duplicate/missing scenarios, crashes, false success markers,
output limits and deadlines.

The [earlier passing candidate](evidence/release-upgrades-earlier-candidate.json)
is retained separately. Its result is not substituted for the final runtime's
fresh 20-scenario run.

Two earlier unsuccessful attempts are retained: the
[initial namespace failure](evidence/release-upgrades-initial-failure.json) and
the [single-upload fixture limit failure](evidence/release-upgrades-upload-limit-failure.json).
Both passed the four local layouts and then failed on the first S3 refresh;
neither is counted as remote acceptance. Review of the shared fixture identified
its fixed authorized tenant namespace and 8 MiB request ceiling. The runner was
aligned with that namespace and remote row count reduced to 40,000; the
multiple-physical-part assertion and every compatibility check remain required.
Both failed attempts cleaned up their owned processes and temporary files.

This evidence belongs to the recorded executable. A later integrated release
needs a fresh run against its actual binary. The report is not evidence for a
real-provider deployment, cross-host failover, all historical releases or a
production capacity guarantee.

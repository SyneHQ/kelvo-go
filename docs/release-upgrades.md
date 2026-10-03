# Release upgrades and snapshot compatibility

For gateway/worker rolling changes, use the separate [application compatibility matrix](rolling-upgrades.md). Snapshot compatibility alone does not establish cluster compatibility.
Binary rollback and data rollback are separate. Before enabling a new writer, retain the old executable/configuration and a verified independent snapshot copy.

## Current compatibility boundary

The gate compares the exact `v0.1.0-preview.1` executable from `73a8eb44b7b00f6416f7bbefcdd6434fa7dbce0e` against a candidate. It records hashes and the supplied candidate revision; build provenance must independently associate that revision with its binary.

| State | Preview/current format | Gate |
| --- | --- | --- |
| Local single | v1 / v1 | Both directions, exact values, verification and backup rollback |
| Local multipart | v2 / v2 | Both directions, physical parts and empty data |
| Remote single/multipart | v4 / v4 | Signed verification, explicit local migration and exact migrated queries |
| Remote v2/v3 | Reader support in both | Existing format tests; no historical pre-v4 writer in this matrix |
| Unknown version | Both reject | Version 999 rejected without overwrite |
| `accelerate migrate-backup` | Preview unavailable | Rejected before storage access; both readers accept candidate-created local copies |

**Do not deploy a pre-v4 reader after a remote v4 writer acts.** Even its first lease claim can publish v4. Never edit manifest versions to force downgrade; recover with compatible binary/configuration and a verified copy.

The matrix covers standalone Linux amd64 snapshots, not rolling cluster/JetStream upgrades, key-policy rollback, extension ABI, live providers or other platforms. Small materializing DuckDB fixtures establish no memory/throughput limit.

## Operator sequence

1. Record executable/launcher hashes, build options, extension versions, catalogs and authorization versions. Run the gate for those exact binaries on a representative host.
2. Stop refresh scheduling and [drain work](operations.md). Keep readers/writers compatible; this procedure does not authorize mixed-release clusters.
3. Verify independent [local backups](snapshot-backup.md). For object data, explicitly [migrate to a new private local root](snapshot-backup.md#migrate-a-current-object-snapshot-to-local-storage) with a matching catalog. This is a storage change, not an object backup or automatic cutover.
4. Upgrade readers before incompatible writers. Check readiness, `accelerate verify` and exact known queries before resuming refresh.
5. On failure, stop the writer and retain evidence. Verify the backup with the intended old binary; recover into a **new** root before switching mounts/catalogs. Never overwrite the live store or only backup.
6. Retain current security revision floors/authorization, original generation and refresh time. A rollback must not restore revoked access or make stale data fresh. Query the restored copy before admission.

The fixture rollback intentionally loses seven rows added after its backup. Independent snapshots are not cross-database transactions or measured source-transaction RPOs.

## Reproduce the release gate

Use a dedicated Linux amd64 host with both binaries, launcher, Python, PyArrow, PyYAML and OpenSSL. The runner builds/downloads nothing and changes no system trust; HTTPS fixtures use a process-scoped CA.

```sh
python3 -m unittest discover -s scripts -p test_release_upgrade_acceptance.py -v
python3 scripts/release_upgrade_acceptance.py \
  --previous-binary /opt/kelvo-preview/kelvo \
  --candidate-binary bin/kelvo \
  --candidate-revision FULL_40_CHARACTER_SOURCE_REVISION \
  --launcher bin/kelvo-landlock --rows 100000 \
  --output artifacts/release-upgrades-new.json
```

Use a new report path. All 20 combinations must pass: local/S3/R2/GCS/Azure, single/multipart, empty/nonempty. Local rows default to 100,000 (allowed 100,000–1,000,000); remote fixtures use 40,000 below their 8 MiB request limit. Multipart requires multiple physical parts.

Checks preserve generation, authorization/schema fingerprints, digests, row/byte totals and refresh time. Migration checks source provenance but intentionally derives a local fingerprint and potentially a new aggregate digest. Exact types include integers, unsigned values, decimals, timestamps, strings and NULLs.

The gate also checks failed-refresh preservation, unknown formats, unavailable commands, unchanged reader metadata, old-reader access and authorized/fresh backup rollback. Remote operations use reader credentials and GET/HEAD only. Exact remote values are queried **after local migration**; direct DuckDB cloud queries are outside scope.

Reports include artifact/helper hashes, command counts, bounded failures and cleanup. Crashes, interruption or cleanup failure cannot pass. Retain failures and rerun for relevant runtime/format/dependency changes; see [remaining release gates](production-status.md).

## Recorded validation

The [recorded run](evidence/release-upgrades.json) passed 20 scenarios and **573 commands** against candidate `7ac9bc3`, with nine control tests, no forced process kills and successful cleanup. Both binary/launcher hashes and imported helper hashes are recorded. The [earlier candidate](evidence/release-upgrades-earlier-candidate.json) remains separate.

The [namespace failure](evidence/release-upgrades-initial-failure.json) and [upload-limit failure](evidence/release-upgrades-upload-limit-failure.json) passed local layouts but failed the first S3 refresh. Correcting the fixture namespace and reducing remote rows to 40,000 preserved every compatibility and physical-part check; failed runs cleaned up.

This certifies the recorded compatibility fixture, not live-provider deployment, cross-host failover, every historical release or production capacity. Later binaries need fresh runs.

# Snapshot backup and recovery

Copy one current local dataset generation into a new private acceleration root with `kelvo accelerate backup`. It verifies bytes, rows and Arrow schema while preserving generation identity, fingerprint, checksums and original refresh time.

Linux atomic no-replace directory rename and directory `fsync` are required. The command supports single/multipart snapshots, never overwrites a destination, and performs no source query, secret lookup or service reconfiguration. For object storage, use [explicit migration](#migrate-a-current-object-snapshot-to-local-storage).

## Create a backup

1. Create an existing parent directory owned by the Kelvo OS user with mode `0700`; choose a new absolute destination beneath it, without symlinks.
2. Run as the source-store owner using its acceleration catalog, not a cluster-node configuration:

```sh
kelvo accelerate backup --config kelvo.yml --dataset orders_daily \
  --destination /var/lib/kelvo-backups/orders-20261003
```

3. Retain the catalog separately and inspect successful YAML for `verified: true` and `snapshot`.

The dataset must match the active fingerprint. Preserve source definitions and `authorization_version`; unavailable referenced database credentials do not prevent backup. A changed policy cannot be bypassed by copying an older snapshot.

Only the acquired current generation and manifests are copied under the original tenant/dataset namespace. History, other datasets, config, secrets and NATS state are excluded. Protect payloads as sensitive data; use operator-managed encryption where needed.

`limits.max_bytes` caps encoded payload up to 1 TiB; metadata/filesystem overhead needs extra space. Bounded copies hold reader leases through verification, so concurrent refresh can retain extra generations. Backup captures the generation acquired at start.

Integrity does not imply freshness: stale data can be backed up and stays stale. `--destination` is backup-only; `--sandbox`, `--generation` and `--expected-generation` are rejected.

## Recover into another fresh root

1. Copy the retained catalog to `kelvo-backup.yml`, changing only its directory:

```yaml
acceleration:
  directory: /var/lib/kelvo-backups/orders-20261003
  # Preserve tenant_id, datasets and all other catalog definitions.
```

2. Copy from backup into another new root under a private parent:

```sh
kelvo accelerate backup --config kelvo-backup.yml --dataset orders_daily \
  --destination /var/lib/kelvo-recovery/orders-recovered
```

3. Point `kelvo-recovered.yml` at that root, then verify and inspect readiness:

```sh
kelvo accelerate verify --config kelvo-recovered.yml --dataset orders_daily
kelvo accelerate status --config kelvo-recovered.yml --dataset orders_daily
```

4. Run the intended analysis before explicitly switching service configuration/mounts. Cluster workers must agree on the path, tenant and catalog.

One dataset does not recover the rest of a shared catalog. Original timestamps and `max_age` remain; stale queries reject until a valid refresh.

`accelerate restore` switches retained generations inside an existing store and requires a verifiable current generation. Backup recovery can establish a fresh store when the original current payload is corrupt.

## Failed or interrupted publication

Copies stage privately and publish by no-replace rename. Before publication, failures remove owned staging where possible; pre-existing or concurrently created destinations are preserved. Filesystem errors may also block cleanup—retain the error and inspect storage.

**`BACKUP_DURABILITY_UNCERTAIN` can occur after publication.** The Go API may return a snapshot plus error; the CLI does not report `verified: true`. Preserve the destination, resolve storage errors and verify through a catalog pointing there. Do not delete or retry over it. Readback cannot prove an earlier failed `fsync` was durable; use a new destination for a fresh attempt.

Scheduling, replication and failover remain operator responsibilities. The command establishes no RTO/RPO; measure recovery and assess the preserved refresh time. Independent backups are not a shared cross-database transaction snapshot.

## Reproduce the recovery gate

Use the designated Linux host with PyArrow, built CLI and sandbox:

```sh
python3 -m unittest discover -s scripts -p test_backup_acceptance.py
python3 scripts/backup_acceptance.py --binary bin/kelvo \
  --launcher bin/kelvo-landlock --rows 1000000 \
  --output artifacts/backup-recovery-new.json
```

The runner uses private synthetic data and a new report path, then removes its source/live store before recovery. It checks exact Arrow values/types/NULLs, overwrite/policy rejection, empty snapshots and stale-query denial. CI uses 100,000 rows.

[Azure evidence](evidence/backup-acceptance.json) records artifact hashes, four scenarios and cleanup:

| Snapshot | Rows | Parquet bytes | Parts | Backup copy and verification | Recovery through verified first query |
| --- | ---: | ---: | ---: | ---: | ---: |
| single | 1,000,000 | 120,907,846 | 1 | 0.865 s | 2.611 s |
| multipart | 1,000,000 | 122,068,203 | 145 | 1.819 s | 3.769 s |
| single empty | 0 | 712 | 1 | 0.064 s | 0.160 s |
| multipart empty | 0 | 712 | 1 | 0.064 s | 0.144 s |

These are single-run warm-cache local results including startup, verification and exact comparisons. They exclude remote transfer, service cutover and cold-cache control, and are not failover SLAs. Seven rows added after backup are intentionally absent after recovery.

## Migrate a current object snapshot to local storage

`kelvo accelerate migrate-backup` copies one current S3/R2/GCS/Azure generation into a new private local root using only reader credentials. It verifies immutable versions, hashes, footer rows and schemas, with no remote writes/listing or source query.

Object location/credential references affect fingerprints, so migration requires both catalogs and derives a new local fingerprint. It preserves payload, generation, schema and age. Retain successful `source_fingerprint`, `source_generation_sha256` and local `snapshot` output with both catalogs; multipart local and remote descriptor digests differ by format.

1. Retain `remote.yml`; create `local.yml` by removing `acceleration.object_storage` and setting a new absolute private `acceleration.directory`.
2. Keep every other source/query, tenant/dataset, authorization, policy, limit, refresh and extension setting identical. Mismatches fail before credentials or I/O.
3. Migrate, verify and query:

```sh
kelvo accelerate migrate-backup --config remote.yml \
  --destination-config local.yml --dataset orders_daily

kelvo accelerate verify --config local.yml --dataset orders_daily
kelvo query --config local.yml --sources orders_daily \
  --sql 'SELECT COUNT(*) AS rows FROM orders_daily' --out recovered.arrow
```

Use `--sandbox` on the verification query when required. Migration runs in the trusted parent and rejects sandbox, generation selectors or query overrides. It resolves reader references only; local queries afterward need no object/database/writer credentials for this snapshot.

Migration does not switch services, replace catalogs, copy history or recover other datasets.

### Integrity, policy and failure boundaries

The acquired generation is copied sequentially through a 256 KiB buffer with bounded footer metadata. Payload/rows must fit unchanged dataset budgets; `limits.timeout` or an earlier caller deadline bounds the whole migration. Set a suitable timeout in both catalogs. The destination needs full dataset space plus overhead; whole objects are not buffered.

Immutable remote generations let same-policy refresh advance current while copying the acquired generation. Before local publication, a final source check rejects missing/unavailable current metadata or changed fingerprints. Later revocations must still propagate to the independent local copy; this is not a distributed policy transaction.

Remote service time and local host time must agree within five seconds before copying/publication. `refreshed_at` is preserved exactly: synchronize clocks, and expect stale-query denial after `max_age`.

Linux private staging, no-replace publication and sync/cleanup rules match local backup. Existing destinations remain untouched. On `BACKUP_DURABILITY_UNCERTAIN`, preserve and investigate the destination; successful readback cannot certify an earlier failed sync.

### Reproduce migration acceptance

On the designated Linux host with built CLI/launcher, approved extensions and existing Python dependencies:

```sh
go test -race ./internal/acceleration ./cmd/kelvo \
  -run 'Test(ObjectMigration|Migration|CLIMigration)' -count=1

KELVO_TEST_SANDBOX_BINARY="$PWD/bin/kelvo-landlock" \
  python3 scripts/object_migration_acceptance.py \
  --binary bin/kelvo --extension-directory /approved/kelvo/extensions
```

The runner uses four signed TLS protocol fixtures with process-scoped CA trust, not host trust. It migrates single/multipart/empty data, removes the source, remote availability and credentials, then compares all values/types/NULLs through sandboxed local queries. It also rejects policy mismatch, corruption and overwrite, and checks read-only requests.

### Migration validation

[Forty-check TLS evidence](evidence/object-migration-acceptance.json) covers all four provider protocols and local recovery. It does not establish live cloud-account acceptance, service failover or throughput. Private source data and test credentials remain outside public evidence.

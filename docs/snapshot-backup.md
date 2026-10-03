# Snapshot backup and recovery

`kelvo accelerate backup` copies one current local dataset generation into a **new private acceleration root**. It verifies the copied Parquet bytes, row counts and Arrow schema before publication, preserving the generation, fingerprint, checksums and original refresh time. Single-file and multipart generations use the same command.

This is an operator command for Linux filesystems supporting atomic no-replace directory rename and directory `fsync`. Other platforms are rejected. The ordinary `backup` command rejects object-storage configurations; use the explicit [remote-to-local migration](#migrate-a-current-object-snapshot-to-local-storage) below when changing storage. It does not run a source query, resolve database secrets, update service configuration, or overwrite an existing destination.

## Create a backup

Provision an existing parent directory owned by the Kelvo OS user with mode `0700`. The destination must be a new, clean absolute path below that parent; symlinks in the path are rejected. Run as the same user that owns the source store:

```sh
kelvo accelerate backup --config kelvo.yml --dataset orders_daily \
  --destination /var/lib/kelvo-backups/orders-20261003
```

Use the source/acceleration YAML catalog, not a cluster node configuration. The selected dataset must match that catalog's current authorization fingerprint. Keep its original source definitions and `authorization_version`; the referenced database credentials need not be available during backup. Changed authorization or source/query definitions cannot be bypassed by copying an older snapshot.

The destination contains only the selected current generation and its manifests under the original tenant/dataset namespace. It does not include historical generations, other datasets, the YAML configuration, secrets or cluster/NATS state. Retain the operator catalog separately. Treat snapshot payloads as sensitive data and use operator-managed encrypted storage where required.

The configured dataset `limits.max_bytes` bounds the total encoded payload copied, up to 1 TiB. Metadata and filesystem overhead still need space outside that budget. Copying uses bounded buffers and holds source reader leases through copying and verification; concurrent refreshes may therefore retain extra source generations temporarily. The backup captures the generation acquired at its start, even if a later refresh completes during the copy.

On success, YAML output contains `verified: true` and the copied `snapshot`. Verification certifies integrity, not freshness: an expired snapshot may be backed up successfully and remains expired. `--destination` is backup-only; backup rejects `--sandbox`, `--generation` and `--expected-generation`.

## Recover into another fresh root

Create `kelvo-backup.yml` from the retained catalog, changing only `acceleration.directory` to the backup root:

```yaml
acceleration:
  directory: /var/lib/kelvo-backups/orders-20261003
  # Preserve tenant_id, datasets and all other catalog definitions.
```

Copy the verified backup into another new root under an existing private parent:

```sh
kelvo accelerate backup --config kelvo-backup.yml --dataset orders_daily \
  --destination /var/lib/kelvo-recovery/orders-recovered
```

Create `kelvo-recovered.yml` with `acceleration.directory` pointing at that recovered root, then verify and inspect readiness:

```sh
kelvo accelerate verify --config kelvo-recovered.yml --dataset orders_daily
kelvo accelerate status --config kelvo-recovered.yml --dataset orders_daily
```

Run the intended analytical query against the recovered catalog before explicitly switching service configuration and mounts. The tool does not perform that switch. A single-dataset root cannot restore the other datasets in a shared catalog; plan their recovery separately. Cluster workers must agree on the recovered path, tenant and catalog definitions.

The original refresh timestamp is never reset. If `max_age` has elapsed, snapshot queries continue to reject the data until a valid refresh occurs. Do not treat a successful copy as a new source observation. Existing `accelerate restore` has a different purpose: it switches a retained generation within an existing store and requires a verifiable current generation. Copying from a separate good backup can establish a fresh store when the original current payload is corrupt.

## Failed or interrupted publication

Kelvo stages the copy privately and publishes with a no-replace rename. Failures before publication do not publish this operation's destination and clean up its owned staging files. A pre-existing destination, or one created concurrently by another process, is preserved and may still exist. Filesystem failures can also prevent cleanup, so preserve the error and inspect operator-owned storage before retrying.

A directory sync or cleanup error can occur **after** the destination was published. The Go API can then return a populated snapshot together with an error; the CLI returns `BACKUP_DURABILITY_UNCERTAIN` with an instruction to preserve the destination, resolve storage errors and verify before use. It does not print `verified: true`. Do not delete the destination or retry onto it merely because the command failed. Inspect whether it exists, use a catalog pointing there to run `accelerate verify`, and resolve the storage durability issue before relying on the copy. A later successful read cannot prove that an earlier failed `fsync` was crash-durable. Use another new destination for a fresh attempt.

Backup scheduling, cross-host replication and service failover remain operator responsibilities. This command alone establishes no recovery-time or recovery-point objective. Measure recovery using your dataset sizes and storage; assess the recovery point from the preserved refresh time and source extraction semantics. Independent dataset backups are not a transactionally consistent cross-database snapshot.

## Reproduce the recovery gate

On a dedicated Linux test machine with PyArrow installed and the Kelvo binary
and sandbox launcher already built:

```sh
python3 -m unittest discover -s scripts -p test_backup_acceptance.py
python3 scripts/backup_acceptance.py --binary bin/kelvo \
  --launcher bin/kelvo-landlock --rows 1000000 \
  --output artifacts/backup-recovery-new.json
```

The runner creates private synthetic fixtures, backs up a current generation,
publishes seven additional fixture rows, then removes only its generated source
and live snapshot store. Recovery must preserve the original identity and every
Arrow value, type and NULL. It also rejects overwrite and changed authorization,
checks empty snapshots, and confirms that copied stale data remains unavailable
to queries. The report path must be new. CI runs the same gate with 100,000 rows.

The [Azure evidence](evidence/backup-acceptance.json) records runtime `5844d62`,
CLI `56331d6` and runner `78d7b5f`, with hashes for the actual binary, launcher and
runner. All four scenarios and private fixture cleanup passed on 3 October 2026.

| Snapshot | Rows | Parquet bytes | Parts | Backup copy and verification | Recovery through verified first query |
| --- | ---: | ---: | ---: | ---: | ---: |
| single | 1,000,000 | 120,907,846 | 1 | 0.865 s | 2.611 s |
| multipart | 1,000,000 | 122,068,203 | 145 | 1.819 s | 3.769 s |
| single empty | 0 | 712 | 1 | 0.064 s | 0.160 s |
| multipart empty | 0 | 712 | 1 | 0.064 s | 0.144 s |

These are single-run, warm-cache local measurements including process startup,
storage verification and exact result comparison, not a failover SLA or a
throughput benchmark. The seven fixture rows written after the backup are
intentionally absent after recovery; this illustrates the backup boundary, not
a measured source-transaction recovery-point objective. No remote transfer,
service configuration switch or cold-cache control was included.

The complete ordinary and federation-bridge Go suites and `go vet` passed on
Azure. Focused backup/CLI race tests cover concurrent refresh/prune, no-replace
publication, short writes, cancellation, corrupt payloads, conflicting manifests
and post-publication sync failures. Five acceptance-control tests reject damaged
or ambiguous evidence, changed types/values/NULLs, truncated IPC and concatenated
Arrow streams. The macOS acceleration package fallback was cross-compiled on
Linux; backup remains unsupported there.


## Migrate a current object snapshot to local storage

`kelvo accelerate migrate-backup` copies one current remote generation into a
**new private local acceleration root**, verifying exact object versions, payload
hashes, Parquet footer rows and Arrow schemas. It works with the configured S3,
R2, GCS or Azure Blob reader path; it requires no remote writer identity, remote
mutation, object listing or source-database query.

This is a deliberate storage migration, distinct from an ordinary backup.
Object-storage location and credential-reference configuration participate in
Kelvo's snapshot fingerprint. The command therefore requires **both catalogs**
and derives the new local fingerprint from the target catalog. It preserves the
original generation, payload bytes, schema and refresh timestamp. Its YAML
success output includes `source_fingerprint` and `source_generation_sha256`
alongside the local `snapshot` for provenance. Retain that output with both
operator catalogs. A multipart local manifest uses its own ordered-part digest;
the original remote descriptor digest remains in the provenance output.

1. Retain the remote catalog as `remote.yml`.
2. Create `local.yml` from the same catalog. Change only
   `acceleration.directory` to a new absolute path under an existing private
   parent and remove `acceleration.object_storage`.
3. Keep all source definitions, query parameters, tenant/dataset IDs,
   authorization versions, schema policies, limits, refresh settings and
   extension directory exactly equal. The command rejects any other change
   before opening object credentials or storage.
4. Run the explicit migration, then verify and query the local copy:

```sh
kelvo accelerate migrate-backup --config remote.yml \
  --destination-config local.yml --dataset orders_daily

kelvo accelerate verify --config local.yml --dataset orders_daily
kelvo query --config local.yml --sources orders_daily \
  --sql 'SELECT COUNT(*) AS rows FROM orders_daily' --out recovered.arrow
```

Use the usual `--sandbox` option on the verification query when deploying
sandboxed query workers. Migration itself runs in the trusted parent and cannot
accept a sandbox, generation selector or source-query override. It needs the
remote catalog's **reader** environment references during copying. Database and
writer credentials are not resolved. After success, verification and queries
against the local catalog require neither object credentials nor object access.
The command does not change running services, replace catalogs or transfer
historical generations. A local root containing one dataset does not recover
other datasets named in a shared catalog.

### Integrity, policy and failure boundaries

The migration captures the current authorized generation at acquisition. Each
payload is fetched with its exact immutable version and copied sequentially
through a 256 KiB buffer; Parquet footer verification has its existing bounded
metadata limits. The complete encoded payload and row count must fit the
unchanged target dataset budgets. The manager/CLI also bounds the complete
migration by the unchanged dataset `limits.timeout` (or an earlier caller
deadline); long copies need a suitable timeout in both catalogs before starting.
Multipart generation descriptors and part
counts retain their existing bounds. No complete object or dataset is buffered
in memory, but the destination needs space for the entire copied dataset plus
metadata and filesystem overhead.

Kelvo currently never deletes remote generations automatically. That immutability
contract lets a concurrent same-policy refresh advance `current` while the copy
finishes with the originally captured generation. A final source-manifest check
rejects a missing/unavailable current manifest or a changed authorization/config
fingerprint before local publication. It is not a distributed transaction with
future remote policy changes: propagate later source revocations and catalog
updates to the local deployment too. A copied dataset is an independent retained
copy and must follow the same access and retention policy.

Remote freshness uses the observed object-service clock; local freshness uses
the target host clock. The command preserves `refreshed_at` exactly and rejects
an observed age discrepancy above five seconds before copying and publication.
Keep the host clock synchronized. An old snapshot remains old, and local queries
continue to reject it when `max_age` has elapsed. Migration is not a refresh.

Destination validation, descriptor-relative private staging, atomic no-replace
publication, directory syncing and cleanup uncertainty use the same filesystem
contract as local backups. Linux support is required. A failure before
publication leaves this operation's destination absent and cleans its owned
staging, subject to filesystem errors. An existing or concurrently created
destination is preserved. If publication happened but durability or cleanup is
uncertain, the CLI returns `BACKUP_DURABILITY_UNCERTAIN`; preserve the destination
and resolve/verify it as described above. Successful readback cannot retroactively
prove a failed `fsync` was durable.

### Reproduce migration acceptance

The dedicated Linux test host needs the built CLI and sandbox launcher, approved
extension directory and the existing Python acceptance dependencies:

```sh
go test -race ./internal/acceleration ./cmd/kelvo \
  -run 'Test(ObjectMigration|Migration|CLIMigration)' -count=1

KELVO_TEST_SANDBOX_BINARY="$PWD/bin/kelvo-landlock" \
  python3 scripts/object_migration_acceptance.py \
  --binary bin/kelvo --extension-directory /approved/kelvo/extensions
```

The runner uses the existing signed TLS object fixtures for all four provider
protocols. Its CA is scoped to the fresh parent CLI processes with `SSL_CERT_FILE`
and an empty `SSL_CERT_DIR`; it never installs host trust. Sandboxed workers read
only local files and receive no fixture trust variables. It migrates single,
multipart and empty snapshots,
then removes the generated source file, makes every remote object unavailable,
removes object credentials and checks every recovered Arrow value/type/NULL in a
sandboxed query. It also checks policy mismatch before object access, read-only
requests, corrupt metadata and no overwrite. These are protocol/development
correctness gates, not real cloud account acceptance, an RTO guarantee or a
throughput measurement. Source snapshots and test credentials remain in private
ignored fixture directories; only bounded result evidence is public.

### Migration validation

The [signed TLS acceptance record](evidence/object-migration-acceptance.json) passed
40 checks across S3, R2, GCS and Azure protocol fixtures. Each covers single-file,
multipart and empty generations, existing-destination rejection, policy mismatch
and corrupted metadata. Recovery queries ran through sandboxed workers after the
source file was removed and all fixture objects and object credentials became
unavailable. These are protocol and local recovery checks, not live cloud-provider
acceptance or a service failover measurement. The fixture uses process-scoped CA
trust and makes no host trust changes.

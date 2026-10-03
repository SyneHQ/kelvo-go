# Local snapshot backup and recovery

`kelvo accelerate backup` copies one current local dataset generation into a **new private acceleration root**. It verifies the copied Parquet bytes, row counts and Arrow schema before publication, preserving the generation, fingerprint, checksums and original refresh time. Single-file and multipart generations use the same command.

This is an operator command for Linux filesystems supporting atomic no-replace directory rename and directory `fsync`. Other platforms and object-storage configurations are rejected. It does not run a source query, resolve database secrets, update service configuration, or overwrite an existing destination.

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

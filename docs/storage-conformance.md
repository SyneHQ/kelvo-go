# Storage release gates

`scripts/storage_conformance.py` runs correctness and optional large/TLS gates on a **dedicated Linux test host**. It does not provision machines, install dependencies, fetch extensions or accept cloud credentials.

The source must be a Git checkout with readable revision/status. Required named tests and every required subtest must emit Go JSON pass events; a zero exit code or skipped test is insufficient.

## Prerequisites

1. Preinstall Go, Git and build dependencies. Module/toolchain downloads, external Go workspaces/settings and inherited Git overrides are disabled; existing compiler/linker settings remain.
2. Choose a canonical, existing, user-owned mode-0700 work directory and a new absolute report path, without symlink components.
3. Run the correctness gate:

```sh
python3 scripts/storage_conformance.py \
  --dedicated-test-machine \
  --work-directory /mnt/test-private \
  --output /mnt/reports/storage-correctness.json
```

`--source-directory` defaults to the script's checkout. Optional `--binary` records an existing executable's hash; ordinary tests do not execute it, and its association with the selected source remains explicitly unverified.

## Datasets above 4 GiB

```sh
python3 scripts/storage_conformance.py \
  --dedicated-test-machine \
  --work-directory /mnt/test-private \
  --output /mnt/reports/storage-large.json \
  --large-local --large-remote \
  --extension-directory /mnt/approved-extensions
```

Each large gate requires 16 GiB free before starting. `TMPDIR`/`GOTMPDIR` use that volume; this reserves neither disk nor memory, and the Go build cache can use another volume.

Required tests are `TestMultipartDatasetLargerThanFourGiB` and `TestObjectMultipartDatasetLargerThanFourGiB`. Preinstall `duckdb_arrow` dependencies and a signed HTTPFS extension; DuckDB checks the signature when loading it. Synthetic disk-backed fixtures do not establish cloud performance.

## TLS protocol fixtures

```sh
python3 scripts/storage_conformance.py \
  --dedicated-test-machine \
  --work-directory /mnt/test-private \
  --output /mnt/reports/storage-tls.json \
  --tls-fixtures --install-test-ca \
  --binary /mnt/release/kelvo \
  --extension-directory /mnt/approved-extensions
```

Prepare PyArrow, OpenSSL, signed HTTPFS and noninteractive sudo for the existing harnesses' temporary CA installation/removal. `--install-test-ca` explicitly acknowledges that mutation.

The single-object/multipart harnesses own their CAs, processes and cleanup, and refresh fixed reports under `docs/evidence/`. The runner checks fresh evidence, expected providers/checks, binary hash, exit status and cleanup. These are local S3/R2/GCS/Azure protocol fixtures, not live cloud accounts.

## Timeouts and evidence

| Setting/event | Behavior |
| --- | --- |
| Gate timeout / cleanup grace | Defaults 1,800s / 120s |
| Ordinary Go timeout | SIGTERM, then SIGKILL after grace |
| TLS harness timeout | SIGTERM only so `finally` cleanup can run |
| TLS cleanup exceeds grace | Fail with `cleanup_pending: true`; stop subsequent gates and leave cleanup running |
| Interrupt or timeout | Gate remains failed even if cleanup succeeds |

If cleanup is pending, inspect the private PID/diagnostics; never assume temporary trust was removed. The runner does not forcibly kill fixture cleanup trees.

Private child logs are capped at 8 MiB each and retained in unique work directories; harness artifacts have separate operator retention/quota requirements. Public evidence excludes paths, commands, environment, raw errors and cloud identities. Durations include compilation/verification and are not query timings.

Reports include revision/dirty state, before/after source digest, runner/binary hashes, named pass proof, skipped/not-run scopes and cleanup. Source hashing covers bounded code/config/fixtures, including untracked inputs: ≤20,000 files and ≤256 MiB. Generated artifacts/evidence, hidden directories, dependency directories and private run output are excluded. Relevant changes or runner mutation fail the gate.

A source digest does not authenticate compilers, module caches, native libraries or a prebuilt binary's origin. Use clean release checkouts and independent build provenance; review private failures before sharing.

Runner-only controls use synthetic commands and do not build Go or change trust:

```sh
python3 -m unittest discover -s scripts -p test_storage_conformance.py
```

## Recorded validation

The [five-stage record](evidence/storage-conformance.json) passed at `57baf2c`: 15 named correctness tests, both over-4-GiB gates, 36 single-object TLS checks and 20 multipart checks. Source inputs stayed unchanged, cleanup completed and ten runner controls passed on Azure.

An initial preflight stopped because `/usr/sbin/update-ca-certificates` was outside PATH; no tests or trust changes occurred. Adding the installed tool path allowed the run. Binary/source association remains unverified in the report; hashes identify artifacts, not a build attestation.

# Storage release gates

`python3 scripts/storage_conformance.py` runs repeatable correctness gates on a
**dedicated Linux test machine**. It never provisions a VM, installs dependencies,
fetches extensions, changes a cloud account, or accepts cloud credentials.
Ordinary Go tests compile from the selected checkout. This is an explicit
operator command, not a recommendation to run builds on a developer laptop.

The selected source must be a Git checkout with a readable commit and status.
An rsync archive without Git metadata fails provenance preflight; the runner does
not invent a revision or silently label an archive as a clean checkout.

The default gate requires named schema-evolution, local multipart and remote
multipart tests to emit Go JSON `pass` events and the package to pass. Exit code
zero alone is insufficient. Missing tests, skipped required tests, skipped
subtests and failed subtests fail the gate. Optional expensive gates execute
sequentially, after ordinary correctness passes.

## Prerequisites

Prepare Go, Git and all module/build dependencies before running. Go module and
network toolchain downloads are disabled with `GOPROXY=off` and
`GOTOOLCHAIN=local`; inherited `GOFLAGS` are cleared. `GOWORK=off` and `GOENV=off`
disable external Go workspaces and persisted Go settings. Inherited `GIT_*`
overrides and global/system Git configuration are disabled for provenance. Existing C/C++ compiler and
linker configuration is preserved. Large gates require the project's
`duckdb_arrow` dependencies already available.

Select an existing, user-owned mode-0700 work directory on the test volume and a
new absolute report filename. Paths must be canonical and contain no symlinks.
The runner refuses to overwrite an existing report. Commands below use example
paths that the operator must replace with their own canonical paths.

```sh
python3 scripts/storage_conformance.py \
  --dedicated-test-machine \
  --work-directory /mnt/test-private \
  --output /mnt/reports/storage-correctness.json
```

`--source-directory` defaults to the checkout containing this script. `--binary`
may optionally identify an already-built executable for provenance; the default
Go gate does not run that binary. Without it, `binary_sha256` is explicitly null.
A supplied binary hash does not prove that the binary was built from the selected
source revision.

## Datasets above 4 GiB

```sh
python3 scripts/storage_conformance.py \
  --dedicated-test-machine \
  --work-directory /mnt/test-private \
  --output /mnt/reports/storage-large.json \
  --large-local --large-remote \
  --extension-directory /mnt/approved-extensions
```

Each large gate checks for at least 16 GiB free on the work volume before starting.
`TMPDIR` and `GOTMPDIR` point inside that volume. This is a preflight check, not a
storage reservation or process-memory limit; concurrent external workloads can
still exhaust the machine. Go's preconfigured build cache can consume space on
its own volume.

The local gate requires `TestMultipartDatasetLargerThanFourGiB` to pass. The remote
gate requires `TestObjectMultipartDatasetLargerThanFourGiB` and a preinstalled,
signed `httpfs.duckdb_extension`. DuckDB validates the extension signature during
loading; file presence alone is not signature verification. These gates use
synthetic data, and the remote gate uses a disk-backed object fixture. They do
not measure a live cloud service or establish customer throughput claims.

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

This explicitly invokes `object_acceleration_acceptance.py` and
`object_multipart_acceptance.py` against all four provider-protocol fixtures.
Prepare PyArrow, OpenSSL, a signed HTTPFS extension and the existing
harnesses' noninteractive sudo access for temporary CA installation/removal.
`--install-test-ca` is mandatory acknowledgement of that trust-store mutation.
The runner does not install or remove trust itself.

The existing harnesses retain ownership of their temporary CA, child processes
and cleanup. Their fixed reports under `docs/evidence/` are refreshed by the
harnesses. The runner checks fresh evidence, every expected provider/check,
binary hash, exit status and absence of cleanup failure. It does not convert
cleanup failure into a pass. These are local TLS protocol tests, not live S3,
R2, GCS or Azure conformance.

## Timeouts and evidence

`--timeout-seconds` defaults to 1,800 per gate; `--cleanup-seconds` defaults to 120.
An interrupt or timeout fails the gate even if cleanup later exits successfully.
Ordinary Go process groups receive SIGTERM, then SIGKILL after the cleanup grace.
TLS harnesses receive SIGTERM only, allowing their `finally` cleanup to run. If a
harness exceeds the grace period, the runner fails with `cleanup_pending: true`,
stops subsequent gates and leaves it running to finish cleanup. Inspect the
private child PID and the harness's private diagnostics on the dedicated host;
do not assume the temporary trust has been removed. The runner never forcibly
kills a fixture's cleanup process tree.

Raw child output is captured privately, capped at 8 MiB per runner log, and never
copied into public evidence. Unique run directories remain under the selected
work directory for operator inspection. Existing harnesses also retain their
own private diagnostics under ignored `artifacts/`; their retention and volume
quotas remain operator responsibilities.

The report includes revision, dirty status and a bounded source-input SHA256
before/after success, the runner script's own SHA256, the supplied
binary hash, selected/skipped/not-run scopes, named pass proof, exit codes and timeout or
cleanup outcomes. It excludes commands, paths, environment values, raw output
and cloud identities. Elapsed gate durations include builds and verification;
they are not query benchmarks.

The source digest includes Go, C/C++/headers/assembly, module files, Python,
configuration/query files and test fixtures, including untracked inputs. It
excludes generated `artifacts/`, `docs/evidence/`, hidden directories,
`node_modules/`, virtual environments and the runner's private run directory.
Input hashing is capped at 20,000 files and 256 MiB; larger input sets fail
explicitly. Relevant content changes between captures fail the run, even if HEAD
and the dirty flag are unchanged. This digest does not authenticate the compiler,
module cache, external libraries or a supplied prebuilt binary's source; the
binary/source association is explicitly marked unverified. The runner hash
identifies orchestration code even when `--source-directory` selects a different
checkout; a changed runner file fails the final identity check. Use a clean release
checkout and your build provenance system for release attribution. Review failed
private logs before sharing them.

The runner's own unit tests use synthetic commands and fixture evidence; they
perform no Go builds or trust-store changes:

```sh
python3 -m unittest discover -s scripts -p test_storage_conformance.py
```


## Recorded validation

The [full five-stage run](evidence/storage-conformance.json) passed against
source revision `57baf2c`: 15 named correctness tests, both over-4-GiB gates,
36 single-object TLS checks and 20 multipart TLS checks. The captured source
input digest stayed unchanged; only generated evidence made the checkout dirty.
All fixture cleanup completed. Ten synthetic runner-control tests also passed
on the dedicated Azure VM.

The first invocation stopped during preflight because `update-ca-certificates`
was outside the noninteractive user's PATH. No tests or trust changes occurred.
Adding the installed `/usr/sbin` tools to PATH allowed the recorded run. The
report explicitly leaves the supplied binary/source association unverified;
its hash and runner hash identify the artifacts used, not a build attestation.

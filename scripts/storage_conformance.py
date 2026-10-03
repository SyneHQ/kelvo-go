#!/usr/bin/env python3
"""Explicit, offline-by-default storage release gates for a dedicated Linux VM."""

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time

PACKAGE = "github.com/SYNEHQ/kelvo-go/internal/acceleration"
REQUIRED = (
    "TestSchemaEvolutionIntegerDirectionalMatrix",
    "TestSchemaEvolutionIndependentFlagsAndAppendOnlyColumns",
    "TestSchemaEvolutionTemporalChangesRemainExplicit",
    "TestSchemaEvolutionPublicationMatrix",
    "TestSchemaEvolutionDoesNotRelaxWithinGeneration",
    "TestSchemaEvolutionPolicyFingerprintFencesExistingSnapshot",
    "TestMultipartStoreRoundTripAndEmpty",
    "TestMultipartStoreRestoreAndWholeGenerationPins",
    "TestMultipartPruneResumesInterruptedRetirement",
    "TestMultipartPruneReclaimsInterruptedPublication",
    "TestMultipartSealRejectsFalseRowCount",
    "TestObjectMultipartCommitStagesOnePartAndPublishesDescriptor",
    "TestObjectMultipartLeaseLossCannotPublish",
    "TestObjectMultipartAmbiguousPublication",
    "TestObjectMultipartCanceledAfterUploadCannotPublish",
)
LARGE = {"large_local": ("TestMultipartDatasetLargerThanFourGiB", "KELVO_TEST_MULTIPART_LARGE"),
         "large_remote": ("TestObjectMultipartDatasetLargerThanFourGiB", "KELVO_TEST_OBJECT_MULTIPART_LARGE")}
PROVIDERS = ("s3", "r2", "gcs", "azure")
FIXTURE_CHECKS = {
    "tls_single": (
        "refresh_publishes_exact_typed_parquet",
        "fresh_reader_uses_bounded_remote_ranges_without_source_or_shared_snapshot",
        "reader_only_status_verify_and_cloud_write_denial",
        "alternating_remote_and_local_snapshot_wall_timings",
        "source_outage_refresh_preserves_committed_generation",
        "missing_corrupt_unsupported_object_metadata_fails_closed",
        "remote_redirect_ignored_range_and_wrong_interval_fail_closed",
        "unsupported_manifest_version_fails_closed",
        "missing_manifest_fails_closed_and_restore_recovers"),
    "tls_multipart": (
        "multipart_upload_commits_descriptor_and_bounded_parts",
        "fresh_reader_queries_all_parts_with_reader_only_bounded_ranges",
        "single_and_multipart_remote_restore_preserves_values_and_freshness",
        "changed_descriptor_and_missing_last_part_fail_closed",
        "multipart_transport_failures_and_reader_write_denial"),
}
LOG_LIMIT = 8 << 20
LINE_LIMIT = 64 << 10
MIN_FREE = 16 << 30
INPUT_LIMIT = 256 << 20
INPUT_FILE_LIMIT = 20000
INPUT_SUFFIXES = {".go", ".c", ".cc", ".cpp", ".cxx", ".h", ".hpp", ".s", ".mod", ".sum",
                  ".py", ".yaml", ".yml", ".json", ".csv", ".sql", ".parquet", ".arrow", ".ipc"}
INPUT_EXCLUDES = {"artifacts", "node_modules", "__pycache__", "venv"}


class GateError(Exception):
    """Only constant, public-safe failure categories belong in this exception."""


def canonical(path):
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path:
        raise GateError("path_must_be_absolute_and_without_symlinks")
    return path


def directory(path, private=False):
    path = canonical(path)
    info = path.stat()
    if not stat.S_ISDIR(info.st_mode):
        raise GateError("directory_required")
    if private and (info.st_uid != os.getuid() or info.st_mode & 0o077):
        raise GateError("work_directory_must_be_owned_and_private")
    return path


def regular_file(path, executable=False):
    path = canonical(path)
    info = path.stat()
    if not stat.S_ISREG(info.st_mode) or (executable and not os.access(path, os.X_OK)):
        raise GateError("regular_file_or_executable_required")
    return path


def output_path(path):
    path = canonical(path)
    directory(path.parent)
    if path.exists() or path.is_symlink():
        raise GateError("report_destination_already_exists")
    return path


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


class GoProof:
    def __init__(self, required):
        self.required = tuple(required)
        self.passed, self.rejected = set(), set()
        self.package_passed = False

    def observe(self, line):
        try:
            event = json.loads(line)
        except (ValueError, UnicodeError):
            return
        if not isinstance(event, dict) or event.get("Package") != PACKAGE:
            return
        test, action = event.get("Test", ""), event.get("Action")
        if not isinstance(test, str):
            return
        root = test.split("/", 1)[0]
        if action in ("fail", "skip") and root in self.required:
            self.rejected.add(root)
        if action == "pass" and test in self.required:
            self.passed.add(test)
        if not test and action == "pass":
            self.package_passed = True

    def complete(self):
        return self.package_passed and not self.rejected and self.passed == set(self.required)

    def public(self):
        return {"required_tests": list(self.required),
                "passed_tests": sorted(self.passed - self.rejected),
                "missing_or_rejected_tests": sorted(set(self.required) - self.passed | self.rejected)}


@dataclass
class ProcessResult:
    exit_code: int | None
    elapsed_seconds: float
    timed_out: bool = False
    interrupted: bool = False
    cleanup_pending: bool = False
    log_truncated: bool = False

    def public(self):
        return dict(vars(self))


def run_process(command, cwd, env, log_path, timeout, grace, observer=None,
                fixture=False, cancelled=lambda: False, log_limit=LOG_LIMIT):
    """Drain bounded logs; a timed-out fixture retains its own cleanup authority."""
    started = time.monotonic()
    result = ProcessResult(None, 0)
    with log_path.open("xb") as raw:
        os.chmod(log_path, 0o600)
        process = subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        # The PID is private operational evidence, never part of the public report.
        raw.write(f"child_pid={process.pid}\n".encode())
        written, pending, discard = raw.tell(), bytearray(), False
        stop_at = None
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        try:
            while selector.get_map() or process.poll() is None:
                now = time.monotonic()
                if stop_at is None and (cancelled() or now - started >= timeout):
                    result.interrupted = bool(cancelled())
                    result.timed_out = not result.interrupted
                    stop_at = now
                    try:
                        if fixture:
                            process.send_signal(signal.SIGTERM)
                        else:
                            os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                if stop_at is not None and now - stop_at >= grace:
                    if fixture:
                        # Never SIGKILL a fixture while its finally block removes
                        # temporary trust. Fail and stop subsequent gates instead.
                        result.cleanup_pending = process.poll() is None
                    else:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            result.cleanup_pending = True
                    break
                for key, _ in selector.select(0.1):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    remaining = max(0, log_limit - written)
                    raw.write(chunk[:remaining])
                    written += min(len(chunk), remaining)
                    result.log_truncated |= len(chunk) > remaining
                    for fragment in chunk.splitlines(keepends=True):
                        if not discard:
                            pending.extend(fragment)
                            if len(pending) > LINE_LIMIT:
                                pending.clear()
                                discard = True
                        if fragment.endswith(b"\n"):
                            if not discard and observer:
                                observer(bytes(pending))
                            pending.clear()
                            discard = False
            if process.poll() is None and not result.cleanup_pending:
                try:
                    process.wait(timeout=grace)
                except subprocess.TimeoutExpired:
                    result.cleanup_pending = True
            result.exit_code = process.poll()
        finally:
            selector.close()
            process.stdout.close()
    result.elapsed_seconds = round(time.monotonic() - started, 3)
    return result


def source_input_digest(source, excluded=()):
    """Bounded content identity of runtime/test inputs, including untracked files."""
    digest, total, count = hashlib.sha256(), 0, 0
    excluded = set(excluded)
    for current, dirs, names in os.walk(source, followlinks=False):
        current = Path(current)
        dirs[:] = sorted(name for name in dirs if not name.startswith(".") and name not in INPUT_EXCLUDES
                         and current / name not in excluded and current / name != source / "docs/evidence")
        for name in sorted(names):
            path = current / name
            relative = path.relative_to(source)
            if path.suffix.lower() not in INPUT_SUFFIXES and "testdata" not in relative.parts:
                continue
            regular_file(path)
            before = path.stat()
            count += 1
            if count > INPUT_FILE_LIMIT or before.st_size > INPUT_LIMIT - total:
                raise GateError("source_input_digest_limit_exceeded")
            content, size = hashlib.sha256(), 0
            with path.open("rb") as stream:
                for block in iter(lambda: stream.read(1 << 20), b""):
                    size += len(block)
                    if size > INPUT_LIMIT - total:
                        raise GateError("source_input_digest_limit_exceeded")
                    content.update(block)
            after = path.stat()
            if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns) or size != before.st_size:
                raise GateError("source_changed_while_hashing")
            encoded = relative.as_posix().encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big") + encoded + size.to_bytes(8, "big") + content.digest())
            total += size
    return {"input_sha256": digest.hexdigest(), "input_files": count, "input_bytes": total,
            "input_scope": "runtime_and_test_inputs_excluding_generated_artifacts_and_evidence"}


def source_provenance(source, run_dir, env, suffix):
    revision = []
    result = run_process(["git", "rev-parse", "--verify", "HEAD"], source, env,
                         run_dir / f"revision-{suffix}.log", 15, 5,
                         lambda line: revision.append(line.strip()) if not revision else None)
    if result.exit_code != 0 or not revision or not re.fullmatch(rb"[0-9a-f]{40,64}", revision[0]):
        raise GateError("source_revision_unavailable")
    dirty = []
    result = run_process(["git", "status", "--porcelain=v1", "--untracked-files=normal"], source, env,
                         run_dir / f"dirty-{suffix}.log", 15, 5,
                         lambda line: dirty.append(True) if line.strip() and not dirty else None)
    if result.exit_code != 0:
        raise GateError("source_status_unavailable")
    return {"revision": revision[0].decode("ascii"), "dirty": bool(dirty), **source_input_digest(source, (run_dir,))}


def fixture_proof(path, previous, required, binary_sha256):
    try:
        return _fixture_proof(path, previous, required, binary_sha256)
    except (OSError, ValueError, TypeError, GateError):
        return False


def _fixture_proof(path, previous, required, binary_sha256):
    regular_file(path)
    info = path.stat()
    if (info.st_ino, info.st_size, info.st_mtime_ns) == previous or info.st_size > 2 << 20:
        return False
    with path.open("rb") as stream:
        encoded = stream.read((2 << 20) + 1)
    if len(encoded) > 2 << 20:
        return False
    data = json.loads(encoded)
    if not isinstance(data, dict) or data.get("passed") is not True or "cleanup_failure" in data:
        return False
    if data.get("binary_sha256") != binary_sha256 or set(data.get("providers", [])) != set(PROVIDERS):
        return False
    checks = data.get("checks", [])
    if not isinstance(checks, list) or len(checks) > 1024:
        return False
    observed = set()
    for check in checks:
        if not isinstance(check, dict) or check.get("passed") is not True:
            return False
        provider, name = check.get("provider"), check.get("test")
        if isinstance(provider, str) and isinstance(name, str):
            observed.add((provider, name))
    return all((provider, name) in observed for provider in PROVIDERS for name in required)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--dedicated-test-machine", action="store_true", required=True)
    p.add_argument("--source-directory", type=Path, default=Path(__file__).resolve().parents[1])
    p.add_argument("--work-directory", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--binary", type=Path)
    p.add_argument("--extension-directory", type=Path)
    p.add_argument("--large-local", action="store_true")
    p.add_argument("--large-remote", action="store_true")
    p.add_argument("--tls-fixtures", action="store_true")
    p.add_argument("--install-test-ca", action="store_true")
    p.add_argument("--timeout-seconds", type=int, default=1800)
    p.add_argument("--cleanup-seconds", type=int, default=120)
    return p


def main(argv=None):
    args = parser().parse_args(argv)
    report = {"schema_version": 1, "passed": False,
              "checked_at": datetime.now(timezone.utc).isoformat(), "steps": [],
              "source": None, "runner_sha256": None, "binary_sha256": None, "binary_source_association": "not_supplied",
              "options": {name: bool(getattr(args, name)) for name in
                          ("large_local", "large_remote", "tls_fixtures", "install_test_ca")},
              "skipped_scope": [name for name in ("large_local", "large_remote", "tls_fixtures") if not getattr(args, name)],
              "scope": "Storage correctness gates; TLS fixtures are not real-cloud conformance or throughput benchmarks"}
    output, run_dir = None, None
    interrupted = [False]
    previous_handlers = {}
    try:
        if sys.platform != "linux":
            raise GateError("dedicated_linux_machine_required")
        if not 1 <= args.timeout_seconds <= 7200 or not 1 <= args.cleanup_seconds <= 300:
            raise GateError("invalid_timeout_bounds")
        output = output_path(args.output)
        runner = regular_file(Path(__file__).resolve())
        report["runner_sha256"] = file_hash(runner)
        source = directory(args.source_directory)
        regular_file(source / "go.mod")
        work = directory(args.work_directory, private=True)
        run_dir = Path(tempfile.mkdtemp(prefix="storage-conformance-", dir=work))
        temp = run_dir / "tmp"
        temp.mkdir(mode=0o700)
        env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
        env.update(GOPROXY="off", GOSUMDB="off", GOTOOLCHAIN="local", GOFLAGS="", GOWORK="off", GOENV="off",
                   TMPDIR=str(temp), GOTMPDIR=str(temp), GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
        for name in ("KELVO_TEST_MULTIPART_LARGE", "KELVO_TEST_OBJECT_MULTIPART_LARGE", "KELVO_OBJECT_EXTENSION_DIRECTORY"):
            env.pop(name, None)
        for tool in ("go", "git"):
            if not shutil.which(tool):
                raise GateError("required_preinstalled_tool_missing")
        if args.binary:
            args.binary = regular_file(args.binary, executable=True)
            report["binary_sha256"] = file_hash(args.binary)
            report["binary_source_association"] = "unverified"
        if args.large_remote or args.tls_fixtures:
            if not args.extension_directory:
                raise GateError("extension_directory_required")
            args.extension_directory = directory(args.extension_directory)
            regular_file(args.extension_directory / "httpfs.duckdb_extension")
        if args.install_test_ca and not args.tls_fixtures:
            raise GateError("test_ca_acknowledgement_requires_tls_fixtures")
        if args.tls_fixtures:
            if not args.binary or not args.install_test_ca:
                raise GateError("tls_requires_binary_and_explicit_test_ca_acknowledgement")
            for tool in ("openssl", "sudo", "update-ca-certificates"):
                if not shutil.which(tool):
                    raise GateError("required_preinstalled_tls_tool_missing")
            import importlib.util
            if importlib.util.find_spec("pyarrow") is None:
                raise GateError("preinstalled_pyarrow_required")
        for number in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[number] = signal.signal(number, lambda *_: interrupted.__setitem__(0, True))
        report["source"] = source_provenance(source, run_dir, env, "before")
        stages = [("correctness", REQUIRED, None)]
        stages.extend((name, (test,), variable) for name, (test, variable) in LARGE.items() if getattr(args, name))
        for name, required, variable in stages:
            if interrupted[0]:
                raise GateError("interrupted")
            if variable and shutil.disk_usage(temp).free < MIN_FREE:
                raise GateError("large_gate_requires_16_gib_free_scratch")
            proof = GoProof(required)
            command = ["go", "test", "-json", "-count=1", "-p", "2", f"-timeout={args.timeout_seconds}s"]
            stage_env = env.copy()
            if variable:
                command.extend(["-tags", "duckdb_arrow"])
                stage_env[variable] = "1"
                if name == "large_remote":
                    stage_env["KELVO_OBJECT_EXTENSION_DIRECTORY"] = str(args.extension_directory)
            command.extend(["./internal/acceleration", "-run", "^(" + "|".join(required) + ")$"])
            result = run_process(command, source, stage_env, run_dir / f"{name}.log",
                                 args.timeout_seconds, args.cleanup_seconds, proof.observe,
                                 cancelled=lambda: interrupted[0])
            passed = result.exit_code == 0 and not (result.timed_out or result.interrupted or result.cleanup_pending) and proof.complete()
            report["steps"].append({"gate": name, "passed": passed, **result.public(), **proof.public()})
            if not passed:
                raise GateError("go_gate_failed_or_required_test_not_passed")
        if args.tls_fixtures:
            for name, script, evidence in (
                    ("tls_single", "object_acceleration_acceptance.py", "object-acceleration.json"),
                    ("tls_multipart", "object_multipart_acceptance.py", "object-multipart-acceptance.json")):
                if interrupted[0]:
                    raise GateError("interrupted")
                fixture_report = canonical(source / "docs/evidence" / evidence)
                before = None
                if fixture_report.exists():
                    regular_file(fixture_report)
                    info = fixture_report.stat()
                    before = (info.st_ino, info.st_size, info.st_mtime_ns)
                command = [sys.executable, str(regular_file(source / "scripts" / script)), "--binary", str(args.binary),
                           "--extension-directory", str(args.extension_directory), "--install-test-ca"]
                result = run_process(command, source, env, run_dir / f"{name}.log", args.timeout_seconds,
                                     args.cleanup_seconds, fixture=True, cancelled=lambda: interrupted[0])
                passed = result.exit_code == 0 and not (result.timed_out or result.interrupted or result.cleanup_pending)
                passed = passed and fixture_proof(fixture_report, before, FIXTURE_CHECKS[name], report["binary_sha256"])
                report["steps"].append({"gate": name, "passed": passed, **result.public(),
                                       "required_fixture_checks": len(PROVIDERS) * len(FIXTURE_CHECKS[name])})
                if not passed:
                    raise GateError("tls_gate_or_trust_cleanup_failed")
        if interrupted[0]:
            raise GateError("interrupted")
        report["source_after"] = source_provenance(source, run_dir, env, "after")
        if report["source_after"]["revision"] != report["source"]["revision"]:
            raise GateError("source_revision_changed_during_run")
        if report["source_after"]["input_sha256"] != report["source"]["input_sha256"]:
            raise GateError("source_inputs_changed_during_run")
        if file_hash(runner) != report["runner_sha256"]:
            raise GateError("runner_changed_during_run")
        if args.binary and file_hash(args.binary) != report["binary_sha256"]:
            raise GateError("binary_changed_during_run")
        if interrupted[0]:
            raise GateError("interrupted")
        report["passed"] = True
    except Exception as error:
        report["failure_category"] = str(error) if isinstance(error, GateError) else "preflight_or_evidence_error"
        if run_dir:
            import traceback
            try:
                with (run_dir / "failure.log").open("xb") as stream:
                    os.chmod(run_dir / "failure.log", 0o600)
                    stream.write(traceback.format_exc().encode()[:LINE_LIMIT])
            except OSError:
                pass
    finally:
        selected = ["correctness"] + [name for name in LARGE if getattr(args, name)]
        if args.tls_fixtures:
            selected.extend(FIXTURE_CHECKS)
        completed = {step["gate"] for step in report["steps"]}
        report["not_run_gates"] = [name for name in selected if name not in completed]
        for number, handler in previous_handlers.items():
            signal.signal(number, handler)
        if output:
            try:
                descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(descriptor, "w") as stream:
                    json.dump(report, stream, indent=2)
                    stream.write("\n")
            except OSError:
                report["passed"] = False
    print("Storage conformance " + ("passed" if report["passed"] else "failed") + "; review the selected report and private work directory.")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())

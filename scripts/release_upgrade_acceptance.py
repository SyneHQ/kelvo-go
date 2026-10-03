#!/usr/bin/env python3
"""Bounded Linux release compatibility against the immutable public preview.

Installs nothing. Uses already built binaries, disposable local snapshots and
signed loopback TLS object fixtures. Object queries are checked after explicit
remote-to-local migration, avoiding host CA changes and extension downloads.
"""
import argparse
import copy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import signal
import subprocess
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from backup_acceptance import AcceptanceError, digest, identity, require, scalar_snapshot, source_table, verify_arrow
from object_acceleration_acceptance import PROVIDERS
from object_migration_acceptance import ObjectMigrationAcceptance

PREVIOUS_TAG = "v0.1.0-preview.1"
PREVIOUS_REVISION = "73a8eb44b7b00f6416f7bbefcdd6434fa7dbce0e"
PREVIOUS_BINARY_SHA256 = "087ca86d2f56c8a0d3ff612ed955f390e75f0111cc1e4bd38211256f8c31a016"
MAX_OUTPUT = 1 << 20
DATASET = "saved"
TENANT = "upgrade-test"
REMOTE_TENANT = "acceptance"  # The shared protocol fixture authorizes exactly this namespace.
REMOTE_ROWS = 40000  # Stay within the shared TLS fixture's 8 MiB request limit.
SCRIPTS = ("release_upgrade_acceptance.py", "backup_acceptance.py", "object_migration_acceptance.py",
           "object_acceleration_acceptance.py", "acceleration_acceptance.py")


def fingerprint_files(directory):
    """Capture immutable payload/metadata state, excluding advisory lock files."""
    return {str(path.relative_to(directory)): digest(path) for path in sorted(directory.rglob("*"))
            if path.is_file() and path.suffix in (".parquet", ".yaml")}


def require_identity(actual, expected):
    require(identity(actual) == identity(expected), "SNAPSHOT_IDENTITY_OR_AGE_CHANGED")


def manifest_version(raw):
    matches = re.findall(rb"^version: ([0-9]+)$", raw, re.MULTILINE)
    require(len(matches) == 1, "MANIFEST_VERSION_AMBIGUOUS")
    return int(matches[0])


def future_manifest(raw):
    manifest_version(raw)
    changed, count = re.subn(rb"^version: [0-9]+$", b"version: 999", raw, flags=re.MULTILINE)
    require(count == 1 and changed != raw, "FUTURE_MANIFEST_NOT_CHANGED")
    return changed


def migration_identity(raw, original):
    migrated = scalar_snapshot(raw, "verified")
    require(migrated["verified"], "MIGRATION_NOT_VERIFIED")
    for key in ("generation", "schema_hash", "rows", "bytes", "refreshed_at"):
        require(migrated[key] == original[key], "MIGRATION_CHANGED_" + key.upper())
    require(migrated["fingerprint"] != original["fingerprint"], "MIGRATION_DID_NOT_DERIVE_LOCAL_POLICY")
    for key, expected in (("source_fingerprint", original["fingerprint"]),
                          ("source_generation_sha256", original["sha256"])):
        matches = re.findall(rb"^" + key.encode() + rb": ([0-9a-f]{64})$", raw, re.MULTILINE)
        require(len(matches) == 1 and matches[0].decode() == expected, "MIGRATION_PROVENANCE_MISMATCH")
    return migrated


def write_yaml(path, document):
    require(not path.is_symlink(), "CONFIG_SYMLINK")
    path.write_text(yaml.safe_dump(document, sort_keys=False))
    path.chmod(0o600)


def base_catalog(source, directory, multipart):
    dataset = {"id": DATASET, "authorization_version": "release-readers-v1", "max_age": "1h",
               "refresh_interval": "0s", "query": {"mode": "federated", "sources": ["raw"], "sql": "SELECT * FROM raw"},
               "limits": {"max_rows": 1000010, "max_bytes": 268435456, "timeout": "1m",
                          "memory_mb": 256, "threads": 1, "max_temp_mb": 256}}
    if multipart:
        dataset["multipart"] = {"max_part_bytes": 2097152, "max_parts": 256}
    return {"sources": [{"id": "raw", "type": "parquet", "path": str(source)}],
            "acceleration": {"directory": str(directory), "tenant_id": TENANT, "datasets": [dataset]}}


def assert_reader_events(events):
    require(bool(events) and all(e["identity"] == "reader" and e["method"] in ("GET", "HEAD") for e in events),
            "READ_COMPATIBILITY_USED_WRITER_OR_MUTATION")


def validate_case_set(checks):
    expected = {("local", layout) for layout in ("single", "multipart", "single_empty", "multipart_empty")}
    expected.update((provider, layout) for provider in PROVIDERS
                    for layout in ("single", "multipart", "single_empty", "multipart_empty"))
    actual = [(item.get("provider"), item.get("layout")) for item in checks]
    require(len(actual) == len(expected) and set(actual) == expected, "INCOMPLETE_OR_DUPLICATE_RELEASE_MATRIX")
    require(all(item.get("passed") is True for item in checks), "FAILED_RELEASE_MATRIX_CASE")


class UpgradeRun(ObjectMigrationAcceptance):
    def __init__(self, previous, candidate, launcher, directory):
        # Reuse only the existing signed protocol fixture and process-local CA
        # setup. All executable commands, report handling and cleanup are ours.
        self.previous, self.candidate, self.launcher = previous, candidate, launcher
        self.binary = candidate
        self.directory = directory
        self.env = {key: value for key, value in os.environ.items() if not key.startswith("KELVO_SOURCE_")}
        self.env["GOMAXPROCS"] = "2"
        self.checks, self.servers, self.states = [], [], []
        self.secret_values, self.writer_names = [], set()
        self.ca_destination = None
        self.command_number = 0
        self.stage = "initialization"
        self.forced_kills = 0
        self.children_cleaned = True
        self.last_command = None
        self.last_stderr = b""

    def cli(self, args, success=True, timeout=90):
        self.command_number += 1
        if args and (args[0] == "query" or args[:2] == ["accelerate", "refresh"]):
            args = [*args, "--sandbox", self.launcher]
        environment = dict(self.env)
        if args[:2] not in (["accelerate", "refresh"], ["accelerate", "restore"]):
            for name in self.writer_names:
                environment.pop(name, None)
        scratch = self.directory / ("command-" + str(self.command_number))
        scratch.mkdir(mode=0o700)
        environment["TMPDIR"] = str(scratch)
        with (scratch / "stdout").open("wb+") as stdout, (scratch / "stderr").open("wb+") as stderr:
            process = subprocess.Popen([str(self.binary), *map(str, args)], env=environment,
                                       stdout=stdout, stderr=stderr, start_new_session=True)
            deadline = time.monotonic() + timeout
            try:
                while process.poll() is None:
                    require(time.monotonic() < deadline, "CLI_DEADLINE_EXCEEDED")
                    require(os.fstat(stdout.fileno()).st_size <= MAX_OUTPUT and os.fstat(stderr.fileno()).st_size <= MAX_OUTPUT,
                            "CLI_OUTPUT_EXCEEDED")
                    time.sleep(0.025)
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self.forced_kills += 1
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                # An exited parent must not strand a query worker in its group.
                try:
                    os.killpg(process.pid, 0)
                except ProcessLookupError:
                    pass
                else:
                    self.children_cleaned = False
                    os.killpg(process.pid, signal.SIGKILL)
                    raise AcceptanceError("CLI_LEFT_RUNNING_DESCENDANT")
            require(os.fstat(stdout.fileno()).st_size <= MAX_OUTPUT and os.fstat(stderr.fileno()).st_size <= MAX_OUTPUT,
                    "CLI_OUTPUT_EXCEEDED")
            stdout.seek(0)
            stderr.seek(0)
            raw, errors = stdout.read(), stderr.read()
        self.last_command = {"role": "previous" if self.binary == self.previous else "candidate",
                             "command": str(args[0]), "returncode": process.returncode}
        if args[0] == "accelerate":
            self.last_command["subcommand"] = str(args[1])
        try:
            error_code = json.loads(errors).get("code")
            if isinstance(error_code, str) and re.fullmatch("[A-Z][A-Z0-9_]{0,63}", error_code):
                self.last_command["public_error_code"] = error_code
        except (ValueError, AttributeError, UnicodeError):
            pass
        require(not any(value.encode() in raw + errors for value in self.secret_values), "CLI_EXPOSED_SYNTHETIC_CREDENTIAL")
        self.last_stderr = self.redact(errors)
        require((process.returncode == 0) == success, "UNEXPECTED_CLI_STATUS")
        if not success:
            require(process.returncode > 0, "CLI_CRASH_IS_NOT_EXPECTED_REJECTION")
            require(b"ready: true" not in raw and b"verified: true" not in raw, "FAILED_CLI_CLAIMED_SUCCESS")
        require(not list(scratch.rglob("*.parquet")), "CLI_LEFT_STAGED_PARQUET")
        shutil.rmtree(scratch)
        return raw, errors

    def as_role(self, role, args, success=True):
        self.binary = self.previous if role == "previous" else self.candidate
        return self.cli(args, success=success)

    def snapshot(self, role, config, command="verify", success=True):
        raw, _ = self.as_role(role, ["accelerate", command, "--config", config, "--dataset", DATASET], success)
        return scalar_snapshot(raw) if success else None

    def query_exact(self, role, config, expected=None, *, success=True):
        output = self.directory / ("query-" + str(self.command_number) + ".arrow")
        self.as_role(role, ["query", "--config", config, "--sources", DATASET,
                           "--sql", "SELECT * FROM saved ORDER BY row_id", "--out", output,
                           "--max-rows", "1000010", "--max-bytes", "268435456", "--memory-mb", "256",
                           "--threads", "1", "--timeout", "1m"], success)
        if success:
            verify_arrow(output, expected)
            output.unlink()
        else:
            require(not output.exists(), "REJECTED_QUERY_PUBLISHED_RESULT")

    def backup_local(self, role, config, destination, success=True):
        raw, _ = self.as_role(role, ["accelerate", "backup", "--config", config, "--dataset", DATASET,
                                   "--destination", destination], success)
        if success:
            snapshot = scalar_snapshot(raw, "verified")
            require(snapshot["verified"], "BACKUP_NOT_VERIFIED")
            return snapshot

    def migrated(self, config, document, destination, original, expected):
        local = copy.deepcopy(document)
        local["acceleration"].pop("object_storage")
        local["acceleration"]["directory"] = str(destination)
        local_config = destination.with_suffix(".yml")
        write_yaml(local_config, local)
        raw, _ = self.as_role("candidate", ["accelerate", "migrate-backup", "--config", config,
                                            "--destination-config", local_config, "--dataset", DATASET])
        snapshot = migration_identity(raw, original)
        for role in ("previous", "candidate"):
            require_identity(self.snapshot(role, local_config), snapshot)
            self.query_exact(role, local_config, expected)
        return local_config, snapshot

    def run_local(self, rows, multipart):
        layout = ("multipart" if multipart else "single") + ("_empty" if rows == 0 else "")
        self.stage = "local/" + layout
        work = self.directory / ("local-" + layout)
        work.mkdir(mode=0o700)
        source, config = work / "source.parquet", work / "catalog.yml"
        origin, backup, rollback = work / "live", work / "before-upgrade", work / "rollback"
        expected = source_table(rows)
        pq.write_table(expected, source, row_group_size=4096)
        source.chmod(0o600)
        document = base_catalog(source, origin, multipart)
        write_yaml(config, document)
        initial = self.snapshot("previous", config, "refresh")
        require(initial["ready"] and initial["rows"] == rows, "PREVIOUS_REFRESH_INVALID")
        manifest = origin / TENANT / DATASET / "current.yaml"
        format_version = manifest_version(manifest.read_bytes())
        require(format_version == (2 if multipart else 1), "UNEXPECTED_LOCAL_WRITER_FORMAT")
        parts = len(list((origin / TENANT / DATASET).glob("*.parquet")))
        require(not multipart or not rows or parts > 1, "MULTIPART_FIXTURE_TOO_SMALL")
        saved_files = fingerprint_files(origin)
        for role in ("previous", "candidate"):
            require_identity(self.snapshot(role, config), initial)
            self.query_exact(role, config, expected)
        require(fingerprint_files(origin) == saved_files, "READ_ONLY_UPGRADE_REWROTE_STORE")
        require_identity(self.backup_local("previous", config, backup), initial)
        backup_files = fingerprint_files(backup)
        source.unlink()
        self.snapshot("candidate", config, "refresh", success=False)
        require_identity(self.snapshot("previous", config), initial)
        require(fingerprint_files(origin) == saved_files, "FAILED_REFRESH_CHANGED_COMMITTED_STORE")
        newer = source_table(rows + 7)
        pq.write_table(newer, source, row_group_size=4096)
        source.chmod(0o600)
        latest = self.snapshot("candidate", config, "refresh")
        require(latest["ready"] and latest["rows"] == rows + 7 and latest["generation"] != initial["generation"],
                "CANDIDATE_DID_NOT_PUBLISH_NEW_GENERATION")
        require(latest["fingerprint"] == initial["fingerprint"], "SAME_CATALOG_FINGERPRINT_CHANGED")
        for role in ("previous", "candidate"):
            require_identity(self.snapshot(role, config), latest)
            self.query_exact(role, config, newer)
        current_raw = manifest.read_bytes()
        require(manifest_version(current_raw) == format_version, "CANDIDATE_CHANGED_LOCAL_FORMAT")
        manifest.chmod(0o600)
        manifest.write_bytes(future_manifest(current_raw))
        manifest.chmod(0o400)
        invalid_files = fingerprint_files(origin)
        try:
            for role in ("previous", "candidate"):
                self.snapshot(role, config, success=False)
                self.query_exact(role, config, success=False)
                self.snapshot(role, config, "refresh", success=False)
            require(fingerprint_files(origin) == invalid_files, "UNKNOWN_FORMAT_WAS_OVERWRITTEN")
        finally:
            manifest.chmod(0o600)
            manifest.write_bytes(current_raw)
            manifest.chmod(0o400)
        source.unlink()
        # Roll back from a preserved backup into a new root, never overwrite the
        # live store. The seven rows introduced after the backup are absent.
        recovered_document = copy.deepcopy(document)
        recovered_document["acceleration"]["directory"] = str(backup)
        recovered_config = work / "rollback.yml"
        write_yaml(recovered_config, recovered_document)
        require_identity(self.backup_local("previous", recovered_config, rollback), initial)
        recovered_document["acceleration"]["directory"] = str(rollback)
        write_yaml(recovered_config, recovered_document)
        for role in ("previous", "candidate"):
            require_identity(self.snapshot(role, recovered_config), initial)
            self.query_exact(role, recovered_config, expected)
        self.backup_local("candidate", config, backup, success=False)
        require(fingerprint_files(backup) == backup_files, "PRE_UPGRADE_BACKUP_CHANGED")
        recovered_document["acceleration"]["datasets"][0]["max_age"] = "1ns"
        write_yaml(recovered_config, recovered_document)
        for role in ("previous", "candidate"):
            stale = self.snapshot(role, recovered_config, "status")
            require_identity(stale, initial)
            require(not stale["ready"], "ROLLBACK_RESET_OR_IGNORED_AGE")
            self.query_exact(role, recovered_config, success=False)
        recovered_document["acceleration"]["datasets"][0]["authorization_version"] = "revoked-readers-v2"
        recovered_document["acceleration"]["datasets"][0]["max_age"] = "1h"
        write_yaml(recovered_config, recovered_document)
        for role in ("previous", "candidate"):
            self.query_exact(role, recovered_config, success=False)
        self.checks.append({"provider": "local", "layout": layout, "passed": True,
                            "previous_snapshot": identity(initial), "candidate_snapshot": identity(latest),
                            "rows_before_upgrade": rows, "rows_after_upgrade": rows + 7,
                            "manifest_version": format_version, "parts_before_upgrade": parts,
                            "old_writer_new_reader": True, "new_writer_old_reader": True,
                            "readers_preserve_store_bytes": True, "failed_refresh_preserves_snapshot": True,
                            "unknown_format_rejected_without_overwrite": True,
                            "backup_rollback_preserves_identity_and_age": True,
                            "exact_values_types_nulls": True, "rollback_enforces_age_and_authorization": True})
        print(self.stage + ": passed", flush=True)

    def run_remote(self, provider, rows, multipart):
        layout = ("multipart" if multipart else "single") + ("_empty" if rows == 0 else "")
        self.stage = provider + "/" + layout
        self.provider = provider
        state, endpoint = self.fixture(provider)
        work = self.directory / (provider + "-" + layout)
        work.mkdir(mode=0o700)
        source, config = work / "source.parquet", work / "remote.yml"
        expected = source_table(rows)
        pq.write_table(expected, source, row_group_size=4096)
        source.chmod(0o600)
        document = base_catalog(source, work / "staging", multipart)
        document["acceleration"]["tenant_id"] = REMOTE_TENANT
        document["acceleration"]["object_storage"] = self.storage_config(state, endpoint)
        write_yaml(config, document)
        initial = self.snapshot("previous", config, "refresh")
        require(initial["ready"] and initial["rows"] == rows, "PREVIOUS_REMOTE_REFRESH_INVALID")
        key = "/snapshots/private/" + REMOTE_TENANT + "/" + DATASET + "/current.yaml"
        original_entry = copy.deepcopy(state.objects[key])
        require(manifest_version(original_entry["body"]) == 4, "PREVIEW_REMOTE_WRITER_NOT_V4")
        parts = len([k for k in state.objects if k.endswith(".parquet")])
        require(not multipart or not rows or parts > 1, "REMOTE_MULTIPART_FIXTURE_TOO_SMALL")
        before = state.event_count()
        for role in ("previous", "candidate"):
            require_identity(self.snapshot(role, config), initial)
        assert_reader_events(state.events_since(before))
        require(state.objects[key] == original_entry, "REMOTE_READ_REWROTE_MANIFEST")
        source.unlink()
        # Previous release does not offer migrate-backup. An unavailable command
        # must fail before contacting storage or publishing a destination.
        unsupported = copy.deepcopy(document)
        unsupported["acceleration"].pop("object_storage")
        unsupported["acceleration"]["directory"] = str(work / "unsupported-target")
        unsupported_config = work / "unsupported.yml"
        write_yaml(unsupported_config, unsupported)
        before = state.event_count()
        _, errors = self.as_role("previous", ["accelerate", "migrate-backup", "--config", config,
                                               "--destination-config", unsupported_config, "--dataset", DATASET], False)
        require(b"flag provided but not defined: -destination-config" in errors, "PREVIOUS_MIGRATION_REJECTION_CHANGED")
        require(not Path(unsupported["acceleration"]["directory"]).exists() and state.event_count() == before,
                "UNSUPPORTED_MIGRATION_ACCESSED_STORAGE")
        before = state.event_count()
        rollback_config, rollback_snapshot = self.migrated(config, document, work / "pre-upgrade-local", initial, expected)
        assert_reader_events(state.events_since(before))
        rollback_files = fingerprint_files(work / "pre-upgrade-local")
        newer = source_table(rows + 7)
        pq.write_table(newer, source, row_group_size=4096)
        source.chmod(0o600)
        latest = self.snapshot("candidate", config, "refresh")
        require(latest["ready"] and latest["rows"] == rows + 7 and latest["generation"] != initial["generation"],
                "CANDIDATE_REMOTE_DID_NOT_ADVANCE")
        require(latest["fingerprint"] == initial["fingerprint"], "REMOTE_FINGERPRINT_CHANGED")
        latest_entry = copy.deepcopy(state.objects[key])
        require(manifest_version(latest_entry["body"]) == 4, "CANDIDATE_REMOTE_FORMAT_CHANGED")
        source.unlink()
        before = state.event_count()
        for role in ("previous", "candidate"):
            require_identity(self.snapshot(role, config), latest)
        assert_reader_events(state.events_since(before))
        self.snapshot("candidate", config, "refresh", success=False)
        for role in ("previous", "candidate"):
            require_identity(self.snapshot(role, config), latest)
        # Restore a valid source so a failed refresh cannot be mistaken for
        # unknown-format rejection merely because the source was unavailable.
        pq.write_table(newer, source, row_group_size=4096)
        source.chmod(0o600)
        unknown = copy.deepcopy(state.objects[key])
        unknown["body"] = future_manifest(unknown["body"])
        unknown["digest"] = hashlib.sha256(unknown["body"]).hexdigest()
        valid = state.objects[key]
        with state.lock:
            state.objects[key] = unknown
        try:
            before = state.event_count()
            for role in ("previous", "candidate"):
                self.snapshot(role, config, success=False)
                self.snapshot(role, config, "refresh", success=False)
            require(all(event["method"] in ("GET", "HEAD") for event in state.events_since(before)),
                    "UNKNOWN_REMOTE_FORMAT_TRIGGERED_WRITE")
            require(state.objects[key] == unknown, "UNKNOWN_REMOTE_FORMAT_OVERWRITTEN")
        finally:
            with state.lock:
                state.objects[key] = valid
        source.unlink()
        before = state.event_count()
        self.migrated(config, document, work / "after-upgrade-local", latest, newer)
        assert_reader_events(state.events_since(before))
        for role in ("previous", "candidate"):
            require_identity(self.snapshot(role, rollback_config), rollback_snapshot)
            self.query_exact(role, rollback_config, expected)
        require(fingerprint_files(work / "pre-upgrade-local") == rollback_files, "REMOTE_UPGRADE_CHANGED_ROLLBACK_COPY")
        require(not state.auth_failures, "SIGNED_FIXTURE_AUTHENTICATION_FAILURE")
        require(self.redirect_sink.foreign_requests == 0, "FIXTURE_REDIRECT_CONTACTED_FOREIGN_ORIGIN")
        self.checks.append({"provider": provider, "layout": layout, "passed": True,
                            "previous_snapshot": identity(initial), "candidate_snapshot": identity(latest),
                            "rows_before_upgrade": rows, "rows_after_upgrade": rows + 7,
                            "manifest_version": 4, "parts_before_upgrade": parts,
                            "old_writer_new_verifier": True, "new_writer_old_verifier": True,
                            "readers_preserve_manifest_bytes": True, "reader_credentials_only": True,
                            "failed_refresh_preserves_snapshot": True,
                            "unknown_format_rejected_without_overwrite": True,
                            "previous_rejects_migration_without_storage_access": True,
                            "explicit_migration_preserves_values_types_nulls_identity_age": True,
                            "previous_reads_candidate_migrated_local_snapshots": True,
                            "independent_local_rollback_copy_preserved": True,
                            "remote_direct_query_tested": False})
        print(self.stage + ": passed", flush=True)

    def cleanup(self):
        errors = []
        for server, thread in reversed(self.servers):
            try:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)
                require(not thread.is_alive() and not server.fixture_errors, "TLS_FIXTURE_CLEANUP_FAILED")
            except Exception as error:
                errors.append(type(error).__name__)
        require(not errors and self.children_cleaned and self.forced_kills == 0, "OWNED_PROCESS_CLEANUP_FAILED")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous-binary", required=True, type=Path)
    parser.add_argument("--candidate-binary", required=True, type=Path)
    parser.add_argument("--candidate-revision", required=True)
    parser.add_argument("--launcher", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=100000)
    args = parser.parse_args()
    require(platform.system() == "Linux" and platform.machine() in ("x86_64", "amd64"), "LINUX_AMD64_REQUIRED")
    require(100000 <= args.rows <= 1000000, "ROWS_OUT_OF_BOUNDS")
    require(re.fullmatch("[0-9a-f]{40}", args.candidate_revision), "CANDIDATE_REVISION_REQUIRED")
    require(not os.path.lexists(args.output) and args.output.parent.is_dir(), "NEW_REPORT_REQUIRED")
    previous, candidate, launcher = [path.resolve(strict=True) for path in (args.previous_binary, args.candidate_binary, args.launcher)]
    require(all(p.is_file() and os.access(p, os.X_OK) for p in (previous, candidate, launcher)), "EXECUTABLES_REQUIRED")
    artifacts = {"previous_binary_sha256": digest(previous), "candidate_binary_sha256": digest(candidate),
                 "launcher_sha256": digest(launcher),
                 "scripts": {name: digest(Path(__file__).with_name(name)) for name in SCRIPTS}}
    require(artifacts["previous_binary_sha256"] == PREVIOUS_BINARY_SHA256, "PREVIOUS_BINARY_NOT_IMMUTABLE_PREVIEW")
    require(artifacts["candidate_binary_sha256"] != PREVIOUS_BINARY_SHA256, "CANDIDATE_EQUALS_PREVIOUS")
    report = {"schema_version": 1, "passed": False, "checked_at": datetime.now(timezone.utc).isoformat(),
              "previous_tag": PREVIOUS_TAG, "previous_revision": PREVIOUS_REVISION,
              "candidate_revision": args.candidate_revision, "candidate_revision_provenance": "operator-supplied; binary hash authoritative",
              "artifacts": artifacts, "kernel_release": platform.release(), "machine": platform.machine(),
              "pyarrow_version": pa.__version__, "checks": [],
              "scope": "Linux amd64 local single/multipart snapshot upgrade and preserved-backup rollback; signed loopback S3/R2/GCS/Azure manifest verification and remote-to-local migration. Both binaries already use remote v4. No pre-v4 binary rollback, direct remote DuckDB query, real cloud, cluster rolling upgrade, throughput or production-readiness claim."}
    os.umask(0o077)
    temporary = tempfile.TemporaryDirectory(prefix="kelvo-release-upgrades-")
    run = UpgradeRun(previous, candidate, launcher, Path(temporary.name))
    original_handlers = {}
    def interrupted(signum, frame):
        raise InterruptedError("release gate interrupted")
    for signum in (signal.SIGINT, signal.SIGTERM):
        original_handlers[signum] = signal.signal(signum, interrupted)
    try:
        version, _ = run.as_role("previous", ["version"])
        require(version.strip() == ("Kelvo Go " + PREVIOUS_TAG).encode(), "PREVIOUS_VERSION_MISMATCH")
        for rows in (args.rows, 0):
            for multipart in (False, True):
                run.run_local(rows, multipart)
        run.setup_certificates()
        for provider in PROVIDERS:
            for rows in (REMOTE_ROWS, 0):
                for multipart in (False, True):
                    run.run_remote(provider, rows, multipart)
        validate_case_set(run.checks)
        require(artifacts == {"previous_binary_sha256": digest(previous), "candidate_binary_sha256": digest(candidate),
                              "launcher_sha256": digest(launcher),
                              "scripts": {name: digest(Path(__file__).with_name(name)) for name in SCRIPTS}},
                "ARTIFACT_CHANGED_DURING_RUN")
        report["passed"] = True
    except BaseException as error:
        report["failure"] = {"stage": run.stage,
                             "code": str(error) if isinstance(error, AcceptanceError) else type(error).__name__}
        if run.last_command is not None:
            report["failure"]["last_command"] = run.last_command
    finally:
        for signum in original_handlers:
            signal.signal(signum, signal.SIG_IGN)
        report["checks"] = run.checks
        report["commands"] = run.command_number
        report["forced_kills"] = run.forced_kills
        try:
            try:
                run.cleanup()
            finally:
                temporary.cleanup()
            report["private_fixture_cleanup"] = not Path(temporary.name).exists()
            require(report["private_fixture_cleanup"], "PRIVATE_FIXTURE_REMAINS")
        except BaseException as error:
            report["passed"] = False
            report["cleanup_failure"] = type(error).__name__
            report["private_fixture_cleanup"] = False
        for signum, handler in original_handlers.items():
            signal.signal(signum, handler)
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps({"passed": report["passed"], "cases": len(report["checks"]), "failure": report.get("failure")}, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

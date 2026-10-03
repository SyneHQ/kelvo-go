#!/usr/bin/env python3
"""Verified local snapshot recovery on a dedicated Linux VM; no source service or build."""
import argparse
from datetime import datetime, timezone
from decimal import Decimal
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

EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"


class AcceptanceError(Exception):
    pass


def require(value, message):
    if not value:
        raise AcceptanceError(message)


def digest(path):
    hashed = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(256 << 10), b""):
            hashed.update(chunk)
    return hashed.hexdigest()


def scalar_snapshot(raw, header="ready"):
    require(len(raw) <= 1 << 20 and header in ("ready", "verified"), "INVALID_CLI_SNAPSHOT")
    text = raw.decode()
    markers = list(re.finditer(r"^" + header + r": (true|false)$", text, re.M))
    require(len(markers) == 1, "CLI_STATUS_MARKER_MISSING")
    marker = markers[0]
    indent = re.search(r"^snapshot:\s*\n( +)\S", text, re.M)
    require(indent is not None, "CLI_SNAPSHOT_MISSING")
    result = {header: marker.group(1) == "true"}
    for key in ("generation", "fingerprint", "schema_hash", "sha256", "rows", "bytes", "refreshed_at"):
        items = list(re.finditer(r"^" + indent.group(1) + key + r": ([^\n]+)$", text, re.M))
        require(len(items) == 1, "CLI_SNAPSHOT_FIELD_MISSING")
        value = items[0].group(1)
        if value.startswith('"'):
            value = json.loads(value)
        result[key] = int(value) if key in ("rows", "bytes") else value
    require(re.fullmatch(r"[0-9a-f]{32}", result["generation"]), "INVALID_GENERATION")
    for key in ("fingerprint", "schema_hash", "sha256"):
        require(re.fullmatch(r"[0-9a-f]{64}", result[key]), "INVALID_SNAPSHOT_HASH")
    return result


def identity(snapshot):
    return {k: v for k, v in snapshot.items() if k not in ("ready", "verified")}


def source_table(rows):
    schema = pa.schema([("row_id", pa.int64()), ("unsigned", pa.uint64()),
                        ("amount", pa.decimal128(18, 4)), ("observed_at", pa.timestamp("us")),
                        ("label", pa.string())])
    return pa.Table.from_arrays([
        pa.array(range(rows), type=pa.int64()),
        pa.array([None if i % 11 == 0 else (1 << 64)-i for i in range(rows)], type=pa.uint64()),
        pa.array([None if i % 7 == 0 else Decimal(i).scaleb(-4) for i in range(rows)], type=pa.decimal128(18, 4)),
        pa.array([None if i % 13 == 0 else i*1000003 for i in range(rows)], type=pa.timestamp("us")),
        pa.array([None if i % 5 == 0 else hashlib.sha512(str(i).encode()).hexdigest() for i in range(rows)], type=pa.string()),
    ], schema=schema)


def verify_arrow(path, expected):
    require(path.stat().st_size >= len(EOS), "RECOVERY_ARROW_EOS_MISSING")
    try:
        with path.open("rb") as stream:
            stream.seek(-len(EOS), 2)
            require(stream.read() == EOS, "RECOVERY_ARROW_EOS_MISSING")
            stream.seek(0)
            with pa.ipc.open_stream(stream) as reader:
                actual = reader.read_all()
                require(stream.read(1) == b"", "RECOVERY_ARROW_TRAILING_BYTES")
        require(actual.equals(expected, check_metadata=False), "RECOVERY_VALUES_OR_TYPES_CHANGED")
    except (pa.ArrowException, OSError, ValueError) as error:
        raise AcceptanceError("RECOVERY_ARROW_INVALID") from error


def catalog(path, source, store, multipart, *, epoch="readers-v1", max_age="1h"):
    # Actual YAML with fixed keys and JSON-escaped scalar strings; no YAML dependency.
    text = f'''sources:
  - id: raw
    type: parquet
    path: {json.dumps(str(source))}
acceleration:
  directory: {json.dumps(str(store))}
  tenant_id: backup-test
  datasets:
    - id: saved
      authorization_version: {json.dumps(epoch)}
      max_age: {max_age}
      refresh_interval: 0s
      query:
        mode: federated
        sources: [raw]
        sql: SELECT * FROM raw
      limits:
        max_rows: 1000010
        max_bytes: 268435456
        timeout: 1m
        memory_mb: 256
        threads: 1
        max_temp_mb: 256
'''
    if multipart:
        text += "      multipart:\n        max_part_bytes: 2097152\n        max_parts: 256\n"
    path.write_text(text)
    path.chmod(0o600)


class Run:
    def __init__(self, binary, launcher, directory):
        self.binary, self.launcher, self.directory = binary, launcher, directory
        self.stage = "initialization"
        self.checks = []
        self.commands = 0

    def cli(self, args, *, success=True):
        self.commands += 1
        with tempfile.TemporaryDirectory(prefix="command-", dir=self.directory) as temporary:
            root = Path(temporary)
            with (root / "stdout").open("wb+") as out, (root / "stderr").open("wb+") as err:
                process = subprocess.Popen([str(self.binary), *map(str, args)], stdout=out, stderr=err,
                                           start_new_session=True)
                try:
                    process.wait(timeout=90)
                except BaseException:
                    if process.returncode is None:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                    raise
                require((process.returncode == 0) == success, "UNEXPECTED_CLI_STATUS")
                require(out.tell() <= 1<<20 and err.tell() <= 1<<20, "UNEXPECTED_CLI_OUTPUT_SIZE")
                out.seek(0)
                return out.read()

    def status(self, config, command="status"):
        args = ["accelerate", command, "--config", config, "--dataset", "saved"]
        if command == "refresh":
            args += ["--sandbox", self.launcher]
        return scalar_snapshot(self.cli(args))

    def backup(self, config, destination, *, success=True):
        raw = self.cli(["accelerate", "backup", "--config", config, "--dataset", "saved",
                        "--destination", destination], success=success)
        if success:
            result = scalar_snapshot(raw, "verified")
            require(result["verified"], "BACKUP_NOT_VERIFIED")
            return result

    def query(self, config, expected):
        output = self.directory / f"result-{self.commands}.arrow"
        self.cli(["query", "--config", config, "--sources", "saved", "--sql",
                  "SELECT * FROM saved ORDER BY row_id", "--out", output,
                  "--max-rows", "1000010", "--max-bytes", "268435456",
                  "--memory-mb", "256", "--threads", "1", "--timeout", "1m",
                  "--sandbox", self.launcher])
        verify_arrow(output, expected)
        output.unlink()

    def run_layout(self, rows, multipart):
        label = ("multipart" if multipart else "single") + ("_empty" if rows == 0 else "")
        work = self.directory / label
        work.mkdir(mode=0o700)
        source, config = work / "source.parquet", work / "catalog.yml"
        origin, backup, recovered = work / "origin", work / "backup", work / "recovered"
        expected = source_table(rows)
        pq.write_table(expected, source, row_group_size=4096)
        source.chmod(0o600)
        catalog(config, source, origin, multipart)
        self.stage = label + "/initial_refresh"
        initial = self.status(config, "refresh")
        require(initial["rows"] == rows and initial["ready"], "INITIAL_SNAPSHOT_INVALID")
        manifest = origin / "backup-test" / "saved" / "current.yaml"
        original_manifest = manifest.read_bytes()
        self.stage = label + "/backup"
        before = time.monotonic()
        copied = self.backup(config, backup)
        backup_seconds = time.monotonic()-before
        require(identity(copied) == identity(initial), "BACKUP_CHANGED_IDENTITY")
        require((backup / "backup-test" / "saved" / "current.yaml").read_bytes() == original_manifest,
                "BACKUP_REWROTE_MANIFEST")
        payloads = sorted((backup / "backup-test" / "saved").glob("*.parquet"))
        require(bool(payloads), "BACKUP_PAYLOAD_MISSING")
        if multipart and rows:
            require(len(payloads) > 1, "MULTIPART_FIXTURE_TOO_SMALL")
        self.backup(config, backup, success=False)
        require((backup / "backup-test" / "saved" / "current.yaml").read_bytes() == original_manifest,
                "EXISTING_BACKUP_OVERWRITTEN")
        self.stage = label + "/subsequent_refresh_and_loss"
        newer_rows = rows+7
        pq.write_table(source_table(newer_rows), source, row_group_size=4096)
        latest = self.status(config, "refresh")
        require(latest["generation"] != initial["generation"] and latest["rows"] == newer_rows,
                "NEW_GENERATION_NOT_PUBLISHED")
        # Destroy only this private run's generated source/live store. The saved
        # copy must recover after both original input and live generation disappear.
        source.unlink()
        shutil.rmtree(origin)
        catalog(config, source, backup, multipart)
        self.stage = label + "/recovery"
        recovery_start = time.monotonic()
        restored = self.backup(config, recovered)
        copy_seconds = time.monotonic()-recovery_start
        require(identity(restored) == identity(initial), "RECOVERY_CHANGED_ORIGINAL_IDENTITY")
        catalog(config, source, recovered, multipart)
        verified = self.status(config, "verify")
        require(identity(verified) == identity(initial) and verified["ready"], "RECOVERED_SNAPSHOT_NOT_READY")
        self.query(config, expected)
        recovery_seconds = time.monotonic()-recovery_start
        self.stage = label + "/authorization_and_staleness"
        catalog(config, source, recovered, multipart, epoch="readers-v2")
        rejected = work / "unauthorized"
        self.backup(config, rejected, success=False)
        require(not rejected.exists(), "UNAUTHORIZED_BACKUP_PUBLISHED")
        catalog(config, source, recovered, multipart, max_age="1ns")
        # Stale backups remain useful disaster-recovery artifacts, but never
        # acquire a new data timestamp or bypass query freshness policy.
        stale_copy = work / "stale-copy"
        stale = self.backup(config, stale_copy)
        require(identity(stale) == identity(initial), "STALE_BACKUP_RELABELLED_FRESH")
        catalog(config, source, stale_copy, multipart, max_age="1ns")
        require(not self.status(config)["ready"], "STALE_RECOVERY_REPORTED_READY")
        output = work / "stale.arrow"
        self.cli(["query", "--config", config, "--sources", "saved", "--sql", "SELECT * FROM saved",
                  "--out", output, "--sandbox", self.launcher], success=False)
        require(not output.exists(), "STALE_QUERY_PUBLISHED_RESULT")
        self.checks.append({"layout": label, "passed": True, "rows": rows,
            "encoded_bytes": initial["bytes"], "parts": len(payloads),
            "backup_seconds": backup_seconds, "recovery_copy_seconds": copy_seconds,
            "recovery_through_verified_first_query_seconds": recovery_seconds,
            "recovered_refreshed_at": initial["refreshed_at"],
            "lost_live_refreshed_at": latest["refreshed_at"],
            "fixture_rows_not_in_backup": newer_rows-rows,
            "exact_typed_values_and_nulls": True, "source_and_live_store_absent_during_recovery": True,
            "authorization_and_staleness_enforced": True, "existing_destination_preserved": True})
        print(label + ": passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--launcher", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=100000)
    args = parser.parse_args()
    require(platform.system() == "Linux", "LINUX_REQUIRED")
    require(100000 <= args.rows <= 1000000, "ROWS_OUT_OF_BOUNDS")
    require(not os.path.lexists(args.output) and args.output.parent.is_dir(), "NEW_REPORT_REQUIRED")
    binary, launcher = args.binary.resolve(strict=True), args.launcher.resolve(strict=True)
    require(all(p.is_file() and os.access(p, os.X_OK) for p in (binary, launcher)), "EXECUTABLES_REQUIRED")
    hashes = {"binary_sha256": digest(binary), "launcher_sha256": digest(launcher), "runner_sha256": digest(Path(__file__))}
    report = {"passed": False, "checked_at": datetime.now(timezone.utc).isoformat(),
              "artifacts": hashes, "kernel_release": platform.release(), "pyarrow_version": pa.__version__,
              "checks": [], "scope": "Local synthetic snapshot copy/recovery with actual sandboxed query. Timings include process startup, verification and exact result comparison; warm OS cache, no service failover or cold-cache control. Row loss is a fixture example, not source transaction RPO."}
    temporary = tempfile.TemporaryDirectory(prefix="kelvo-backup-acceptance-")
    run = Run(binary, launcher, Path(temporary.name))
    try:
        for rows in (args.rows, 0):
            for multipart in (False, True):
                run.run_layout(rows, multipart)
        require(hashes == {"binary_sha256": digest(binary), "launcher_sha256": digest(launcher),
                           "runner_sha256": digest(Path(__file__))}, "ARTIFACT_CHANGED_DURING_RUN")
        report["checks"] = run.checks
        report["passed"] = len(run.checks) == 4
    except Exception as error:
        report["checks"] = run.checks
        report["failure"] = {"stage": run.stage, "code": str(error) if isinstance(error, AcceptanceError) else type(error).__name__}
    finally:
        try:
            temporary.cleanup()
            report["private_temp_cleanup"] = True
        except Exception:
            report["passed"] = False
            report["private_temp_cleanup"] = False
    descriptor = os.open(args.output, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps({"passed": report["passed"], "cases": len(report["checks"]), "failure": report.get("failure")}, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

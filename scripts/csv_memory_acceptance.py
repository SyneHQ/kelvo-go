#!/usr/bin/env python3
"""Linux-only, sandboxed CSV buffer acceptance; a tiny fixture, not throughput evidence."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import tempfile
import threading
import time

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as parquet

SQL = "SELECT length(list(i))::BIGINT AS total FROM events,LATERAL range(events.n) input(i)"
CSV = b"n\n3\n"
MAX_BYTES = 1 << 20
WORKER_TIMEOUT_SECONDS = 20
PROCESS_TIMEOUT_SECONDS = 25
SAMPLE_SECONDS = 0.01
PROFILES = {
    "csv_default": {},
    "csv_1mib_256kib": {"buffer_size": "1048576", "maximum_line_size": "262144"},
    "csv_8000000_2000000": {"buffer_size": "8000000", "maximum_line_size": "2000000"},
    "parquet_control": {},
}
PUBLIC_CODES = frozenset({
    "RESOURCE_EXHAUSTED", "QUERY_FAILED", "INVALID_ARGUMENT", "CONFIGURATION_ERROR",
    "PERMISSION_DENIED", "UNAUTHENTICATED", "UNAVAILABLE", "UNSUPPORTED",
    "NOT_SUPPORTED", "UNIMPLEMENTED", "INTERNAL", "CANCELLED", "DEADLINE_EXCEEDED",
})


class FixtureError(Exception):
    """Only static public codes enter reports; never include process diagnostics."""


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sampled_rss_bytes(pid):
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except (FileNotFoundError, ProcessLookupError):
        return 0
    # A just-exited zombie can still have status but no VmRSS field.
    return 0


def decode_result(data):
    if len(data) > MAX_BYTES or not data.endswith(b"\xff\xff\xff\xff\x00\x00\x00\x00"):
        raise FixtureError("INVALID_ARROW_RESULT")
    source = io.BytesIO(data)
    try:
        with ipc.open_stream(source) as reader:
            table = reader.read_all()
    except Exception:
        raise FixtureError("INVALID_ARROW_RESULT") from None
    expected = pa.schema([pa.field("total", pa.int64(), nullable=True)])
    if source.tell() != len(data) or not table.schema.equals(expected, check_metadata=False):
        raise FixtureError("INVALID_ARROW_SCHEMA")
    if table.to_pylist() != [{"total": 3}]:
        raise FixtureError("INCORRECT_ARROW_VALUE")
    return {"schema": [{"name": "total", "type": "int64", "nullable": True}],
            "rows": [{"total": 3}], "row_count": 1, "arrow_eos": True,
            "wire_bytes": len(data), "result_sha256": hashlib.sha256(data).hexdigest()}


def decode_outcome(data):
    try:
        value = json.loads(data)
    except (UnicodeDecodeError, ValueError):
        raise FixtureError("INVALID_WORKER_OUTCOME") from None
    if not isinstance(value, dict) or not isinstance(value.get("stats"), dict):
        raise FixtureError("INVALID_WORKER_OUTCOME")
    error = value.get("error")
    if error is None:
        return "OK"
    if not isinstance(error, dict) or error.get("code") not in PUBLIC_CODES:
        raise FixtureError("INVALID_WORKER_OUTCOME")
    return error["code"]


def worker_input(source, profile, memory_mb):
    limits = {"max_rows": 10, "max_bytes": MAX_BYTES,
              "timeout": WORKER_TIMEOUT_SECONDS * 1_000_000_000,
              "memory_mb": memory_mb, "threads": 1, "max_temp_mb": 128}
    config_source = {"id": "events", "type": "parquet" if profile == "parquet_control" else "csv",
                     "path": str(source)}
    if PROFILES[profile]:
        config_source["options"] = PROFILES[profile]
    return json.dumps({"config": {"sources": [config_source]}, "limits": limits,
                       "request": {"mode": "federated", "sources": ["events"], "sql": SQL}},
                      separators=(",", ":")).encode()


def kill_owned_process(process):
    # The parent has not reaped this process before a timeout/monitor failure;
    # its process-group identity cannot yet be reused by unrelated work.
    if process.returncode is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def run_case(binary, launcher, source, directory, profile, memory_mb):
    directory.mkdir(mode=0o700)
    result = {"profile": profile, "duckdb_memory_limit_mb": memory_mb,
              "threads": 1, "options": PROFILES[profile], "source_rows": 1,
              "worker_timeout_seconds": WORKER_TIMEOUT_SECONDS,
              "process_timeout_seconds": PROCESS_TIMEOUT_SECONDS,
              "max_result_bytes": MAX_BYTES, "sampled_peak_worker_rss_bytes": 0}
    command = [str(launcher), "--read", str(source), "--write", str(directory),
               "--", str(binary), "worker"]
    env = {"HOME": str(directory), "TMPDIR": str(directory), "GOMAXPROCS": "1"}
    stdout_path, stderr_path = directory / "stdout.private", directory / "stderr.private"
    stop, bad_monitor = threading.Event(), threading.Event()
    monitor_failure = []
    peak_rss, samples = 0, 0
    started = time.monotonic()
    with stdout_path.open("xb") as stdout_file, stderr_path.open("xb") as stderr_file:
        process = subprocess.Popen(command, cwd=directory, env=env, stdin=subprocess.PIPE,
                                   stdout=stdout_file, stderr=stderr_file, start_new_session=True)

        def sample():
            nonlocal peak_rss, samples
            while not stop.is_set():
                try:
                    rss = sampled_rss_bytes(process.pid)
                    if rss:
                        samples += 1
                        peak_rss = max(peak_rss, rss)
                    # Worker output is additionally bounded independently of its
                    # reported limits. Private files never enter public evidence.
                    if stdout_path.stat().st_size > MAX_BYTES or stderr_path.stat().st_size > 64 << 10:
                        raise FixtureError("WORKER_OUTPUT_LIMIT")
                except Exception as exc:
                    monitor_failure.append(str(exc) if isinstance(exc, FixtureError) else "RSS_SAMPLE_UNAVAILABLE")
                    bad_monitor.set()
                    # Popen serializes its PID status check; the monitor must
                    # not signal a process group concurrently with reaping.
                    process.kill()
                    return
                stop.wait(SAMPLE_SECONDS)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        timed_out = False
        try:
            process.communicate(input=worker_input(source, profile, memory_mb), timeout=PROCESS_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            timed_out = True
            kill_owned_process(process)
            process.communicate(timeout=5)
        finally:
            stop.set()
            sampler.join(timeout=2)
            if process.returncode is None:
                kill_owned_process(process)
                process.wait(timeout=5)
            if sampler.is_alive():
                raise FixtureError("RSS_SAMPLER_CLEANUP_FAILED")
    result.update({"elapsed_seconds": time.monotonic() - started,
                   "sampled_peak_worker_rss_bytes": peak_rss,
                   "rss_samples": samples, "exit_code": process.returncode})
    if timed_out:
        result["outcome_code"] = "HARNESS_TIMEOUT"
        return result
    if bad_monitor.is_set():
        result["outcome_code"] = monitor_failure[0]
        return result
    if stdout_path.stat().st_size > MAX_BYTES or stderr_path.stat().st_size > 64 << 10:
        result["outcome_code"] = "WORKER_OUTPUT_LIMIT"
        return result
    try:
        code = decode_outcome(stderr_path.read_bytes())
        result["outcome_code"] = code
        if code == "OK":
            if process.returncode != 0:
                raise FixtureError("SUCCESS_WITH_FAILED_EXIT")
            result.update(decode_result(stdout_path.read_bytes()))
        elif code == "RESOURCE_EXHAUSTED":
            # A resource failure must not advertise a complete valid result.
            try:
                decode_result(stdout_path.read_bytes())
            except FixtureError:
                pass
            else:
                raise FixtureError("FAILED_WORKER_PUBLISHED_RESULT")
    except FixtureError as exc:
        result["outcome_code"] = str(exc)
    return result


def acceptance(cases):
    failures = []
    indexed = {(case["profile"], case["duckdb_memory_limit_mb"]): case for case in cases}
    if len(cases) != 12 or len(indexed) != 12 or any(
            (profile, memory) not in indexed for profile in PROFILES for memory in (16, 32, 64)):
        return False, [], ["INCOMPLETE_MATRIX"]
    for case in cases:
        if case.get("outcome_code") not in {"OK", "RESOURCE_EXHAUSTED"}:
            failures.append("UNEXPECTED_WORKER_FAILURE")
        if case.get("rss_samples", 0) < 1:
            failures.append("RSS_NOT_OBSERVED")
        if case.get("outcome_code") == "OK" and (case.get("rows") != [{"total": 3}] or case.get("row_count") != 1 or not case.get("arrow_eos") or
                case.get("schema") != [{"name": "total", "type": "int64", "nullable": True}] or
                not 0 < case.get("wire_bytes", 0) <= MAX_BYTES or case.get("exit_code") != 0):
            failures.append("INVALID_SUCCESS_EVIDENCE")
    for profile in ("csv_1mib_256kib", "csv_8000000_2000000"):
        if indexed[(profile, 64)].get("outcome_code") != "OK":
            failures.append("OPT_IN_64MB_FAILED")
    for memory in (32, 64):
        if indexed[("parquet_control", memory)].get("outcome_code") != "OK":
            failures.append("PARQUET_CONTROL_FAILED")
    improved = []
    for memory in (16, 32):
        if indexed[("csv_default", memory)].get("outcome_code") != "RESOURCE_EXHAUSTED" or indexed[("parquet_control", memory)].get("outcome_code") != "OK":
            continue
        for profile in ("csv_1mib_256kib", "csv_8000000_2000000"):
            if indexed[(profile, memory)].get("outcome_code") == "OK":
                improved.append({"memory_mb": memory, "profile": profile})
    if not improved:
        failures.append("NO_LOW_MEMORY_IMPROVEMENT")
    return not failures, improved, sorted(set(failures))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--launcher", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path, help="New JSON evidence file; never overwritten")
    args = parser.parse_args()
    if platform.system() != "Linux":
        raise SystemExit("CSV acceptance requires the Linux test VM")
    binary, launcher = args.binary.resolve(strict=True), args.launcher.resolve(strict=True)
    if any(not path.is_file() or not os.access(path, os.X_OK) for path in (binary, launcher)):
        raise SystemExit("Binary and launcher must be executable regular files")
    if os.path.lexists(args.output) or not args.output.parent.is_dir():
        raise SystemExit("Output must be a new file in an existing directory")
    artifacts = {"binary_sha256": sha256(binary), "launcher_sha256": sha256(launcher),
                 "runner_sha256": sha256(Path(__file__))}
    report = {"acceptance": "kelvo-csv-buffer-memory-v1", "fixture_only": True,
              "passed": False, "artifacts": artifacts, "kernel_release": platform.release(),
              "machine": platform.machine(), "pyarrow_version": pa.__version__,
              "query": SQL, "cases": [], "failures": [],
              "sampling": {"interval_ms": 10, "rss_source": "/proc/<worker-pid>/status:VmRSS",
                           "caveat": "Sampled process RSS is distinct from DuckDB memory_limit and can exceed it. Samples may miss brief peaks."},
              "scope": "One local CSV row n=3, exact three-element list aggregate. This is not a throughput benchmark, tenant capacity test, or hard RSS cap."}
    temporary = None
    try:
        temporary = tempfile.TemporaryDirectory(prefix="kelvo-csv-memory-")
        work = Path(temporary.name)
        csv_path, parquet_path = work / "events.csv", work / "events.parquet"
        csv_path.write_bytes(CSV)
        parquet.write_table(pa.table({"n": pa.array([3], type=pa.int64())}), parquet_path)
        for source in (csv_path, parquet_path):
            source.chmod(0o400)
        report["sources"] = {"csv_sha256": sha256(csv_path), "parquet_sha256": sha256(parquet_path),
                             "logical_schema": [{"name": "n", "type": "int64"}], "rows": [{"n": 3}]}
        for memory in (16, 32, 64):
            for profile in PROFILES:
                source = parquet_path if profile == "parquet_control" else csv_path
                report["cases"].append(run_case(binary, launcher, source, work / f"{profile}-{memory}", profile, memory))
        if artifacts != {"binary_sha256": sha256(binary), "launcher_sha256": sha256(launcher), "runner_sha256": sha256(Path(__file__))}:
            raise FixtureError("ARTIFACT_CHANGED_DURING_RUN")
        if report["sources"]["csv_sha256"] != sha256(csv_path) or report["sources"]["parquet_sha256"] != sha256(parquet_path):
            raise FixtureError("SOURCE_CHANGED_DURING_RUN")
        report["passed"], report["low_memory_improvements"], report["failures"] = acceptance(report["cases"])
    except Exception as exc:
        report["passed"] = False
        report["failures"].append(str(exc) if isinstance(exc, FixtureError) else "HARNESS_FAILURE")
    finally:
        if temporary is not None:
            try:
                temporary.cleanup()
                report["private_temp_cleanup"] = True
            except Exception:
                report["private_temp_cleanup"] = False
                report["passed"] = False
                report["failures"].append("PRIVATE_TEMP_CLEANUP_FAILED")
    descriptor = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "w") as output:
        json.dump(report, output, indent=2, sort_keys=True)
        output.write("\n")
    print(json.dumps({"passed": report["passed"], "cases": len(report["cases"]), "failures": report["failures"]}, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

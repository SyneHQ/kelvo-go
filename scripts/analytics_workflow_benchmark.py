#!/usr/bin/env python3
"""Supervise bounded analytical workflow trials on a provisioned Linux VM.

Only the Python standard library, adjacent federation_micro_benchmark.py,
systemd, cgroup v2 and passwordless sudo are required. This script never
provisions, installs, builds, connects to SSH or decodes Arrow. All source
credentials and command lines stay in private inputs and trial directories.
Execution and byte/EOS evidence do not establish canonical result correctness.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import pwd
import re
import secrets
import signal
import stat
import subprocess
import sys
import time

# Even --check is input-only; importing the shared sampler must not create a
# bytecode cache beside deployed source files.
sys.dont_write_bytecode = True

from federation_micro_benchmark import (
    BenchmarkError, DISK_RESERVE, MIB, SAMPLE_SECONDS, STATUS_POLL_SECONDS,
    UNIT_PROPERTIES, Sampler, absolute_path, filesystem_type, free_bytes,
    hash_file, integer, read_counters, read_json, safe_stats, systemctl,
    utc_now, write_json,
)


LABEL = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,95}\Z")
VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
SOURCE_ENV = re.compile(r"(?:KELVO_SOURCE_|ANALYTICS_SOURCE_|CLICKHOUSE_)[A-Z0-9_]{1,100}\Z")
LOG_MAX_BYTES = 4 * MIB
MINIMUM_SCRATCH_HEADROOM = 64 * MIB
FIXED_ENV = {
    "GOMAXPROCS": "2", "POLARS_MAX_THREADS": "2", "OMP_NUM_THREADS": "2",
    "OPENBLAS_NUM_THREADS": "2", "MKL_NUM_THREADS": "2", "NUMEXPR_NUM_THREADS": "2",
    "PYTHONNOUSERSITE": "1", "PYTHONHASHSEED": "0", "PYTHONUNBUFFERED": "1",
    "PYTHONDONTWRITEBYTECODE": "1",
}
RUNTIME_ENV = {"PYTHONPATH", "PYTHONHOME", "VIRTUAL_ENV", "KELVO_SANDBOX_LAUNCHER"}
PLACEHOLDERS = {"{output}", "{scratch}", "{metrics}"}
SIDE_NUMBERS = {
    "rows", "batches", "arrow_buffer_bytes", "arrow_bytes", "wire_bytes",
    "source_wire_bytes", "rows_fetched", "arrow_bytes_fetched", "batches_fetched",
    "threads", "query_memory_mb", "configured_temp_mb", "preload_seconds",
    "memory_budget_profile_mb", "engine_memory_limit_mb",
    "polars_ooc_memory_budget_mb", "polars_ooc_disk_budget_mb",
    "import_seconds", "setup_seconds", "compute_serialize_seconds", "teardown_seconds",
    "query_to_first_arrow_batch_seconds", "in_process_wall_seconds", "process_cpu_seconds",
    "process_peak_rss_kib", "prepare_ns", "duration_ns",
}

# Private invocation data is read inside the service, never from systemd's
# command line. Pipes drain even after log truncation, so noisy diagnostics do
# not fill the shared filesystem. Only stdout Arrow has a fatal pipe byte cap.
WRAPPER = r'''import json, os, pathlib, signal, stat, subprocess, sys, threading
def save(path, value):
    with path.open("x") as output:
        json.dump(value, output, allow_nan=False)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

os.umask(0o077)
status = {"command_returncode": None, "output_fsynced": False, "errors": []}
directory = None
process = None
try:
    specpath = pathlib.Path(sys.argv[1])
    info = specpath.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 2097152:
        raise ValueError()
    spec = json.loads(specpath.read_text())
    directory = pathlib.Path(spec["directory"])
    environment = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": spec["home"],
        "LANG": "C.UTF-8", "TMPDIR": spec["scratch"], "POLARS_TEMP_DIR": spec["scratch"]}
    environment.update(spec["environment"])
    os.chdir(directory)
    targets = [directory / ("result.arrow" if spec["stdout_is_arrow"] else "stdout.log"), directory / "stderr.log"]
    files = [path.open("xb") for path in targets]
    process = subprocess.Popen(spec["argv"], env=environment, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False, start_new_session=True)
    lock = threading.Lock()
    def error(code):
        with lock:
            status["errors"].append(code)
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    def drain(source, target, name, maximum, fatal):
        seen = saved = 0
        try:
            while True:
                block = source.read(65536)
                if not block:
                    break
                seen += len(block)
                permitted = block[:max(0, maximum - saved)]
                if permitted:
                    target.write(permitted)
                    saved += len(permitted)
                if fatal and seen > maximum:
                    error("stdout_arrow_byte_limit_exceeded")
                    fatal = False
            target.flush()
            os.fsync(target.fileno())
        except BaseException:
            error("capture_io_failed")
        finally:
            source.close()
            target.close()
            with lock:
                status[name + "_bytes_seen"] = seen
                status[name + "_bytes_retained"] = saved
    workers = []
    for index, source in enumerate((process.stdout, process.stderr)):
        arrow = index == 0 and spec["stdout_is_arrow"]
        worker = threading.Thread(target=drain, args=(source, files[index],
            "stdout" if index == 0 else "stderr",
            spec["max_output_bytes"] if arrow else spec["log_max_bytes"], arrow), daemon=True)
        worker.start()
        workers.append(worker)
    status["command_returncode"] = process.wait()
    for worker in workers:
        worker.join(timeout=5)
    if any(worker.is_alive() for worker in workers):
        error("capture_descendants_did_not_close")
    if status["command_returncode"] != 0:
        status["errors"].append("command_failed")
    if not status["errors"]:
        output = directory / "result.arrow"
        fd = os.open(output, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or not 0 < info.st_size <= spec["max_output_bytes"]:
                raise ValueError()
            os.fsync(fd)
        finally:
            os.close(fd)
        metadata = directory / "driver-metrics.json"
        if metadata.exists():
            fd = os.open(metadata, os.O_RDONLY | os.O_NOFOLLOW)
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_size > 1048576:
                    raise ValueError()
                os.fsync(fd)
            finally:
                os.close(fd)
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        status["output_fsynced"] = True
except BaseException:
    status["errors"].append("wrapper_io_or_input_failed")
finally:
    if directory is not None:
        try:
            save(directory / "wrapper-status.json", status)
        except BaseException:
            status["errors"].append("wrapper_status_write_failed")
sys.exit(0 if status["output_fsynced"] and not status["errors"] else 125)
'''


def require_label(value, code):
    if not isinstance(value, str) or not LABEL.fullmatch(value):
        raise BenchmarkError(code)
    return value


def hash_mapping(value, code):
    if not isinstance(value, dict) or len(value) > 64:
        raise BenchmarkError(code)
    for key, digest in value.items():
        require_label(key, code)
        if not isinstance(digest, str) or not SHA256.fullmatch(digest):
            raise BenchmarkError(code)
    return dict(value)


def environment_mapping(value):
    if not isinstance(value, dict) or len(value) > 96:
        raise BenchmarkError("invalid_environment_mapping")
    result = {}
    for key, entry in value.items():
        if (not isinstance(key, str) or not (SOURCE_ENV.fullmatch(key) or key in RUNTIME_ENV or key in FIXED_ENV)
                or not isinstance(entry, str) or "\0" in entry or len(entry) > 65536):
            raise BenchmarkError("invalid_environment_entry")
        if key in FIXED_ENV and entry != FIXED_ENV[key]:
            raise BenchmarkError("runtime_environment_conflicts_with_fixed_limits")
        result[key] = entry
    return result


def executable_path(value):
    """Validate the target but preserve venv Python's invocation path."""
    path = Path(value)
    if not path.is_absolute():
        raise BenchmarkError("case_executable_must_be_absolute")
    # Python derives venv context from the invoked path. Resolving argv[0]
    # would silently replace the venv interpreter with its base interpreter.
    absolute_path(str(path.resolve(strict=True)), "case_executable", executable=True)
    return str(path)


def validate_manifest(path):
    manifest = read_json(path, 2 * MIB)
    required = {"schema_version", "profile", "trials", "limits", "cases"}
    if not isinstance(manifest, dict) or set(manifest) != required or type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
        raise BenchmarkError("invalid_manifest_contract")
    require_label(manifest["profile"], "invalid_profile")
    integer(manifest["trials"], 1, 5, "trials")
    supplied = manifest["limits"]
    required_limits = {"memory_max_mib", "threads", "tasks_max", "temp_mib"}
    optional_limits = {"max_output_bytes", "timeout_seconds"}
    if not isinstance(supplied, dict) or not required_limits.issubset(supplied) or set(supplied) - required_limits - optional_limits:
        raise BenchmarkError("invalid_limits_contract")
    limits = {"max_output_bytes": 128 * MIB, "timeout_seconds": 180, **supplied}
    integer(limits["memory_max_mib"], 640, 2048, "memory_max_mib")
    integer(limits["threads"], 2, 2, "threads")
    integer(limits["tasks_max"], 96, 96, "tasks_max")
    if type(limits["temp_mib"]) is not int or limits["temp_mib"] not in (512, 2048):
        raise BenchmarkError("invalid_temp_mib")
    integer(limits["max_output_bytes"], 1024, 256 * MIB, "max_output_bytes")
    integer(limits["timeout_seconds"], 1, 600, "timeout_seconds")
    cases = manifest["cases"]
    if not isinstance(cases, list) or not 1 <= len(cases) <= 64:
        raise BenchmarkError("invalid_case_count")
    required_case = {"id", "panel", "engine", "workflow", "argv"}
    optional_case = {"environment_file", "environment", "reference_file", "expected_rows",
                     "canonical_value_sha256", "input_hashes", "code_hashes", "stdout_is_arrow"}
    seen = set()
    for case in cases:
        if not isinstance(case, dict) or not required_case.issubset(case) or set(case) - required_case - optional_case:
            raise BenchmarkError("invalid_case_contract")
        for key in ("id", "panel", "engine", "workflow"):
            require_label(case[key], "invalid_case_label")
        if case["id"] in seen:
            raise BenchmarkError("repeated_case_id")
        seen.add(case["id"])
        argv = case["argv"]
        if not isinstance(argv, list) or not 1 <= len(argv) <= 128:
            raise BenchmarkError("invalid_argv")
        if any(not isinstance(arg, str) or "\0" in arg or len(arg) > 131072 for arg in argv):
            raise BenchmarkError("invalid_argv")
        if sum(len(arg) for arg in argv) > MIB:
            raise BenchmarkError("argv_exceeds_size_limit")
        for arg in argv:
            if any(token in arg and arg != token for token in PLACEHOLDERS):
                raise BenchmarkError("placeholder_must_be_whole_argument")
        case["argv"] = [executable_path(argv[0]), *argv[1:]]
        case["stdout_is_arrow"] = case.get("stdout_is_arrow", False)
        if type(case["stdout_is_arrow"]) is not bool:
            raise BenchmarkError("invalid_stdout_is_arrow")
        if (case["stdout_is_arrow"] and "{output}" in argv) or (not case["stdout_is_arrow"] and "{output}" not in argv):
            raise BenchmarkError("output_requires_exactly_one_capture_mode")
        values = {}
        if "environment_file" in case:
            environment_file = absolute_path(case["environment_file"], "environment_file", private=True)
            values = environment_mapping(read_json(environment_file))
        inline = environment_mapping(case.get("environment", {}))
        if any(key in values and values[key] != value for key, value in inline.items()):
            raise BenchmarkError("conflicting_environment_entries")
        values.update(inline)
        values.update(FIXED_ENV)
        case["_environment"] = values
        if "reference_file" in case:
            case["reference_file"] = str(absolute_path(case["reference_file"], "reference_file"))
        if "expected_rows" in case:
            integer(case["expected_rows"], 0, 100_000_000, "expected_rows")
        if "canonical_value_sha256" in case and (not isinstance(case["canonical_value_sha256"], str) or not SHA256.fullmatch(case["canonical_value_sha256"])):
            raise BenchmarkError("invalid_canonical_value_sha256")
        for key in ("input_hashes", "code_hashes"):
            case[key] = hash_mapping(case.get(key, {}), "invalid_" + key)
    if manifest["trials"] * len(cases) * (limits["timeout_seconds"] + 45) > 6 * 3600:
        raise BenchmarkError("planned_maximum_runtime_exceeds_six_hours")
    manifest["limits"] = limits
    return manifest


class WorkflowSampler(Sampler):
    """Shared host/cgroup/disk sampler plus all generic service processes.

    coordinator means the Python service wrapper; worker means its command and
    descendants. These labels are roles for measurement, not engine topology.
    """

    def __init__(self, unit, directory):
        super().__init__(unit, directory, "__no_kelvo_argv_classification__")
        self.output_size_peak = 0

    def sample(self):
        super().sample()
        for key in ("pids.events",):
            value = read_counters(self.cgroup / key)
            if value:
                self.counters[key] = value
        try:
            quota, period = (self.cgroup / "cpu.max").read_text().split()
            if quota.isdigit() and period.isdigit():
                self.counters["cpu.max"] = {"quota_usec": int(quota), "period_usec": int(period)}
        except (OSError, ValueError):
            pass
        try:
            info = (self.directory / "result.arrow").lstat()
            if stat.S_ISREG(info.st_mode):
                self.output_size_peak = max(self.output_size_peak, info.st_size)
        except FileNotFoundError:
            pass
        totals = {"coordinator": 0, "worker": 0}
        try:
            pids = (self.cgroup / "cgroup.procs").read_text().split()
        except FileNotFoundError:
            pids = []
        for pid in pids:
            try:
                root = Path("/proc") / pid
                fields = (root / "stat").read_text().rsplit(") ", 1)[1].split()
                identity = (int(pid), int(fields[19]))
                # Keep the first observed role if a descendant is reparented.
                role = "coordinator" if int(fields[1]) == 1 else "worker"
                record = self.processes.setdefault(identity, {"pid": int(pid), "role": role,
                    "rss_bytes_peak": 0, "user_ticks": 0, "system_ticks": 0,
                    "no_new_privs_observed": None, "seccomp_mode_observed": None,
                    "seccomp_filters_observed": None})
                role = record["role"]
                rss = max(0, int(fields[21])) * os.sysconf("SC_PAGE_SIZE")
                totals[role] += rss
                record["rss_bytes_peak"] = max(record["rss_bytes_peak"], rss)
                record["user_ticks"] = max(record["user_ticks"], int(fields[11]))
                record["system_ticks"] = max(record["system_ticks"], int(fields[12]))
                for line in (root / "status").read_text().splitlines():
                    if line.startswith("NoNewPrivs:"):
                        record["no_new_privs_observed"] = int(line.split()[1]) == 1
                    elif line.startswith("Seccomp:"):
                        record["seccomp_mode_observed"] = int(line.split()[1])
                    elif line.startswith("Seccomp_filters:"):
                        record["seccomp_filters_observed"] = int(line.split()[1])
            except (OSError, ValueError, IndexError):
                continue
        for role, total in totals.items():
            self.rss_peaks[role] = max(self.rss_peaks[role], total)
        self.rss_peaks["combined"] = max(self.rss_peaks["combined"], sum(totals.values()))

    def stop(self):
        result = super().stop()
        result.pop("cgroup_path", None)
        result["output_bytes_sampled_peak"] = self.output_size_peak
        return result


def driver_metrics(path):
    """Ignore all unrecognized content, including paths, SQL and diagnostics."""
    try:
        path = absolute_path(str(path), "driver_metrics", private=True)
        value = read_json(path)
    except (OSError, ValueError, BenchmarkError):
        return None
    if not isinstance(value, dict):
        return None
    result = {key: entry for key, entry in value.items() if key in SIDE_NUMBERS
              and type(entry) in (int, float) and 0 <= entry <= 1e30 and math.isfinite(entry)}
    for key in ("success", "source_read_included_in_compute", "engine_streaming"):
        if type(value.get(key)) is bool:
            result[key] = value[key]
    enums = {"engine": ("duckdb", "polars", "kelvo", "clickhouse"),
             "implementation": ("duckdb_sql", "polars_lazy"),
             "result_compression": ("none", "lz4_frame"),
             "execution_engine_requested": ("streaming", "in-memory", "auto")}
    for key, permitted in enums.items():
        if value.get(key) in permitted:
            result[key] = value[key]
    for key in ("manifest_sha256", "sql_sha256"):
        if isinstance(value.get(key), str) and SHA256.fullmatch(value[key]):
            result[key] = value[key]
    versions = value.get("versions")
    if isinstance(versions, dict):
        result["versions"] = {key: entry for key, entry in versions.items()
            if key in ("duckdb", "polars", "pyarrow", "kelvo", "clickhouse", "python")
            and isinstance(entry, str) and VERSION.fullmatch(entry)}
    return result or None


def query_metrics(path):
    result = safe_stats(path)
    if result and "federation" in result:
        for scan in result["federation"]:
            scan.pop("source", None)
            scan.pop("table", None)
    return result


def numeric_state(state, key):
    value = state.get(key, "")
    return int(value) if value.isdigit() and int(value) < (1 << 64) - 1 else None


def public_unit_exit(state):
    enums = {"ActiveState": {"active", "activating", "deactivating", "inactive", "failed"},
        "SubState": {"running", "exited", "dead", "failed", "start", "stop", "stop-sigterm", "stop-sigkill"},
        "Result": {"success", "exit-code", "signal", "core-dump", "timeout", "watchdog", "oom-kill", "resources", "protocol"}}
    result = {key: state.get(key) if state.get(key) in values else None for key, values in enums.items()}
    result.update({key: numeric_state(state, key) for key in ("ExecMainCode", "ExecMainStatus")})
    return result


def safe_wrapper_status(path):
    allowed_errors = {"stdout_arrow_byte_limit_exceeded", "capture_io_failed", "capture_descendants_did_not_close",
                      "command_failed", "wrapper_io_or_input_failed", "wrapper_status_write_failed"}
    try:
        value = read_json(absolute_path(str(path), "wrapper_status", private=True))
        if not isinstance(value, dict):
            return None
        result = {key: value[key] for key in ("stdout_bytes_seen", "stdout_bytes_retained", "stderr_bytes_seen", "stderr_bytes_retained")
                  if type(value.get(key)) is int and 0 <= value[key] <= (1 << 63) - 1}
        if type(value.get("command_returncode")) is int and -255 <= value["command_returncode"] <= 255:
            result["command_returncode"] = value["command_returncode"]
        if type(value.get("output_fsynced")) is bool:
            result["output_fsynced"] = value["output_fsynced"]
        if isinstance(value.get("errors"), list):
            result["errors"] = sorted({entry for entry in value["errors"] if isinstance(entry, str) and entry in allowed_errors})
        return result
    except (OSError, ValueError, BenchmarkError):
        return None


@contextmanager
def defer_cleanup_interrupts(errors):
    """Record interruption without abandoning a service during teardown."""
    previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}

    def interrupted(*_):
        if "interrupted" not in errors:
            errors.append("interrupted")

    try:
        for number in previous:
            signal.signal(number, interrupted)
        yield
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)


def execute_trial(manifest, case, trial, sequence, session, wrapper):
    limits = manifest["limits"]
    directory = session / (str(sequence).zfill(3) + "-" + case["id"] + "-" + str(trial))
    directory.mkdir(mode=0o700)
    scratch = directory / "scratch"
    scratch.mkdir(mode=0o700)
    artifact, log = directory / "result.arrow", directory / "systemd.log"
    substitutions = {"{output}": str(artifact), "{scratch}": str(scratch), "{metrics}": str(directory / "driver-metrics.json")}
    spec = directory / "invocation.json"
    write_json(spec, {"argv": [substitutions.get(arg, arg) for arg in case["argv"]],
        "environment": case["_environment"], "directory": str(directory), "scratch": str(scratch),
        "home": pwd.getpwuid(os.getuid()).pw_dir, "stdout_is_arrow": case["stdout_is_arrow"],
        "max_output_bytes": limits["max_output_bytes"], "log_max_bytes": LOG_MAX_BYTES})
    unit = "kelvo-analytics-" + secrets.token_hex(12) + ".service"
    result = {key: case[key] for key in ("id", "panel", "engine", "workflow")}
    result.update({"trial": trial, "sequence": sequence, "unit": unit, "started_at": utc_now(),
        "execution_state": "failed", "artifact_validation": {"state": "not_checked"},
        "canonical_validation": {"state": "pending_external"}, "errors": [], "unit_cleanup": {},
        "control_status_timeout_count": 0, "control_status_timeouts": [], "final_status_uncertain": False})
    sampler = None
    owned_unit = False
    state = {}
    started = deadline = None

    def read_status(phase, timeout):
        try:
            properties = (*UNIT_PROPERTIES, "CPUQuotaPerSecUSec", "CPUQuotaPeriodUSec")
            observed = systemctl(unit, ["show", "--no-pager", "--property=" + ",".join(properties)], log, timeout=timeout)
            if observed.returncode:
                raise BenchmarkError("unit_status_unavailable")
            values = dict(line.split("=", 1) for line in observed.stdout.decode().splitlines() if "=" in line)
        except subprocess.TimeoutExpired:
            result["control_status_timeout_count"] += 1
            result["control_status_timeouts"].append({"phase": phase, "at": utc_now(),
                "elapsed_seconds": time.monotonic() - started, "command_timeout_seconds": timeout})
            return None
        result["last_status_elapsed_seconds"] = time.monotonic() - started
        return values

    def verify_limits(values):
        finished = bool(numeric_state(values, "ExecMainExitTimestampMonotonic")) or values.get("ActiveState") == "failed"
        if values.get("ControlGroup") != "/system.slice/" + unit and not (finished and not values.get("ControlGroup")):
            raise BenchmarkError("unexpected_service_cgroup")
        if values.get("MemoryMax") != str(limits["memory_max_mib"] * MIB) or values.get("MemorySwapMax") != "0":
            raise BenchmarkError("service_memory_limits_not_applied")
        if values.get("TasksMax") != "96" or values.get("CPUWeight") != "20":
            raise BenchmarkError("service_task_or_cpu_weight_not_applied")
        if values.get("CPUQuotaPerSecUSec") != "2s" or values.get("CPUQuotaPeriodUSec") != "100ms":
            raise BenchmarkError("service_cpu_quota_not_applied")
        result["service_limits_verified"] = True
        return finished

    try:
        available = free_bytes(directory)
        result["filesystem_available_bytes_before_launch"] = available
        result["scratch_headroom_bytes_before_launch"] = max(0, available - limits["max_output_bytes"] - DISK_RESERVE - 2 * LOG_MAX_BYTES)
        if result["scratch_headroom_bytes_before_launch"] < MINIMUM_SCRATCH_HEADROOM:
            raise BenchmarkError("insufficient_scratch_headroom_for_trial")
        command = ["sudo", "-n", "systemd-run", "--quiet", "--unit=" + unit, "--service-type=exec"]
        properties = ["User=" + pwd.getpwuid(os.getuid()).pw_name, "Slice=system.slice",
            "MemoryMax=" + str(limits["memory_max_mib"]) + "M", "MemorySwapMax=0", "TasksMax=96",
            "CPUWeight=20", "CPUQuota=200%", "CPUQuotaPeriodSec=100ms", "RemainAfterExit=yes",
            "RuntimeMaxSec=" + str(limits["timeout_seconds"] + 30), "MemoryAccounting=yes",
            "CPUAccounting=yes", "TasksAccounting=yes", "OOMPolicy=kill", "KillMode=control-group",
            "TimeoutStopSec=5", "UMask=0077", "LimitCORE=0", "StandardOutput=null", "StandardError=null",
            "WorkingDirectory=" + str(directory)]
        command.extend("--property=" + value for value in properties)
        command.extend(["--", str(Path(sys.executable).resolve()), "-I", str(wrapper), str(spec)])
        sampler = WorkflowSampler(unit, directory)
        sampler.start()
        started = time.monotonic()
        deadline = started + limits["timeout_seconds"] + 45
        owned_unit = True  # A timed-out launch may already have been accepted.
        with log.open("ab") as diagnostics:
            launched = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=diagnostics,
                stderr=diagnostics, timeout=15, check=False)
        result["systemd_run_exit_status"] = launched.returncode
        if launched.returncode:
            raise BenchmarkError("systemd_run_failed")
        while True:
            if sampler.low_disk:
                raise BenchmarkError("scratch_free_space_below_reserve")
            if sampler.scratch_peak["logical_bytes"] > limits["temp_mib"] * MIB:
                raise BenchmarkError("sampled_scratch_limit_exceeded")
            if sampler.output_size_peak > limits["max_output_bytes"]:
                raise BenchmarkError("sampled_output_limit_exceeded")
            if sampler.disk_peak["logical_bytes"] > limits["temp_mib"] * MIB + limits["max_output_bytes"] + 2 * LOG_MAX_BYTES + 2 * MIB:
                raise BenchmarkError("sampled_run_files_limit_exceeded")
            if "scratch_file_count_exceeded" in sampler.failures:
                raise BenchmarkError("scratch_file_count_exceeded")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BenchmarkError("harness_deadline_exceeded")
            observed = read_status("monitor", min(10, remaining))
            if observed is not None:
                state = observed
                if verify_limits(state):
                    break
            time.sleep(min(STATUS_POLL_SECONDS, max(0, deadline - time.monotonic())))
        result["wall_seconds_including_service_start_and_status_poll"] = time.monotonic() - started
        if state.get("ExecMainCode") != "1" or state.get("ExecMainStatus") != "0" or state.get("Result") != "success":
            raise BenchmarkError("workflow_process_failed")
        result["execution_state"] = "succeeded"
    except KeyboardInterrupt:
        result["errors"].append("interrupted")
    except BenchmarkError as error:
        result["errors"].append(str(error))
    except subprocess.TimeoutExpired:
        result["errors"].append("systemd_command_timed_out")
    except (OSError, ValueError):
        result["errors"].append("harness_io_or_parse_error")
    finally:
        with defer_cleanup_interrupts(result["errors"]):
            if started is not None and "wall_seconds_including_service_start_and_status_poll" not in result:
                result["wall_seconds_including_service_start_and_status_poll"] = time.monotonic() - started
            if owned_unit:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    result["final_status_uncertain"] = True
                    result["final_status_observation"] = "retained_last_state_after_deadline"
                else:
                    try:
                        observed = read_status("final", min(10, remaining))
                        if observed is None:
                            result["final_status_uncertain"] = True
                            result["final_status_observation"] = "timeout_retained_last_state"
                        else:
                            state = observed
                            result["final_status_observation"] = "final_read"
                    except (OSError, ValueError, BenchmarkError):
                        result["final_status_uncertain"] = True
                        result["final_status_observation"] = "unavailable_retained_last_state"
            if sampler is not None:
                try:
                    result["metrics"] = sampler.stop()
                except (OSError, ValueError, BenchmarkError):
                    result["errors"].append("metrics_unavailable")
            # Counters and status above precede exact-unit cleanup. Teardown is
            # excluded from failed-process measurements and always recorded.
            if owned_unit:
                for action in ("stop", "reset-failed"):
                    attempts = []
                    for attempt in range(1, 3):
                        try:
                            cleaned = systemctl(unit, [action], log, timeout=15)
                            result["unit_cleanup"][action + "_exit_status"] = cleaned.returncode
                            attempts.append({"attempt": attempt, "exit_status": cleaned.returncode})
                            break
                        except subprocess.TimeoutExpired:
                            attempts.append({"attempt": attempt, "control_timeout": True})
                        except OSError:
                            attempts.append({"attempt": attempt, "control_io_error": True})
                            break
                    result["unit_cleanup"][action + "_attempts"] = attempts
                try:
                    # cgroup.events populated covers descendant cgroups too.
                    cgroup = Path("/sys/fs/cgroup/system.slice") / unit
                    if cgroup.exists():
                        events = read_counters(cgroup / "cgroup.events")
                        if "populated" not in events:
                            raise BenchmarkError("cgroup_cleanup_verification_failed")
                        empty = events["populated"] == 0
                    else:
                        empty = True
                    result["unit_cleanup"]["cgroup_empty"] = empty
                    if not empty:
                        result["errors"].append("owned_processes_remain_after_cleanup")
                except (OSError, ValueError, BenchmarkError):
                    result["unit_cleanup"]["cgroup_empty"] = None
                    result["errors"].append("cgroup_cleanup_verification_failed")
                if result["unit_cleanup"].get("stop_exit_status") != 0:
                    result["errors"].append("unit_stop_not_confirmed")
            result["unit_exit"] = public_unit_exit(state)
            result["systemd_accounting"] = {key: numeric_state(state, key) for key in
                ("MemoryPeak", "MemoryCurrent", "CPUUsageNSec") if numeric_state(state, key) is not None}
            counters = result.get("metrics", {}).get("cgroup_last_observed", {})
            events = counters.get("memory.events", {})
            result["oom_kill_observed"] = state.get("Result") == "oom-kill" or any(events.get(key, 0) > 0 for key in ("oom_kill", "oom_group_kill"))
            result["oom_event_observed"] = result["oom_kill_observed"] or events.get("oom", 0) > 0
            result["task_limit_event_observed"] = counters.get("pids.events", {}).get("max", 0) > 0
            start_us = numeric_state(state, "ExecMainStartTimestampMonotonic") or 0
            exit_us = numeric_state(state, "ExecMainExitTimestampMonotonic") or 0
            result["process_exit_status_verified"] = exit_us > 0 or state.get("ActiveState") == "failed"
            result["process_seconds_including_wrapper_and_fsync"] = (exit_us - start_us) / 1e6 if exit_us >= start_us > 0 else None
    result["wrapper_status"] = safe_wrapper_status(directory / "wrapper-status.json")
    if result["execution_state"] == "succeeded" and (not result["wrapper_status"] or not result["wrapper_status"].get("output_fsynced")):
        result["errors"].append("successful_wrapper_status_missing")
    # No Arrow imports: decoding and canonical comparison happen externally.
    try:
        if artifact.exists() or artifact.is_symlink():
            absolute_path(str(artifact), "artifact", private=True)
            if artifact.stat().st_size > limits["max_output_bytes"]:
                raise BenchmarkError("output_byte_limit_exceeded")
            hash_started = time.monotonic()
            evidence = hash_file(artifact)
            result["artifact_validation"] = {"state": "byte_integrity_observed" if evidence["arrow_eos_present"] else "failed", **evidence}
            result["artifact_hash_seconds_outside_process_timing"] = time.monotonic() - hash_started
            if not evidence["arrow_eos_present"]:
                result["errors"].append("arrow_eos_missing")
            result["output_from_verified_successful_process"] = result["execution_state"] == "succeeded" and result["process_exit_status_verified"]
        elif result["execution_state"] == "succeeded":
            result["errors"].append("successful_process_output_missing")
    except BenchmarkError as error:
        result["artifact_validation"]["state"] = "failed"
        result["errors"].append(str(error))
    except (OSError, ValueError):
        result["artifact_validation"]["state"] = "failed"
        result["errors"].append("artifact_read_failed")
    result["query_stats"] = query_metrics(directory / "stderr.log")
    result["driver_metrics"] = driver_metrics(directory / "driver-metrics.json")
    result["trial_state"] = "execution_and_byte_checks_passed" if not result["errors"] and result["execution_state"] == "succeeded" else "failed"
    result["finished_at"] = utc_now()
    write_json(directory / "measurement.json", result)
    return result


def public_case(case):
    result = {key: case[key] for key in ("id", "panel", "engine", "workflow", "stdout_is_arrow", "input_hashes", "code_hashes")}
    for key in ("expected_rows", "canonical_value_sha256"):
        if key in case:
            result[key] = case[key]
    result["reference_configured"] = "reference_file" in case
    result["executable_sha256"] = hash_file(Path(case["argv"][0]))["sha256"]
    return result


def round_robin(manifest):
    groups = {}
    for case in manifest["cases"]:
        groups.setdefault((case["workflow"], case["panel"]), []).append(case)
    ordered = [(key, sorted(cases, key=lambda case: (case["engine"], case["id"])))
               for key, cases in sorted(groups.items())]
    sequence = 0
    for trial in range(1, manifest["trials"] + 1):
        for _, cases in ordered:
            rotation = (trial - 1) % len(cases)
            for case in cases[rotation:] + cases[:rotation]:
                sequence += 1
                yield sequence, trial, case


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Private manifest schema 1:
{"schema_version":1,"profile":"azure-local","trials":3,
 "limits":{"memory_max_mib":2048,"threads":2,"tasks_max":96,"temp_mib":2048,
           "max_output_bytes":134217728,"timeout_seconds":180},
 "cases":[{"id":"daily-duckdb","panel":"C","engine":"duckdb","workflow":"daily",
   "argv":["/absolute/python","/absolute/driver.py","--output","{output}",
           "--scratch","{scratch}","--metrics","{metrics}"]}]}
Cases may add environment_file (private JSON), environment (private mapping),
reference_file, expected_rows, canonical_value_sha256, input_hashes/code_hashes
(label-to-SHA256 objects), stdout_is_arrow. stdout_is_arrow requires no {output}.
Only whole-argument placeholders are replaced; commands never use a shell.
Allowed environment: KELVO_SOURCE_*, ANALYTICS_SOURCE_*, CLICKHOUSE_*, PYTHONPATH,
PYTHONHOME, VIRTUAL_ENV, KELVO_SANDBOX_LAUNCHER and fixed Python/thread settings.
Limits: 640-2048 MiB memory, 2 threads, 96 tasks, CPU quota 200%, swap zero,
512/2048 MiB sampled temporary files, 1-600 seconds plus 30 seconds service
grace, 1-5 trials, 1-64 cases and six hours planned maximum runtime.
The workdir must be private, owned and disk-backed. Provision any shared
filesystem capacity bound externally. Output and metadata consume it too.
At least output budget + 64 MiB reserve + 64 MiB scratch + log budget must be
available before launch; the nominal temp limit is not a disk reservation.
Public labels/hashes must describe public fixtures. No paths, argv, source
environment, SQL or arbitrary logs enter the public report. Input hashes are
declared provenance, not recomputed fixture validation. All trials are retained.
--check validates inputs only: no systemd, process launch, Arrow read or output.
Canonical validation remains pending_external even when execution/EOS pass.
''')
    for name in ("manifest", "workdir", "output"):
        parser.add_argument("--" + name, required=True, metavar="ABS")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        args.manifest = absolute_path(args.manifest, "manifest", private=True)
        args.workdir = absolute_path(args.workdir, "workdir", directory=True, private=True)
        args.output = Path(args.output)
        if not args.output.is_absolute() or args.output.exists() or args.output.is_symlink() or not args.output.parent.is_dir():
            raise BenchmarkError("output_must_be_new_absolute_file_in_existing_directory")
        manifest = validate_manifest(args.manifest)
        if args.check:
            print('{"input_check":"passed","execution":"not_started","canonical_validation":"pending_external"}')
            return 0
        if sys.platform != "linux" or os.getuid() == 0 or not Path("/sys/fs/cgroup/cgroup.controllers").is_file():
            raise BenchmarkError("requires_nonroot_linux_user_and_cgroup_v2")
        fs_type = filesystem_type(args.workdir)
        if fs_type in ("tmpfs", "ramfs"):
            raise BenchmarkError("workdir_must_be_on_disk")
    except BenchmarkError as error:
        parser.error(str(error))
    except (OSError, ValueError, TypeError):
        parser.error("input_path_or_json_unavailable_or_invalid")
    os.umask(0o077)
    session = args.workdir / ("analytics-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + secrets.token_hex(6))
    session.mkdir(mode=0o700)
    wrapper = session / "workflow-wrapper.py"
    with wrapper.open("x") as output:
        output.write(WRAPPER)
        output.flush()
        os.fsync(output.fileno())
    schedule = list(round_robin(manifest))
    filesystem = os.statvfs(args.workdir)
    host = {"logical_cpus": os.cpu_count()}
    for key, value in (("machine", os.uname().machine), ("kernel", os.uname().release)):
        if VERSION.fullmatch(value):
            host[key] = value
    report = {"schema_version": 1, "profile": manifest["profile"], "started_at": utc_now(), "state": "running",
        "harness_sha256": hash_file(Path(__file__).resolve())["sha256"],
        "shared_sampler_sha256": hash_file(Path(__file__).with_name("federation_micro_benchmark.py"))["sha256"],
        "host": host, "limits": {**manifest["limits"], "memory_swap_max_bytes": 0, "cpu_weight": 20,
            "cpu_quota_percent": 200, "cpu_quota_period_usec": 100000, "trials": manifest["trials"],
            "disk_reserve_bytes": DISK_RESERVE, "minimum_scratch_headroom_bytes": MINIMUM_SCRATCH_HEADROOM,
            "log_max_bytes_per_stream": LOG_MAX_BYTES, "sample_seconds": SAMPLE_SECONDS,
            "status_poll_seconds": STATUS_POLL_SECONDS},
        "filesystem": {"type": fs_type if VERSION.fullmatch(fs_type) else "unknown",
            "capacity_bytes": filesystem.f_blocks * filesystem.f_frsize,
            "available_bytes_at_start": filesystem.f_bavail * filesystem.f_frsize,
            "capacity_scope": "shared_workdir_filesystem_including_all_retained_trials_and_other_files",
            "per_trial_scratch_limit_enforcement": "sampled_soft_limit"},
        "headline_time_metric": "process_seconds_including_wrapper_and_fsync",
        "canonical_validation": {"state": "pending_external"},
        "cases": [public_case(case) for case in manifest["cases"]],
        "schedule": [{"sequence": sequence, "trial": trial, "case": case["id"]} for sequence, trial, case in schedule],
        "results": [], "limitations": [
            "Execution success, byte SHA256 and Arrow EOS do not verify schema, rows, NULL semantics or values; canonical comparison remains external and pending.",
            "Headline time includes the fresh wrapper, interpreter and driver, complete Arrow output, captured logs, metadata and fsync. Hashing and external canonical decoding are outside it.",
            "Wall time includes scheduling and polling delay; the headline uses systemd process start/exit timestamps. Forced-stop failure counters end before cleanup.",
            "No caches are dropped. Trial number is outermost, workflows/panels are sorted, and sorted engine order rotates by trial. There are no hidden retries or concurrent timed services in this runner.",
            "Two-thread environment settings and a 200% cgroup CPU quota are common limits, not proof of equal VM entitlement or exactly two OS threads. Host steal and unrelated activity are reported.",
            "Cgroup memory includes page cache. Summed process RSS can double-count shared pages; process samples may miss brief peaks or short-lived descendants.",
            "Executable hashing before trials warms executable pages outside the service cgroup; dynamic engine libraries may first load inside it. Cgroup memory alone is not a total deployment RAM comparison.",
            "Process roles coordinator and worker mean wrapper and command/descendants, respectively; orphan reparenting can affect this sampled classification.",
            "Scratch and directory byte limits are sampled soft limits. Any external filesystem capacity applies to all retained outputs, logs, metadata and scratch together; nominal temp size is not reserved per trial.",
            "The shared filesystem capacity is observed, not provisioned or independently certified as an isolated quota by this script. Input files may reside elsewhere.",
            "Native source database execution and network paths are outside the local cgroup. Cross-host timing and remote pushdown cannot establish same-input local-library speedups.",
            "Federation fetched rows/Arrow bytes describe adapter output, not database storage scans. Supported source wire counters exclude HTTP/TLS framing and discovery.",
            "The command owns engine memory/temp/thread options beyond the forced environment and cgroup; a manifest must configure corresponding engine options consistently.",
            "Status timeouts are retried inside the original deadline; last known status and uncertainty survive failed observations. Exact-unit cleanup must establish an empty cgroup before further timed work.",
            "Only recognized sidecar counters and versions are copied; declared input/code hashes and expected canonical values are provenance claims pending independent validation."]}
    write_json(args.output, report, 0o644)
    interrupted = False
    previous_handler = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        for sequence, trial, case in schedule:
            result = execute_trial(manifest, case, trial, sequence, session, wrapper)
            report["results"].append(result)
            write_json(args.output, report, 0o644)
            print(json.dumps({"case": case["id"], "trial": trial,
                "execution": result["execution_state"], "trial_state": result["trial_state"],
                "canonical_validation": "pending_external"}), flush=True)
            if "interrupted" in result["errors"]:
                raise KeyboardInterrupt
            stop_codes = {"owned_processes_remain_after_cleanup", "cgroup_cleanup_verification_failed",
                "unit_stop_not_confirmed", "scratch_free_space_below_reserve", "insufficient_scratch_headroom_for_trial",
                "sampled_scratch_limit_exceeded", "sampled_run_files_limit_exceeded", "scratch_file_count_exceeded"}
            if stop_codes.intersection(result["errors"]):
                raise BenchmarkError("stopped_after_cleanup_or_disk_failure")
    except KeyboardInterrupt:
        interrupted = True
    except BenchmarkError as error:
        report["error"] = str(error)
    except (OSError, ValueError):
        report["error"] = "report_or_run_io_failed"
    finally:
        signal.signal(signal.SIGTERM, previous_handler)
        report["finished_at"] = utc_now()
        report["all_execution_and_byte_checks_passed"] = not interrupted and "error" not in report and len(report["results"]) == len(schedule) and all(
            result["trial_state"] == "execution_and_byte_checks_passed" for result in report["results"])
        report["state"] = "interrupted" if interrupted else "execution_complete_validation_pending" if report["all_execution_and_byte_checks_passed"] else "failed"
        write_json(args.output, report, 0o644)
    return 0 if report["all_execution_and_byte_checks_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())

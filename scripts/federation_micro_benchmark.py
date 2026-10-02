#!/usr/bin/env python3
"""Run bounded Kelvo query trials on a provisioned Linux micro VM.

Requires Python 3's standard library, cgroup v2, systemd and passwordless sudo.
The binary, Landlock launcher, YAML catalog, private source environment JSON and
an owned directory on disk must already exist. This script never provisions,
builds, downloads or deletes query artifacts. Run --help for the input contract.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import secrets
import signal
import stat
import subprocess
import sys
import threading
import time


MIB = 1 << 20
TEMP_MIB = 512
DISK_RESERVE = 64 * MIB
SAMPLE_SECONDS = 0.1
STATUS_POLL_SECONDS = 0.5
EOS = b"\xff\xff\xff\xff\0\0\0\0"
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]{0,127}\Z")
UNIT_PROPERTIES = (
    "LoadState", "ActiveState", "SubState", "Result", "ControlGroup",
    "ExecMainPID", "ExecMainCode", "ExecMainStatus",
    "ExecMainStartTimestampMonotonic", "ExecMainExitTimestampMonotonic",
    "MemoryMax", "MemorySwapMax", "TasksMax", "CPUWeight",
    "MemoryPeak", "MemoryCurrent", "CPUUsageNSec",
)

# Only paths and public query arguments enter this private spec. Secrets are
# read inside the service and never put in systemd properties or argv.
WRAPPER = r'''import json, os, pathlib, re, sys
try:
    spec = json.loads(pathlib.Path(sys.argv[1]).read_text())
    os.umask(0o077)
    for descriptor, name in ((1, "stdout.log"), (2, "stderr.log")):
        fd = os.open(str(pathlib.Path(spec["directory"]) / name),
                     os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.dup2(fd, descriptor)
        os.close(fd)
    path = pathlib.Path(spec["environment"])
    info = path.lstat()
    if not path.is_file() or path.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("private environment file permissions")
    if info.st_size > 1048576:
        raise ValueError("environment file size")
    values = json.loads(path.read_text())
    if not isinstance(values, dict) or not 1 <= len(values) <= 64:
        raise ValueError("source environment object")
    for key, value in values.items():
        if not re.fullmatch(r"KELVO_SOURCE_[A-Z0-9_]{1,100}", key) or not isinstance(value, str) or "\0" in value or len(value) > 65536:
            raise ValueError("source environment entry")
    environment = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": spec["home"],
        "LANG": "C.UTF-8", "TMPDIR": spec["scratch"],
        "GOMAXPROCS": str(spec["threads"]), "KELVO_SANDBOX_LAUNCHER": spec["launcher"]}
    environment.update(values)
    os.chdir(spec["directory"])
    os.execve(spec["binary"], spec["argv"], environment)
except BaseException as error:
    # Never print an exception message: a parser or native error may quote data.
    os.write(2, ("benchmark wrapper failed: " + type(error).__name__ + "\n").encode())
    sys.exit(125)
'''


class BenchmarkError(Exception):
    """A fixed, public error code; never wrap arbitrary exception messages."""


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def integer(value, minimum, maximum, label):
    if type(value) is not int or not minimum <= value <= maximum:
        raise BenchmarkError("invalid_" + label)
    return value


def absolute_path(value, label, directory=False, private=False, executable=False):
    path = Path(value)
    if not path.is_absolute():
        raise BenchmarkError(label + "_must_be_absolute")
    info = path.lstat()
    if stat.S_ISLNK(info.st_mode):
        raise BenchmarkError(label + "_must_not_be_a_symlink")
    if not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)):
        raise BenchmarkError("invalid_" + label + "_type")
    if private and (info.st_uid != os.getuid() or info.st_mode & 0o077):
        raise BenchmarkError(label + "_must_be_owned_and_private")
    if executable and (not os.access(path, os.X_OK) or info.st_mode & 0o022):
        raise BenchmarkError(label + "_must_be_executable_and_not_group_writable")
    return path.resolve()


def read_json(path, maximum=1 << 20):
    if path.stat().st_size > maximum:
        raise BenchmarkError("input_json_too_large")
    return json.loads(path.read_text())


def write_json(path, value, mode=0o600):
    temporary = path.with_name(path.name + ".tmp-" + secrets.token_hex(4))
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        os.fchmod(fd, mode)
        with os.fdopen(fd, "w") as output:
            json.dump(value, output, indent=2, allow_nan=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def hash_file(path):
    digest, size, tail = hashlib.sha256(), 0, b""
    with path.open("rb") as source:
        while True:
            block = source.read(MIB)
            if not block:
                break
            digest.update(block)
            size += len(block)
            tail = (tail + block)[-8:]
    return {"bytes": size, "sha256": digest.hexdigest(), "arrow_eos_present": tail == EOS}


def filesystem_type(path):
    matches = []
    for line in Path("/proc/self/mountinfo").read_text().splitlines():
        left, right = line.split(" - ", 1)
        mount = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), left.split()[4])
        if path == Path(mount) or Path(mount) in path.parents:
            matches.append((len(mount), right.split()[0]))
    if not matches:
        raise BenchmarkError("scratch_filesystem_unknown")
    return max(matches)[1]


def validate_queries(path, memory_max):
    entries = read_json(path)
    if not isinstance(entries, list) or not 1 <= len(entries) <= 16:
        raise BenchmarkError("queries_must_be_a_list_of_1_to_16_entries")
    required = {"name", "sql", "mode", "sources", "memory_mb", "threads",
                "max_rows", "max_bytes", "timeout_seconds"}
    optional = {"result_compression"}
    names = set()
    for query in entries:
        if not isinstance(query, dict) or not required.issubset(query) or set(query) - required - optional:
            raise BenchmarkError("query_fields_do_not_match_contract")
        if not isinstance(query["name"], str) or not IDENTIFIER.fullmatch(query["name"]) or query["name"] in names:
            raise BenchmarkError("query_name_invalid_or_repeated")
        names.add(query["name"])
        if not isinstance(query["sql"], str) or not 1 <= len(query["sql"]) <= 65536 or "\0" in query["sql"]:
            raise BenchmarkError("invalid_sql")
        if query["mode"] not in ("federated", "native"):
            raise BenchmarkError("invalid_query_mode")
        if query.get("result_compression", "none") not in ("none", "lz4_frame"):
            raise BenchmarkError("invalid_result_compression")
        sources = query["sources"]
        if not isinstance(sources, str) or not 1 <= len(sources.split(",")) <= 8:
            raise BenchmarkError("invalid_query_sources")
        if any(not IDENTIFIER.fullmatch(source) for source in sources.split(",")):
            raise BenchmarkError("invalid_query_sources")
        if query["mode"] == "native" and "," in sources:
            raise BenchmarkError("native_query_requires_one_source")
        if query["memory_mb"] not in ((128, 256, 512) if query["mode"] == "native" else (128,)):
            raise BenchmarkError("memory_mb_must_be_128_or_native_256_or_512")
        integer(query["memory_mb"], 128, memory_max - 64, "memory_mb")
        integer(query["threads"], 1, 2, "threads")
        integer(query["max_rows"], 1, 100_000_000, "max_rows")
        integer(query["max_bytes"], 1024, 1 << 30, "max_bytes")
        integer(query["timeout_seconds"], 1, 600, "timeout_seconds")
    return entries


def read_integer(path):
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return None


def read_counters(path):
    try:
        return {key: int(value) for key, value in
                (line.split() for line in path.read_text().splitlines())}
    except (OSError, ValueError):
        return {}


def host_memory_available():
    for line in Path("/proc/meminfo").read_text().splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise BenchmarkError("host_memavailable_unavailable")


def host_cpu():
    ticks = [int(value) for value in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
    return {"total_ticks": sum(ticks[:8]), "steal_ticks": ticks[7]}


def disk_usage(root):
    logical = allocated = count = 0
    for directory, subdirs, files in os.walk(root, followlinks=False):
        subdirs[:] = [name for name in subdirs if not (Path(directory) / name).is_symlink()]
        for name in files:
            count += 1
            if count > 100_000:
                raise BenchmarkError("scratch_file_count_exceeded")
            try:
                info = (Path(directory) / name).lstat()
                if stat.S_ISREG(info.st_mode):
                    logical += info.st_size
                    allocated += info.st_blocks * 512
            except FileNotFoundError:
                pass
    return {"logical_bytes": logical, "allocated_bytes": allocated}


def free_bytes(path):
    info = os.statvfs(path)
    return info.f_bavail * info.f_frsize


class Sampler:
    """Outside-service sampling; cgroup counters supplement short-lived /proc data."""

    def __init__(self, unit, directory, binary):
        self.cgroup = Path("/sys/fs/cgroup/system.slice") / unit
        self.directory, self.binary = directory, str(binary)
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.samples, self.failures, self.processes = 0, set(), {}
        self.memory_baseline = self.memory_minimum = host_memory_available()
        self.free_baseline = self.free_minimum = free_bytes(directory)
        self.cpu_before = host_cpu()
        self.cpu_after = dict(self.cpu_before)
        self.memory_current_peak = self.memory_kernel_peak = 0
        self.rss_peaks = {"coordinator": 0, "worker": 0, "combined": 0}
        self.disk_peak = {"logical_bytes": 0, "allocated_bytes": 0}
        self.scratch_peak = dict(self.disk_peak)
        self.counters = {}
        self.low_disk = False
        self.last_sample, self.maximum_gap = None, 0.0

    def start(self):
        self.thread.start()

    def sample(self):
        now = time.monotonic()
        if self.last_sample is not None:
            self.maximum_gap = max(self.maximum_gap, now - self.last_sample)
        self.last_sample = now
        self.memory_minimum = min(self.memory_minimum, host_memory_available())
        self.free_minimum = min(self.free_minimum, free_bytes(self.directory))
        self.low_disk = self.free_minimum < DISK_RESERVE
        self.cpu_after = host_cpu()
        for key in ("memory.current", "memory.peak", "memory.swap.current", "pids.peak"):
            value = read_integer(self.cgroup / key)
            if value is not None:
                self.counters[key] = value
                if key == "memory.current":
                    self.memory_current_peak = max(self.memory_current_peak, value)
                if key == "memory.peak":
                    self.memory_kernel_peak = max(self.memory_kernel_peak, value)
        for key in ("memory.events", "memory.events.local", "cpu.stat"):
            value = read_counters(self.cgroup / key)
            if value:
                self.counters[key] = value
        totals = {"coordinator": 0, "worker": 0}
        try:
            pids = (self.cgroup / "cgroup.procs").read_text().split()
        except OSError:
            pids = []
        for pid in pids:
            try:
                root = Path("/proc") / pid
                fields = (root / "stat").read_text().rsplit(") ", 1)[1].split()
                argv = (root / "cmdline").read_bytes().split(b"\0")
                if len(argv) < 2 or os.fsdecode(argv[0]) != self.binary or argv[1] not in (b"query", b"worker"):
                    continue
                role = "worker" if argv[1] == b"worker" else "coordinator"
                # A fork child can briefly retain its parent's query argv before
                # exec. The service coordinator itself is parented by systemd.
                if role == "coordinator" and int(fields[1]) != 1:
                    continue
                identity = (int(pid), int(fields[19]))
                record = self.processes.setdefault(identity, {"pid": int(pid), "role": role,
                    "rss_bytes_peak": 0, "user_ticks": 0, "system_ticks": 0,
                    "no_new_privs_observed": None, "seccomp_mode_observed": None,
                    "seccomp_filters_observed": None})
                record["role"] = role
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
        for root, peaks in ((self.directory, self.disk_peak),
                            (self.directory / "scratch", self.scratch_peak)):
            current = disk_usage(root)
            for key, value in current.items():
                peaks[key] = max(peaks[key], value)
        self.samples += 1

    def run(self):
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                self.sample()
            except BenchmarkError as error:
                self.failures.add(str(error))
            except (OSError, ValueError):
                self.failures.add("sample_unavailable")
            self.stop_event.wait(max(0, SAMPLE_SECONDS - (time.monotonic() - started)))

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=10)
        if self.thread.is_alive():
            raise BenchmarkError("sampler_did_not_stop")
        try:
            self.sample()
        except (OSError, ValueError, BenchmarkError):
            self.failures.add("final_sample_unavailable")
        hz = os.sysconf("SC_CLK_TCK")
        processes = []
        for original in self.processes.values():
            record = dict(original)
            record["sampled_user_seconds"] = record.pop("user_ticks") / hz
            record["sampled_system_seconds"] = record.pop("system_ticks") / hz
            processes.append(record)
        counts = {role: sum(record["role"] == role for record in processes)
                  for role in ("coordinator", "worker")}
        cpu_roles = {role: {kind: sum(record["sampled_" + kind + "_seconds"]
                    for record in processes if record["role"] == role)
                    for kind in ("user", "system")} if counts[role] else None
                    for role in counts}
        rss_peaks = {role: value if (processes if role == "combined" else counts[role]) else None
                     for role, value in self.rss_peaks.items()}
        delta = {key: self.cpu_after[key] - value for key, value in self.cpu_before.items()}
        return {"samples": self.samples, "interval_seconds": SAMPLE_SECONDS,
            "maximum_sample_gap_seconds": self.maximum_gap, "sampling_errors": sorted(self.failures),
            "processes": processes, "observed_process_counts": counts,
            "sampled_cpu_seconds_by_role": cpu_roles, "sampled_rss_bytes_peak": rss_peaks,
            "cgroup_path": str(self.cgroup), "cgroup_last_observed": self.counters,
            "cgroup_exists_at_final_sample": self.cgroup.is_dir(),
            "cgroup_memory_current_bytes_sampled_peak": self.memory_current_peak,
            "cgroup_memory_peak_bytes": self.memory_kernel_peak or None,
            "host_memavailable_bytes_baseline": self.memory_baseline,
            "host_memavailable_bytes_minimum": self.memory_minimum,
            "host_cpu_delta": {**delta, "clock_ticks_per_second": hz,
                "steal_seconds": delta["steal_ticks"] / hz,
                "steal_fraction": delta["steal_ticks"] / delta["total_ticks"] if delta["total_ticks"] else None},
            "run_files_bytes_peak": self.disk_peak, "scratch_files_bytes_peak": self.scratch_peak,
            "filesystem_available_bytes_baseline": self.free_baseline,
            "filesystem_available_bytes_minimum": self.free_minimum}


def systemctl(unit, arguments, log, timeout=10):
    with log.open("ab") as errors:
        return subprocess.run(["sudo", "-n", "systemctl", *arguments, unit],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=errors,
            timeout=timeout, check=False)


def unit_state(unit, log, timeout=10):
    result = systemctl(unit, ["show", "--no-pager", "--property=" + ",".join(UNIT_PROPERTIES)], log, timeout=timeout)
    if result.returncode:
        raise BenchmarkError("unit_status_unavailable")
    return dict(line.split("=", 1) for line in result.stdout.decode().splitlines() if "=" in line)


def safe_stats(path):
    """Accept only known success counters; never copy arbitrary stderr text."""
    try:
        value = read_json(path)
    except (OSError, ValueError, BenchmarkError):
        return None
    if not isinstance(value, dict):
        return None
    numeric = ("rows", "batches", "arrow_bytes", "wire_bytes", "source_wire_bytes", "prepare_ns", "duration_ns")
    result = {key: value[key] for key in numeric if type(value.get(key)) is int and value[key] >= 0}
    if type(value.get("engine_streaming")) is bool:
        result["engine_streaming"] = value["engine_streaming"]
    if value.get("backend") in ("duckdb", "clickhouse", "postgresql", "postgres", "mysql", "sqlite"):
        result["backend"] = value["backend"]
    scans = value.get("federation", [])
    if isinstance(scans, list) and len(scans) <= 256:
        result["federation"] = []
        for scan in scans:
            if not isinstance(scan, dict):
                continue
            clean = {key: scan[key] for key in ("scans", "rows_fetched", "arrow_bytes_fetched", "batches_fetched", "source_wire_bytes")
                     if type(scan.get(key)) is int and scan[key] >= 0}
            for key in ("source", "table"):
                if isinstance(scan.get(key), str) and IDENTIFIER.fullmatch(scan[key]):
                    clean[key] = scan[key]
            result["federation"].append(clean)
    return result or None


def execute_trial(args, query, trial, session, wrapper):
    directory = session / (query["name"] + "-" + str(trial))
    directory.mkdir(mode=0o700)
    scratch = directory / "scratch"
    scratch.mkdir(mode=0o700)
    artifact, log = directory / "result.arrow", directory / "systemd.log"
    unit = "kelvo-micro-" + secrets.token_hex(12) + ".service"
    argv = [str(args.binary), "query", "--config", str(args.config), "--sandbox", str(args.launcher), "--sql", query["sql"],
        "--mode", query["mode"], "--connection" if query["mode"] == "native" else "--sources", query["sources"],
        "--out", str(artifact), "--memory-mb", str(query["memory_mb"]), "--threads", str(query["threads"]),
        "--max-rows", str(query["max_rows"]), "--max-bytes", str(query["max_bytes"]),
        "--timeout", str(query["timeout_seconds"]) + "s", "--temp-mb", str(TEMP_MIB)]
    # Preserve compatibility with binaries from before this option existed.
    if "result_compression" in query:
        argv.extend(["--result-compression", query["result_compression"]])
    spec = directory / "invocation.json"
    write_json(spec, {"binary": str(args.binary), "launcher": str(args.launcher), "argv": argv,
        "environment": str(args.environment), "directory": str(directory), "scratch": str(scratch),
        "home": pwd.getpwuid(os.getuid()).pw_dir, "threads": query["threads"]})
    result = {"query": query["name"], "trial": trial, "unit": unit, "started_at": utc_now(),
              "result_compression": query.get("result_compression", "none"),
              "result_compression_explicit": "result_compression" in query,
              "state": "failed", "private_run_directory": str(directory),
              "private_logs": {"service": str(directory / "stderr.log"), "systemd": str(log),
                               "stdout": str(directory / "stdout.log")},
              "configured_sandbox_launcher": str(args.launcher), "unit_cleanup": {}, "errors": [],
              "control_status_timeout_count": 0, "control_status_timeouts": [],
              "final_status_uncertain": False}
    sampler = None
    owned_unit = False
    state = {}
    started = None
    deadline = None

    def read_status(phase, timeout):
        try:
            observed = unit_state(unit, log, timeout=timeout)
        except subprocess.TimeoutExpired:
            result["control_status_timeout_count"] += 1
            result["control_status_timeouts"].append({"phase": phase, "at": utc_now(),
                "elapsed_seconds": time.monotonic() - started, "command_timeout_seconds": timeout})
            return None
        result["last_status_observed_at"] = utc_now()
        result["last_status_elapsed_seconds"] = time.monotonic() - started
        return observed

    try:
        if free_bytes(directory) < query["max_bytes"] + TEMP_MIB * MIB + DISK_RESERVE:
            raise BenchmarkError("insufficient_scratch_space_for_bounded_trial")
        command = ["sudo", "-n", "systemd-run", "--quiet", "--unit=" + unit, "--service-type=exec"]
        properties = ["User=" + pwd.getpwuid(os.getuid()).pw_name, "Slice=system.slice",
            "MemoryMax=" + str(args.memory_max_mib) + "M", "MemorySwapMax=0", "TasksMax=96",
            "CPUWeight=20", "RemainAfterExit=yes", "RuntimeMaxSec=" + str(query["timeout_seconds"] + 30),
            "MemoryAccounting=yes", "CPUAccounting=yes", "TasksAccounting=yes", "OOMPolicy=kill",
            "KillMode=control-group", "TimeoutStopSec=5", "UMask=0077", "LimitCORE=0",
            "StandardOutput=null", "StandardError=null", "WorkingDirectory=" + str(directory)]
        command.extend("--property=" + value for value in properties)
        command.extend(["--", str(Path(sys.executable).resolve()), "-I", str(wrapper), str(spec)])
        sampler = Sampler(unit, directory, args.binary)
        sampler.start()
        started = time.monotonic()
        deadline = started + query["timeout_seconds"] + 45
        owned_unit = True  # Start may time out after systemd has accepted the unit.
        with log.open("ab") as diagnostics:
            launched = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=diagnostics,
                stderr=diagnostics, timeout=15, check=False)
        result["systemd_run_exit_status"] = launched.returncode
        if launched.returncode:
            raise BenchmarkError("systemd_run_failed")
        while True:
            if sampler.low_disk:
                raise BenchmarkError("scratch_free_space_below_reserve")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise BenchmarkError("harness_deadline_exceeded")
            observed = read_status("monitor", min(10, remaining))
            if observed is None:
                # A slow control command is not evidence that the query failed.
                # Keep sampling and retry without extending either deadline.
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BenchmarkError("harness_deadline_exceeded")
                time.sleep(min(STATUS_POLL_SECONDS, remaining))
                continue
            state = observed
            finished = (state.get("ExecMainExitTimestampMonotonic", "0") not in ("", "0")
                        or state.get("ActiveState") == "failed")
            if state.get("ControlGroup") != "/system.slice/" + unit and not (finished and not state.get("ControlGroup")):
                raise BenchmarkError("unexpected_service_cgroup")
            if state.get("MemoryMax") != str(args.memory_max_mib * MIB) or state.get("MemorySwapMax") != "0":
                raise BenchmarkError("service_memory_limits_not_applied")
            if state.get("TasksMax") != "96" or state.get("CPUWeight") != "20":
                raise BenchmarkError("service_task_or_cpu_limits_not_applied")
            if finished:
                break
            if sampler.low_disk:
                raise BenchmarkError("scratch_free_space_below_reserve")
            if time.monotonic() >= deadline:
                raise BenchmarkError("harness_deadline_exceeded")
            time.sleep(min(STATUS_POLL_SECONDS, max(0, deadline - time.monotonic())))
        result["wall_seconds_including_service_start_and_cli_fsync"] = time.monotonic() - started
        if state.get("ExecMainCode") != "1" or state.get("ExecMainStatus") != "0" or state.get("Result") != "success":
            raise BenchmarkError("query_process_failed")
        result["state"] = "completed"
    except KeyboardInterrupt:
        result["errors"].append("interrupted")
    except BenchmarkError as error:
        result["errors"].append(str(error))
    except subprocess.TimeoutExpired:
        result["errors"].append("systemd_command_timed_out")
    except (OSError, ValueError):
        result["errors"].append("harness_io_or_parse_error")
    finally:
        if started is not None and "wall_seconds_including_service_start_and_cli_fsync" not in result:
            result["wall_seconds_including_service_start_and_cli_fsync"] = time.monotonic() - started
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
        # Read all counters before stopping/resetting this exact owned unit.
        if owned_unit:
            for action in ("stop", "reset-failed"):
                attempts = []
                for attempt in range(1, 3):
                    try:
                        cleaned = systemctl(unit, [action], log, timeout=15)
                        result["unit_cleanup"][action + "_exit_status"] = cleaned.returncode
                        attempts.append({"attempt": attempt, "exit_status": cleaned.returncode})
                        if cleaned.returncode and action == "stop":
                            result["errors"].append("unit_stop_failed")
                        break
                    except subprocess.TimeoutExpired:
                        attempts.append({"attempt": attempt, "control_timeout": True, "at": utc_now()})
                        if attempt == 2:
                            result["errors"].append("unit_" + action + "_failed")
                    except OSError:
                        attempts.append({"attempt": attempt, "control_io_error": True})
                        result["errors"].append("unit_" + action + "_failed")
                        break
                result["unit_cleanup"][action + "_attempts"] = attempts
        result["unit_exit"] = {key: state.get(key) for key in
            ("ActiveState", "SubState", "Result", "ExecMainCode", "ExecMainStatus")}
        result["systemd_accounting"] = {key: int(state[key]) for key in
            ("MemoryPeak", "MemoryCurrent", "CPUUsageNSec")
            if state.get(key, "").isdigit() and int(state[key]) < (1 << 64) - 1}
        memory_events = result.get("metrics", {}).get("cgroup_last_observed", {}).get("memory.events", {})
        result["oom_observed"] = (state.get("Result") == "oom-kill" or
            any(memory_events.get(key, 0) > 0 for key in ("oom_kill", "oom_group_kill")))
        start_us = int(state.get("ExecMainStartTimestampMonotonic", "0") or "0")
        exit_us = int(state.get("ExecMainExitTimestampMonotonic", "0") or "0")
        result["query_exit_status_verified"] = exit_us > 0 or state.get("ActiveState") == "failed"
        result["cli_process_seconds_including_wrapper_and_fsync"] = (exit_us - start_us) / 1e6 if exit_us >= start_us > 0 else None
    try:
        if artifact.is_file() and not artifact.is_symlink():
            result["output_published_with_unverified_exit_status"] = not result["query_exit_status_verified"]
            result["failed_query_published_output"] = (result["query_exit_status_verified"] and
                (state.get("ExecMainCode") != "1" or state.get("ExecMainStatus") != "0" or state.get("Result") != "success"))
            hash_started = time.monotonic()
            result["artifact"] = {"path": str(artifact), **hash_file(artifact)}
            result["artifact_hash_seconds_outside_query_timing"] = time.monotonic() - hash_started
            if result["state"] == "completed" and not result["artifact"]["arrow_eos_present"]:
                result["errors"].append("arrow_eos_missing")
        elif result["state"] == "completed":
            result["errors"].append("successful_query_output_missing")
        if result["state"] == "completed":
            result["query_stats"] = safe_stats(directory / "stderr.log")
    except (OSError, ValueError):
        result["errors"].append("artifact_read_failed")
    if result["errors"]:
        result["state"] = "failed"
    result["finished_at"] = utc_now()
    write_json(directory / "measurement.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Queries JSON: [{\"name\":\"zone_join\",\"sql\":\"SELECT ...\",\"mode\":\"federated\",
  \"sources\":\"taxi\",\"memory_mb\":128,\"threads\":2,\"max_rows\":1000,
  \"max_bytes\":67108864,\"timeout_seconds\":180}]
Native queries use a single sources value as --connection. Limits: 1-16 queries,
1-5 trials, timeout 1-600 seconds, at most 6 hours of planned maximum runtimes,
1 GiB returned bytes per query, 512 MiB temporary disk per query, 1-2 threads.
memory_mb is 128; native source client/server budgets also allow 256 or 512.
Optional result_compression is none (default) or lz4_frame. The CLI flag is
passed only when that field is supplied, preserving older-binary compatibility.
The workdir and source environment JSON must be owned by the invoking non-root
user, with no group/world permissions. Environment JSON contains only string
KELVO_SOURCE_* values. The output report must be new; its parent must exist.
The workdir must be on disk, not tmpfs/ramfs. Successful and failed artifacts are
retained. Only Arrow byte hashes/EOS are checked; decode and compare separately.
""")
    for name in ("binary", "launcher", "config", "environment", "workdir", "output", "queries"):
        parser.add_argument("--" + name, required=True, metavar="ABS")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--memory-max-mib", type=int, default=640, help="cgroup limit, 256-768 MiB (default: 640)")
    args = parser.parse_args()
    try:
        integer(args.trials, 1, 5, "trials")
        integer(args.memory_max_mib, 256, 768, "memory_max_mib")
        if sys.platform != "linux" or os.getuid() == 0 or not Path("/sys/fs/cgroup/cgroup.controllers").is_file():
            raise BenchmarkError("requires_nonroot_linux_user_and_cgroup_v2")
        args.binary = absolute_path(args.binary, "binary", executable=True)
        args.launcher = absolute_path(args.launcher, "launcher", executable=True)
        args.config = absolute_path(args.config, "config")
        if args.config.stat().st_size > MIB:
            raise BenchmarkError("config_exceeds_one_mib")
        args.environment = absolute_path(args.environment, "environment", private=True)
        args.workdir = absolute_path(args.workdir, "workdir", directory=True, private=True)
        args.queries = absolute_path(args.queries, "queries")
        args.output = Path(args.output)
        if not args.output.is_absolute() or args.output.exists() or args.output.is_symlink() or not args.output.parent.is_dir():
            raise BenchmarkError("output_must_be_new_absolute_file_in_existing_directory")
        fs_type = filesystem_type(args.workdir)
        if fs_type in ("tmpfs", "ramfs"):
            raise BenchmarkError("workdir_must_be_on_disk")
        queries = validate_queries(args.queries, args.memory_max_mib)
        if args.trials * sum(query["timeout_seconds"] + 45 for query in queries) > 6 * 3600:
            raise BenchmarkError("planned_maximum_runtime_exceeds_six_hours")
    except BenchmarkError as error:
        parser.error(str(error))
    except (OSError, ValueError):
        parser.error("input_path_or_json_unavailable_or_invalid")
    os.umask(0o077)
    session = args.workdir / ("benchmark-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-") + secrets.token_hex(6))
    session.mkdir(mode=0o700)
    wrapper = session / "query-wrapper.py"
    fd = os.open(wrapper, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o700)
    with os.fdopen(fd, "w") as output:
        output.write(WRAPPER)
    report = {"schema_version": 1, "started_at": utc_now(), "state": "running",
        "harness_sha256": hash_file(Path(__file__).resolve())["sha256"],
        "binary_sha256": hash_file(args.binary)["sha256"],
        "launcher_sha256": hash_file(args.launcher)["sha256"],
        "config_sha256": hash_file(args.config)["sha256"],
        "queries_sha256": hash_file(args.queries)["sha256"],
        "host": {"machine": os.uname().machine, "kernel": os.uname().release, "logical_cpus": os.cpu_count()},
        "limits": {"memory_max_mib": args.memory_max_mib, "memory_swap_max_bytes": 0,
            "tasks_max": 96, "cpu_weight": 20, "cpu_quota_configured": False, "temporary_disk_mib": TEMP_MIB,
            "disk_reserve_bytes": DISK_RESERVE, "trials": args.trials, "sample_seconds": SAMPLE_SECONDS,
            "status_poll_seconds": STATUS_POLL_SECONDS},
        "headline_time_metric": "cli_process_seconds_including_wrapper_and_fsync",
        "scratch_filesystem": fs_type,
        "queries": [{**{key: value for key, value in query.items() if key != "sql"},
            "result_compression": query.get("result_compression", "none"),
            "result_compression_explicit": "result_compression" in query,
            "sql_sha256": hashlib.sha256(query["sql"].encode()).hexdigest()} for query in queries],
        "results": [],
        "limitations": [
            "Byte SHA256 and Arrow EOS do not prove decoded schema, rows or value correctness; validate retained outputs independently.",
            "Process RSS/CPU and scratch peaks are sampled every 100 ms and can miss brief peaks or short-lived processes.",
            "Process CPU counters stop at the last observed sample; cgroup cpu.stat includes all service processes, including the wrapper and launcher.",
            "Cgroup memory includes charged page cache; it is not process RSS. memory.peak availability depends on the kernel.",
            "Failed services may lose their cgroup before the final sample; retained counters are the last observation and systemd accounting is reported separately.",
            "Counters and wall times for forced-stop failures end before unit teardown; cleanup is outside these measurements.",
            "Host steal time, MemAvailable and filesystem free space include unrelated host activity.",
            "Service wall time includes scheduling, the environment wrapper and CLI fsync; hashing runs after the query timing.",
            "Service status is polled every 500 ms; wall time includes polling delay. The headline CLI time uses systemd process start/exit timestamps.",
            "Status-command timeouts are recorded and retried only inside the original query observation deadline; they do not establish query failure or OOM.",
            "If the final status read is unavailable, the report flags uncertainty and retains the last observed state; a previously verified exit remains valid.",
            "Exact-unit cleanup retries a timed-out control command once; cleanup remains outside query timings.",
            "The CLI is explicitly configured with --sandbox; NoNewPrivs and Seccomp observations alone do not verify the complete Landlock policy.",
            "Federation rows/bytes are fetched Arrow data, not remote database storage rows/bytes scanned.",
            "Source wire bytes count supported adapters' encoded response body bytes consumed; federation excludes discovery, and HTTP/TLS framing is excluded.",
            "No caches are dropped. Trials execute sequentially in query order and retain all completed and failed artifacts."]}
    write_json(args.output, report, 0o644)
    interrupted = False
    previous_handler = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        for query in queries:
            for trial in range(1, args.trials + 1):
                result = execute_trial(args, query, trial, session, wrapper)
                report["results"].append(result)
                write_json(args.output, report, 0o644)  # Durable before starting another service.
                print(json.dumps({"query": query["name"], "trial": trial, "state": result["state"]}), flush=True)
                if "interrupted" in result["errors"]:
                    interrupted = True
                    raise KeyboardInterrupt
                if any(code in result["errors"] for code in ("unit_stop_failed", "scratch_free_space_below_reserve", "insufficient_scratch_space_for_bounded_trial")):
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
        report["state"] = "interrupted" if interrupted else "completed"
        report["all_queries_completed"] = (not interrupted and "error" not in report and
            len(report["results"]) == len(queries) * args.trials and
            all(result["state"] == "completed" for result in report["results"]))
        if not report["all_queries_completed"] and not interrupted:
            report["state"] = "failed"
        write_json(args.output, report, 0o644)
    return 0 if report["all_queries_completed"] else 1


if __name__ == "__main__":
    sys.exit(main())

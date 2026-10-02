#!/usr/bin/env python3
"""Run the bounded HTTP companion on an already provisioned Linux micro VM.

Requires the adjacent federation_micro_benchmark.py, Python's standard library,
systemd/cgroup v2 and passwordless sudo. Credentials remain in private files.
The exact transient unit is stopped after --stop-file appears or 450 seconds.
"""
from __future__ import annotations

import argparse
import http.client
import json
import os
from pathlib import Path
import pwd
import re
import signal
import stat
import subprocess
import sys
import time

from federation_micro_benchmark import (
    BenchmarkError, MIB, Sampler, absolute_path, disk_usage, filesystem_type,
    free_bytes, hash_file, read_json, systemctl, unit_state,
    utc_now, write_json,
)


UNIT = re.compile(r"kelvo-micro-[a-z0-9][a-z0-9-]{0,80}\.service\Z")
WRAPPER = r'''import json, os, pathlib, re, stat, sys
try:
    os.umask(0o077)
    spec = json.loads(pathlib.Path(sys.argv[1]).read_text())
    for descriptor, name in ((1, "stdout.log"), (2, "stderr.log")):
        fd = os.open(str(pathlib.Path(spec["logs"]) / name), os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.dup2(fd, descriptor)
        os.close(fd)
    def private_json(name):
        path = pathlib.Path(name)
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 1048576:
            raise ValueError("private input")
        return json.loads(path.read_text())
    values = private_json(spec["environment"])
    if not isinstance(values, dict) or not 1 <= len(values) <= 64:
        raise ValueError("source environment")
    for key, value in values.items():
        if not re.fullmatch(r"KELVO_SOURCE_[A-Z0-9_]{1,100}", key) or not isinstance(value, str) or "\0" in value or len(value) > 65536:
            raise ValueError("source environment entry")
    token = private_json(spec["manifest"])["token"]
    if not isinstance(token, str) or not 32 <= len(token) <= 4096 or any(ord(c) < 33 or ord(c) > 126 for c in token):
        raise ValueError("token")
    environment = {"PATH": "/usr/local/bin:/usr/bin:/bin", "HOME": spec["home"],
                   "LANG": "C.UTF-8", "TMPDIR": spec["scratch"], "GOMAXPROCS": "2", "KELVO_TOKEN": token}
    environment.update(values)
    os.chdir(spec["logs"])
    os.execve(spec["binary"], spec["argv"], environment)
except BaseException as error:
    os.write(2, ("HTTP benchmark wrapper failed: " + type(error).__name__ + "\n").encode())
    sys.exit(125)
'''


class ServerSampler(Sampler):
    """Reuse cgroup sampling, adding serve to the coordinator role."""

    def __init__(self, unit, directory, binary, scratch):
        super().__init__(unit, directory, binary)
        self.scratch = scratch
        self.initial_cpu = None

    def sample(self):
        super().sample()
        counters = self.counters.get("cpu.stat")
        if counters and self.initial_cpu is None:
            self.initial_cpu = dict(counters)
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
                if len(argv) < 2 or os.fsdecode(argv[0]) != self.binary or argv[1] not in (b"serve", b"worker"):
                    continue
                role = "coordinator" if argv[1] == b"serve" else "worker"
                # Between fork and exec a child still exposes the parent's
                # argv; only the systemd-owned process is the HTTP server.
                if role == "coordinator" and int(fields[1]) != 1:
                    continue
                rss = max(0, int(fields[21])) * os.sysconf("SC_PAGE_SIZE")
                totals[role] += rss
                record = self.processes.setdefault((int(pid), int(fields[19])), {
                    "pid": int(pid), "role": role, "rss_bytes_peak": 0,
                    "user_ticks": 0, "system_ticks": 0, "no_new_privs_observed": None,
                    "seccomp_mode_observed": None, "seccomp_filters_observed": None})
                record["role"] = role
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
        for role, rss in totals.items():
            self.rss_peaks[role] = max(self.rss_peaks[role], rss)
        self.rss_peaks["combined"] = max(self.rss_peaks["combined"], sum(totals.values()))
        for key, value in disk_usage(self.scratch).items():
            self.scratch_peak[key] = max(self.scratch_peak[key], value)

    def stop(self):
        metrics = super().stop()
        last = metrics["cgroup_last_observed"].get("cpu.stat", {})
        metrics["cgroup_cpu_delta_from_first_sample"] = {
            key: value - self.initial_cpu.get(key, 0)
            for key, value in last.items()} if self.initial_cpu is not None else None
        metrics["coordinator_role"] = "kelvo serve"
        return metrics


def healthy(port):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
    try:
        connection.request("GET", "/health", headers={"Connection": "close"})
        response = connection.getresponse()
        return response.status == 200 and len(response.read(4097)) <= 4096
    except (OSError, http.client.HTTPException):
        return False
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("directory", "unit", "output", "stop-file", "manifest"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--port", type=int, default=18626)
    args = parser.parse_args()
    try:
        if sys.platform != "linux" or os.getuid() == 0 or not Path("/sys/fs/cgroup/cgroup.controllers").is_file():
            raise BenchmarkError("requires_nonroot_linux_and_cgroup_v2")
        if not UNIT.fullmatch(args.unit) or not 1024 <= args.port <= 65535:
            raise BenchmarkError("invalid_unit_or_port")
        root = absolute_path(args.directory, "directory", directory=True)
        binary = absolute_path(root / "bin/kelvo", "binary", executable=True)
        launcher = absolute_path(root / "bin/kelvo-landlock", "launcher", executable=True)
        config = absolute_path(root / "catalog.yml", "config")
        environment = absolute_path(root / "private/environment.json", "environment", private=True)
        runs = absolute_path(root / "runs", "runs", directory=True, private=True)
        manifest_path = absolute_path(args.manifest, "manifest", private=True)
        manifest = read_json(manifest_path)
        if not isinstance(manifest, dict) or set(manifest) != {"port", "token", "binary_sha256", "cases"}:
            raise BenchmarkError("invalid_manifest")
        token = manifest["token"]
        if (manifest["port"] != args.port or not isinstance(token, str) or not 32 <= len(token) <= 4096
                or any(ord(c) < 33 or ord(c) > 126 for c in token)):
            raise BenchmarkError("invalid_manifest_port_or_token")
        binary_hash = hash_file(binary)["sha256"]
        if manifest["binary_sha256"] != binary_hash:
            raise BenchmarkError("manifest_binary_digest_mismatch")
        del token, manifest
        output = Path(args.output)
        stop = Path(args.stop_file)
        for path in (output, stop):
            if not path.is_absolute() or path.exists() or path.is_symlink() or not path.parent.is_dir():
                raise BenchmarkError("output_and_stop_file_must_be_new_absolute_paths")
        absolute_path(stop.parent, "stop_file_parent", directory=True, private=True)
        if filesystem_type(runs) != "ext4":
            raise BenchmarkError("runs_must_be_on_ext4")
        if free_bytes(runs) < 640 * MIB:
            raise BenchmarkError("insufficient_disk_space")
    except BenchmarkError as error:
        parser.error(str(error))
    except (OSError, ValueError):
        parser.error("invalid_input_file_or_path")

    os.umask(0o077)
    logs = runs / ("http-" + args.unit.removesuffix(".service"))
    logs.mkdir(mode=0o700)
    scratch = runs / "http-scratch"
    scratch.mkdir(mode=0o700)
    wrapper = logs / "server-wrapper.py"
    wrapper.write_text(WRAPPER)
    spec = logs / "invocation.json"
    argv = [str(binary), "serve", "--sandbox", str(launcher), "--config", str(config),
            "--listen", "127.0.0.1:" + str(args.port), "--concurrency", "1", "--max-queries", "8",
            "--memory-mb", "128", "--threads", "2", "--temp-mb", "512", "--max-rows", "1000000",
            "--max-bytes", "33554432", "--timeout", "180s"]
    write_json(spec, {"binary": str(binary), "argv": argv, "manifest": str(manifest_path),
                     "environment": str(environment), "logs": str(logs), "scratch": str(scratch),
                     "home": pwd.getpwuid(os.getuid()).pw_dir})
    log = logs / "systemd.log"
    report = {"schema_version": 1, "started_at": utc_now(), "state": "starting", "unit": args.unit,
              "binary_sha256": binary_hash, "launcher_sha256": hash_file(launcher)["sha256"],
              "listen_host": "127.0.0.1", "port": args.port, "errors": [],
              "limits": {"memory_max_mib": 640, "memory_swap_max_bytes": 0, "tasks_max": 96,
                         "cpu_weight": 20, "concurrency": 1, "query_memory_mib": 128, "threads": 2,
                         "query_temp_mib": 512, "query_max_rows": 1000000, "query_max_bytes": 33554432,
                         "query_timeout_seconds": 180, "monitor_seconds": 450, "service_runtime_seconds": 600},
              "limitations": ["Resources cover the whole HTTP service interval, including idle time and readiness probes.",
                  "Process RSS and CPU are sampled every 100 ms and may miss brief peaks or workers.",
                  "Cgroup memory includes charged page cache and is separate from RSS.",
                  "Final counters precede forced service teardown; disappeared cgroups retain only last sampled counters.",
                  "NoNewPrivs and Seccomp flags alone do not prove the full Landlock policy."]}
    write_json(output, report, 0o644)
    owned, sampler, state = False, None, {}
    started = time.monotonic()
    previous = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        existing = systemctl(args.unit, ["show", "--property=LoadState", "--value"], log)
        if existing.stdout.strip() != b"not-found":
            raise BenchmarkError("unit_name_already_exists_or_unavailable")
        properties = ["User=" + pwd.getpwuid(os.getuid()).pw_name, "Slice=system.slice", "MemoryMax=640M",
                      "MemorySwapMax=0", "TasksMax=96", "CPUWeight=20", "OOMPolicy=kill", "RuntimeMaxSec=600",
                      "KillMode=control-group", "TimeoutStopSec=5", "RemainAfterExit=yes", "MemoryAccounting=yes",
                      "CPUAccounting=yes", "TasksAccounting=yes", "UMask=0077", "LimitCORE=0",
                      "StandardOutput=null", "StandardError=null", "WorkingDirectory=" + str(logs)]
        command = ["sudo", "-n", "systemd-run", "--quiet", "--unit=" + args.unit, "--service-type=exec"]
        command.extend("--property=" + value for value in properties)
        command.extend(["--", str(Path(sys.executable).resolve()), "-I", str(wrapper), str(spec)])
        sampler = ServerSampler(args.unit, logs, binary, scratch)
        sampler.start()
        owned = True
        with log.open("ab") as diagnostic:
            launched = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=diagnostic, stderr=diagnostic, timeout=15)
        if launched.returncode:
            raise BenchmarkError("service_start_failed")
        ready, next_state_check = False, 0.0
        while time.monotonic() - started < 450:
            if time.monotonic() >= next_state_check:
                state = unit_state(args.unit, log)
                next_state_check = time.monotonic() + 1
                if state.get("ControlGroup") != "/system.slice/" + args.unit:
                    raise BenchmarkError("service_cgroup_unavailable_or_changed")
                if any(state.get(key) != value for key, value in
                       {"MemoryMax": str(640 * MIB), "MemorySwapMax": "0", "TasksMax": "96", "CPUWeight": "20"}.items()):
                    raise BenchmarkError("service_limits_not_applied")
                if state.get("ActiveState") != "active" or state.get("SubState") != "running":
                    raise BenchmarkError("service_not_running")
            if not ready and healthy(args.port):
                ready = True
                report["state"], report["ready_at"] = "active", utc_now()
                write_json(output, report, 0o644)
                print(json.dumps({"state": "active", "unit": args.unit, "port": args.port}), flush=True)
            if stop.exists() or stop.is_symlink():
                info = stop.lstat()
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
                    raise BenchmarkError("invalid_stop_file")
                report["stop_reason"] = "stop_file"
                break
            if sampler.low_disk:
                raise BenchmarkError("scratch_free_space_below_reserve")
            if not ready and time.monotonic() - started > 20:
                raise BenchmarkError("readiness_deadline_exceeded")
            time.sleep(0.1)
        else:
            report["stop_reason"] = "monitor_deadline"
            report["errors"].append("monitor_deadline_reached")
    except KeyboardInterrupt:
        report["stop_reason"] = "interrupted"
        report["errors"].append("interrupted")
    except BenchmarkError as error:
        report["errors"].append(str(error))
    except subprocess.TimeoutExpired:
        report["errors"].append("systemd_command_timed_out")
    except (OSError, ValueError):
        report["errors"].append("server_companion_io_or_parse_error")
    finally:
        if owned:
            try:
                state = unit_state(args.unit, log)
            except (OSError, ValueError, BenchmarkError, subprocess.TimeoutExpired):
                report["errors"].append("final_unit_status_unavailable")
        if sampler is not None:
            try:
                report["metrics"] = sampler.stop()
            except (OSError, ValueError, BenchmarkError):
                report["errors"].append("final_metrics_unavailable")
        report["unit_before_stop"] = {key: state.get(key) for key in ("ActiveState", "SubState", "Result", "ExecMainCode", "ExecMainStatus")}
        report["systemd_accounting_before_stop"] = {key: int(state[key]) for key in
            ("MemoryPeak", "MemoryCurrent", "CPUUsageNSec") if state.get(key, "").isdigit() and int(state[key]) < (1 << 64) - 1}
        counters = report.get("metrics", {}).get("cgroup_last_observed", {}).get("memory.events", {})
        report["oom_observed"] = state.get("Result") == "oom-kill" or any(counters.get(key, 0) > 0 for key in ("oom_kill", "oom_group_kill"))
        report["cleanup"] = {}
        if owned:
            for action in ("stop", "reset-failed"):
                try:
                    result = systemctl(args.unit, [action], log, timeout=15)
                    report["cleanup"][action + "_exit_status"] = result.returncode
                    if result.returncode and action == "stop":
                        report["errors"].append("unit_stop_failed")
                except (OSError, subprocess.TimeoutExpired):
                    report["errors"].append("unit_" + action + "_failed")
            group = Path("/sys/fs/cgroup/system.slice") / args.unit
            try:
                try:
                    members = (group / "cgroup.procs").read_text().split()
                except FileNotFoundError:
                    members = []
                report["cleanup"]["cgroup_empty"] = not members
                report["cleanup"]["remaining_process_count"] = len(members)
                if members:
                    report["errors"].append("owned_service_processes_remain")
            except OSError:
                report["errors"].append("cgroup_cleanup_verification_failed")
        report["state"] = "failed" if report["errors"] else "completed"
        report["finished_at"], report["monitored_seconds"] = utc_now(), time.monotonic() - started
        signal.signal(signal.SIGTERM, previous)
        write_json(output, report, 0o644)
    return 0 if report["state"] == "completed" else 1


if __name__ == "__main__":
    sys.exit(main())

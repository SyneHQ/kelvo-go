#!/usr/bin/env python3
"""VM-only acceptance for a sandboxed DuckDB temporary-file spill.

This is deliberately a single, reproducible fixture rather than a benchmark.
It drives the worker protocol directly so its process directory is the only
write path admitted by kelvo-landlock, samples that directory while the query
runs, and compares its Arrow result with an unsandboxed control.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import threading
import time

import pyarrow.ipc as ipc


SQL = (
    "SELECT sum(id * rn)::VARCHAR AS checksum FROM "
    "(SELECT i AS id,row_number() OVER (ORDER BY hash(i)) AS rn "
    "FROM range(5000000) t(i))"
)
LIMITS = {
    "max_rows": 10,
    "max_bytes": 1 << 20,
    "timeout": 45_000_000_000,
    "memory_mb": 64,
    "threads": 1,
    "max_temp_mb": 1024,
}


def tree_size(path: Path) -> int:
    """Return allocated logical bytes below the dedicated worker directory."""
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += (Path(root) / name).stat().st_size
            except FileNotFoundError:
                # DuckDB may delete a temporary file between readdir and stat.
                pass
    return total


def sampled_rss_bytes(pid: int) -> int:
    """Read the current RSS from procfs; a vanished process contributes zero."""
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except (FileNotFoundError, IndexError, ValueError):
        pass
    return 0


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def worker_input() -> bytes:
    return json.dumps(
        {
            "config": {"sources": []},
            "limits": LIMITS,
            "request": {"mode": "federated", "sources": [], "sql": SQL},
        },
        separators=(",", ":"),
    ).encode()


def decode_arrow(data: bytes):
    eos = b"\xff\xff\xff\xff\x00\x00\x00\x00"
    if not data.endswith(eos):
        raise AssertionError("worker Arrow output did not end with EOS")
    with ipc.open_stream(io.BytesIO(data)) as reader:
        rows = reader.read_all().to_pylist()
    if len(rows) != 1 or set(rows[0]) != {"checksum"} or not isinstance(rows[0]["checksum"], str):
        raise AssertionError("fixture result did not contain one string checksum")
    return rows, len(data), True


def run_worker(binary: Path, launcher: Path | None, directory: Path):
    env = {"HOME": str(directory), "TMPDIR": str(directory), "GOMAXPROCS": "1"}
    command = [str(binary), "worker"]
    if launcher is not None:
        command = [str(launcher), "--write", str(directory), "--", str(binary), "worker"]
    high_water = 0
    peak_rss = 0
    stop = threading.Event()
    process = subprocess.Popen(
        command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        cwd=directory, env=env,
    )

    def sample():
        nonlocal high_water, peak_rss
        while not stop.is_set():
            high_water = max(high_water, tree_size(directory))
            peak_rss = max(peak_rss, sampled_rss_bytes(process.pid))
            stop.wait(0.01)

    # The same monitor is used for the control to make its comparison fields
    # meaningful. The 10ms RSS samples are observations, not a memory limit.
    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    started = time.monotonic()
    try:
        stdout, stderr = process.communicate(input=worker_input(), timeout=60)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        raise RuntimeError("worker process exceeded fixture timeout")
    finally:
        stop.set()
        sampler.join(timeout=2)
        high_water = max(high_water, tree_size(directory))
        peak_rss = max(peak_rss, sampled_rss_bytes(process.pid))
    elapsed = time.monotonic() - started
    if process.returncode != 0:
        raise RuntimeError("worker process failed")
    try:
        outcome = json.loads(stderr)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError("worker returned no structured outcome") from exc
    if outcome.get("error"):
        raise RuntimeError("worker reported an error: " + str(outcome["error"].get("code", "unknown")))
    rows, wire_bytes, eos = decode_arrow(stdout)
    return {"rows": rows, "elapsed_seconds": elapsed, "wire_bytes": wire_bytes,
            "eos": eos, "temp_high_water_bytes": high_water, "peak_rss_bytes": peak_rss}


def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, default=root / "bin" / "kelvo")
    parser.add_argument("--launcher", type=Path, default=root / "bin" / "kelvo-landlock")
    parser.add_argument("--output", type=Path, default=root / "docs" / "evidence" / "duckdb-spill.json")
    args = parser.parse_args()
    binary, launcher = args.binary.resolve(), args.launcher.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise SystemExit("--binary must be an executable worker binary")
    if not launcher.is_file() or not os.access(launcher, os.X_OK):
        raise SystemExit("--launcher must be an executable Linux Landlock launcher")

    with tempfile.TemporaryDirectory(prefix="kelvo-duckdb-spill-") as temporary:
        work = Path(temporary)
        sandbox_dir, control_dir = work / "sandbox", work / "control"
        sandbox_dir.mkdir(mode=0o700)
        control_dir.mkdir(mode=0o700)
        sandboxed = run_worker(binary, launcher, sandbox_dir)
        control = run_worker(binary, None, control_dir)
        if sandboxed["temp_high_water_bytes"] <= 0:
            raise AssertionError("sandboxed fixture produced no observable temporary files")
        if sandboxed["rows"] != control["rows"]:
            raise AssertionError("sandboxed checksum differs from unsandboxed control")
        result = {
            "fixture_only": True,
            "binary_sha256": sha256(binary),
            "launcher_sha256": sha256(launcher),
            "kernel_release": platform.release(),
            "query": SQL,
            "limits": LIMITS,
            "sampling": {
                "interval_ms": 10,
                "rss_source": "/proc/<worker-pid>/status:VmRSS",
                "caveat": "Sampled RSS is an observed peak, not a hard process memory cap.",
            },
            "sandboxed": {
                "arrow_eos": sandboxed["eos"],
                "checksum": sandboxed["rows"][0]["checksum"],
                "elapsed_seconds": sandboxed["elapsed_seconds"],
                "temp_high_water_bytes": sandboxed["temp_high_water_bytes"],
                "sampled_peak_rss_bytes": sandboxed["peak_rss_bytes"],
                "wire_bytes": sandboxed["wire_bytes"],
            },
            "unsandboxed_control": {
                "arrow_eos": control["eos"],
                "checksum": control["rows"][0]["checksum"],
                "elapsed_seconds": control["elapsed_seconds"],
                "temp_high_water_bytes": control["temp_high_water_bytes"],
                "sampled_peak_rss_bytes": control["peak_rss_bytes"],
                "wire_bytes": control["wire_bytes"],
            },
            "assertions": [
                "sandboxed worker completed with Arrow EOS",
                "dedicated sandbox worker directory had nonzero temporary-file high-water mark",
                "sandboxed checksum equals unsandboxed same-query control",
            ],
        }
    encoded = json.dumps(result, indent=2, sort_keys=True) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(encoded)
    print(encoded, end="")


if __name__ == "__main__":
    main()

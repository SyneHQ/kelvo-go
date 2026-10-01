#!/usr/bin/env python3
"""Measure one native ClickHouse-to-Arrow Kelvo export reproducibly.

The timed interval starts before the Kelvo coordinator is spawned and ends after
it exits and the resulting Arrow file is fsynced. Arrow validation is outside
that interval. The script never reads or prints source credentials; configure
source environment variables in the invoking environment.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import threading
import time
from typing import Any

DEFAULT_QUERY = """SELECT id, tenant_id, customer_id, event_time, amount, payload
FROM kelvo_bench.fact_events
WHERE tenant_id < 100
ORDER BY tenant_id, event_day, grp, id"""
SAMPLE_INTERVAL_SECONDS = 0.02


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True, help="Built kelvo binary")
    parser.add_argument("--catalog", type=Path, required=True, help="Public source catalog JSON")
    parser.add_argument("--connection", default="clickhouse", help="Registered ClickHouse source ID")
    parser.add_argument("--query", default=DEFAULT_QUERY, help="Native ClickHouse SQL")
    parser.add_argument("--output", type=Path, required=True, help="New Arrow IPC output path")
    parser.add_argument("--memory-mb", type=int, default=1024,
                        help="Native source + Arrow decoder budget in MiB")
    parser.add_argument("--max-bytes", type=int, default=1024 * 1024 * 1024)
    parser.add_argument("--max-rows", type=int, default=11_000_000)
    parser.add_argument("--timeout", default="120s")
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--cache-state", choices=("warm-uncontrolled", "cold-controlled"),
                        default="warm-uncontrolled")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--report", type=Path, help="Write the JSON report here too")
    args = parser.parse_args()
    if not sys.platform.startswith("linux"):
        parser.error("this helper requires Linux /proc RSS sampling")
    if args.memory_mb < 1 or args.max_bytes < 1 or args.max_rows < 1 or args.threads < 1:
        parser.error("numeric limits must be positive")
    return args


def read_rss_kib(pid: int) -> int | None:
    try:
        for line in (Path("/proc") / str(pid) / "status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except FileNotFoundError:
        return None
    return None


def children_of(pid: int) -> set[int]:
    children: set[int] = set()
    try:
        for path in (Path("/proc") / str(pid) / "task").glob("*/children"):
            children.update(int(value) for value in path.read_text().split())
    except FileNotFoundError:
        pass
    return children


class RSSSampler:
    """Separately tracks coordinator and every observed direct worker child."""

    def __init__(self, coordinator_pid: int) -> None:
        self.coordinator_pid = coordinator_pid
        self.stop_event = threading.Event()
        self.samples = 0
        self.coordinator_peak_kib: int | None = None
        self.worker_peak_kib: dict[int, int] = {}
        self.thread = threading.Thread(target=self.run, daemon=True)

    def observe(self, pid: int) -> None:
        value = read_rss_kib(pid)
        if value is None:
            return
        if pid == self.coordinator_pid:
            if self.coordinator_peak_kib is None or value > self.coordinator_peak_kib:
                self.coordinator_peak_kib = value
        else:
            self.worker_peak_kib[pid] = max(self.worker_peak_kib.get(pid, 0), value)

    def run(self) -> None:
        while not self.stop_event.is_set():
            self.observe(self.coordinator_pid)
            for worker_pid in children_of(self.coordinator_pid):
                self.observe(worker_pid)
            self.samples += 1
            self.stop_event.wait(SAMPLE_INTERVAL_SECONDS)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> dict[str, Any]:
        self.stop_event.set()
        self.thread.join()
        return {
            "method": "Linux /proc/<pid>/status VmRSS; coordinator and direct worker children sampled separately",
            "sample_interval_seconds": SAMPLE_INTERVAL_SECONDS,
            "samples": self.samples,
            "coordinator_peak_rss_kib": self.coordinator_peak_kib,
            "worker_peak_rss_kib_by_pid": self.worker_peak_kib,
            "worker_peak_rss_kib_max": max(self.worker_peak_kib.values(), default=None),
        }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def default_fixture_expectations() -> dict[str, int]:
    # The default query takes tenant IDs 0..99 from each 1,000-row block.
    source_rows = 100_000_000
    rows = 10_000_000
    selected_per_block = 100
    blocks = source_rows // 1000
    id_sum = sum(1000 * block * selected_per_block + (selected_per_block * 99 // 2)
                 for block in range(blocks))

    def nulls(modulus: int) -> int:
        return sum(((1000 * block + 99) // modulus) - ((1000 * block - 1) // modulus)
                   for block in range(blocks))

    return {"rows": rows, "id_sum": id_sum, "amount_nulls": nulls(23), "payload_nulls": nulls(17)}


def validate_arrow(path: Path, expected: dict[str, int] | None) -> dict[str, Any]:
    """Read each batch without retaining result data; called outside the timer."""
    try:
        import pyarrow.compute as compute
        import pyarrow.ipc as ipc
    except ImportError as error:
        raise RuntimeError("validation requires pyarrow; install it in the benchmark environment") from error

    checksum = hashlib.sha256()
    rows = 0
    nulls: dict[str, int] = {}
    sums: dict[str, int | float] = {}
    with path.open("rb") as source, ipc.open_stream(source) as reader:
        schema = [(field.name, str(field.type), field.nullable) for field in reader.schema]
        expected_schema = [("id", "int64"), ("tenant_id", "int32"),
                           ("customer_id", "int32"), ("event_time", "uint32"),
                           ("amount", "double"), ("payload", "string")]
        if expected is not None and [(name, typ) for name, typ, _ in schema] != expected_schema:
            raise RuntimeError(f"unexpected default transfer schema: {schema}")
        checksum.update(str(schema).encode())
        for batch in reader:
            rows += batch.num_rows
            if expected is not None:
                # ClickHouse ArrowStream maps DateTime to UInt32 epoch seconds.
                # Check the fixture's exact generator relation without retaining
                # the batch beyond this validation iteration.
                ids = batch.column(batch.schema.get_field_index("id"))
                event_times = batch.column(batch.schema.get_field_index("event_time"))
                # Integer Arrow division truncates; this computes the positive
                # fixture IDs' remainder without an optional NumPy dependency.
                remainder = compute.subtract(ids, compute.multiply(
                    compute.divide(ids, 63_072_000), 63_072_000))
                expected_times = compute.add(remainder, 1_704_067_200)
                if len(ids) and not compute.all(compute.equal(event_times, expected_times)).as_py():
                    raise RuntimeError("event_time fixture formula validation failed")
            for field, column in zip(reader.schema, batch.columns):
                nulls[field.name] = nulls.get(field.name, 0) + column.null_count
                if field.name == "id":
                    value = compute.sum(column).as_py()
                    sums["id"] = sums.get("id", 0) + (int(value) if value is not None else 0)
                if expected is not None and field.name == "payload":
                    present = compute.filter(column, compute.invert(compute.is_null(column)))
                    if len(present) and not compute.all(compute.match_substring_regex(
                            present, r"^event-[0-9]+-c[0-9]+-[0-9A-F]{2,12}$")).as_py():
                        raise RuntimeError("payload format validation failed")
                for buffer in column.buffers():
                    if buffer is not None:
                        checksum.update(memoryview(buffer))
    observed = {"rows": rows, "id_sum": sums.get("id"),
                "amount_nulls": nulls.get("amount"), "payload_nulls": nulls.get("payload")}
    if expected is not None and observed != expected:
        raise RuntimeError(f"default fixture validation failed: observed={observed}, expected={expected}")
    return {
        "row_count": rows,
        "value_sums": sums,
        "default_fixture_expected": expected,
        "schema": schema,
        "null_counts": nulls,
        "arrow_buffer_sha256": checksum.hexdigest(),
        "arrow_file_sha256": sha256_file(path),
        "arrow_file_bytes": path.stat().st_size,
    }


def parse_stats(stderr: str) -> dict[str, Any] | None:
    # Kelvo emits one JSON stats record on stderr. Do not include arbitrary
    # stderr in the report because a future dependency could echo credentials.
    for line in reversed(stderr.splitlines()):
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    return None


def main() -> int:
    args = arguments()
    binary = args.binary.resolve()
    catalog = args.catalog.resolve()
    output = args.output.resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("--binary must name an executable file")
    if not catalog.is_file():
        raise RuntimeError("--catalog must name a file")
    if output.exists() and not args.overwrite:
        raise RuntimeError("refusing to overwrite --output; pass --overwrite explicitly")
    output.parent.mkdir(parents=True, exist_ok=True)

    command = [str(binary), "query", "--config", str(catalog), "--mode", "native",
               "--connection", args.connection, "--sql", args.query, "--out", str(output),
               "--memory-mb", str(args.memory_mb), "--max-bytes", str(args.max_bytes),
               "--max-rows", str(args.max_rows), "--timeout", args.timeout,
               "--threads", str(args.threads)]
    started = time.perf_counter()
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                               stderr=subprocess.PIPE, text=True)
    sampler = RSSSampler(process.pid)
    sampler.start()
    stderr = process.communicate()[1]
    if process.returncode == 0:
        # Kelvo syncs before it exits; this explicit fsync includes the caller's
        # final output durability boundary in the measured interval as well.
        with output.open("rb") as result:
            os.fsync(result.fileno())
    elapsed_seconds = time.perf_counter() - started
    rss = sampler.stop()
    report = {
        "benchmark": "kelvo-clickhouse-native-arrow-v1",
        "scope": "Kelvo coordinator process, isolated worker, native ClickHouse query, Arrow IPC export, and caller fsync; validation is outside the timed interval.",
        "binary_sha256": sha256_file(binary),
        "connection_id": args.connection,
        "query": args.query,
        "cache_state": args.cache_state,
        "cache_note": "Warm/uncontrolled does not establish a cold-cache measurement.",
        "source_memory_flag": "--memory-mb",
        "source_memory_mib": args.memory_mb,
        "source_memory_note": "In native mode this budget covers the ClickHouse source query and Arrow decoder allocations; it is not a process-RSS cap.",
        "command_exit": process.returncode,
        "elapsed_seconds_process_plus_fsync": elapsed_seconds,
        "rss": rss,
        "platform": {"system": platform.platform(), "python": sys.version},
    }
    if process.returncode != 0:
        # Avoid emitting arbitrary stderr: it can contain a future driver's
        # unredacted source details. The numeric exit and generic public error
        # keep the failure report safe to preserve.
        report["error"] = "Kelvo query failed"
        rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
        print(rendered, end="")
        if args.report:
            args.report.write_text(rendered)
        return 1

    expected = default_fixture_expectations() if args.query == DEFAULT_QUERY else None
    try:
        validation = validate_arrow(output, expected)
    except Exception:
        # The export timing and RSS observation remain useful evidence even if
        # post-export correctness validation rejects the resulting file. Do not
        # serialize arbitrary exception text because an external decoder may
        # include source details in it.
        report["phase"] = "validation"
        report["error"] = "Arrow validation failed"
        report["kelvo_stats"] = parse_stats(stderr)
        rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
        print(rendered, end="")
        if args.report:
            args.report.write_text(rendered)
        return 1
    report["kelvo_stats"] = parse_stats(stderr)
    report["validation_outside_timer"] = validation
    report["throughput"] = {
        "rows_per_second": validation["row_count"] / elapsed_seconds,
        "arrow_file_bytes_per_second": validation["arrow_file_bytes"] / elapsed_seconds,
    }
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    print(rendered, end="")
    if args.report:
        args.report.write_text(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

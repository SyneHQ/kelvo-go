#!/usr/bin/env python3
"""Benchmark the existing ClickHouse fixture without creating or changing tables.

Run on the test VM with an already built Kelvo binary and pyarrow installed.
KELVO_SOURCE_BENCH_URL is required; KELVO_SOURCE_BENCH_USER and
KELVO_SOURCE_BENCH_PASSWORD are optional. Credentials remain in the environment.
Each invocation creates a private, retained artifact directory and replaces only
the sanitized docs/evidence/acceleration-clickhouse.json evidence report.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import statistics
import subprocess
import sys
import time
import traceback
import uuid


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/evidence/acceleration-clickhouse.json"
URL_ENV = "KELVO_SOURCE_BENCH_URL"
REFRESH_BYTES = 1 << 30
MEMORY_MB = 512
THREADS = 2
TIMEOUT_SECONDS = 300
GNU_TIME_FORMAT = '{"elapsed_seconds":%e,"max_rss_kib":%M,"exit_status":%x}'
GROUP_COLUMNS = ["grp", "row_count", "id_sum", "min_id", "max_id"]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def private_directory(path):
    require(not path.is_symlink(), "private directory cannot be a symlink")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    require(stat.S_IMODE(path.stat().st_mode) == 0o700,
            "private directory must have mode 0700")
    return path


def private_write(path, text):
    with path.open("x", encoding="utf-8") as output:
        output.write(text)
    path.chmod(0o600)


def file_digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def catalog_yaml(rows, env):
    # A static YAML template needs no optional YAML package and contains only
    # environment-variable references, never the referenced values.
    auth = ""
    if "KELVO_SOURCE_BENCH_USER" in env:
        auth += "    username_env: KELVO_SOURCE_BENCH_USER\n"
    if "KELVO_SOURCE_BENCH_PASSWORD" in env:
        auth += "    password_env: KELVO_SOURCE_BENCH_PASSWORD\n"
    return f"""sources:
  - id: bench
    type: clickhouse
    url_env: KELVO_SOURCE_BENCH_URL
{auth}acceleration:
  directory: snapshots
  tenant_id: benchmark
  datasets:
    - id: events_fast
      query:
        mode: native
        connection_id: bench
        sql: >-
          SELECT id, tenant_id, grp, amount
          FROM kelvo_bench.fact_events
          WHERE id >= 0 AND id < {rows}
      refresh_interval: 0s
      max_age: 1h
      authorization_version: benchmark-v1
      limits:
        max_rows: {rows}
        max_bytes: {REFRESH_BYTES}
        timeout: 5m
        memory_mb: {MEMORY_MB}
        threads: {THREADS}
        max_temp_mb: 1024
"""


def parse_snapshot(path):
    # Read only fixed public scalar fields from the CLI's YAML. In particular,
    # snapshot paths and any diagnostic text never enter published evidence.
    text = path.read_text(encoding="utf-8")
    result = {}
    for key in ("ready", "rows", "bytes"):
        values = re.findall(r"^\s*" + key + r":\s*([^\n]+)$", text, re.MULTILINE)
        require(len(values) == 1, "refresh status is missing an expected scalar")
        raw = values[0].strip()
        if key == "ready":
            require(raw in ("true", "false"), "invalid ready status")
            result[key] = raw == "true"
        else:
            require(raw.isdecimal(), "invalid numeric snapshot status")
            result[key] = int(raw)
    return result


def timed_cli(binary, directory, label, arguments, env, expected_success=True,
              timeout=TIMEOUT_SECONDS + 30):
    stdout_path = directory / (label + ".stdout")
    stderr_path = directory / (label + ".stderr")
    timing_path = directory / (label + ".time.json")
    command = ["/usr/bin/time", "-f", GNU_TIME_FORMAT, "-o", str(timing_path),
               str(binary), *map(str, arguments)]
    started = time.perf_counter()
    with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
        process = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                   stdout=stdout, stderr=stderr, start_new_session=True)
        def deadline(_signum, _frame):
            raise subprocess.TimeoutExpired(command, timeout)

        # Popen.wait(timeout=...) polls with sleeps up to 50ms on POSIX, which
        # distorts these short query timings. A signal deadline permits a
        # blocking waitpid with immediate notification of process completion.
        previous_alarm = signal.signal(signal.SIGALRM, deadline)
        signal.setitimer(signal.ITIMER_REAL, timeout)
        try:
            return_code = process.wait()
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            # Signal only the session created for this invocation, including
            # its worker, so a timed-out GNU time process cannot orphan a CLI.
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_alarm)
    wall_seconds = time.perf_counter() - started
    timing = None
    for line in timing_path.read_text(encoding="utf-8").splitlines():
        try:
            candidate = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict) and set(candidate) == {
                "elapsed_seconds", "max_rss_kib", "exit_status"}:
            timing = candidate
    require(timing is not None, "GNU time did not produce timing metadata")
    require((return_code == 0) == expected_success, "CLI returned an unexpected exit status")
    require(timing["max_rss_kib"] >= 0 and timing["elapsed_seconds"] >= 0,
            "invalid GNU time result")
    return {
        "wall_seconds": wall_seconds,
        "gnu_time_elapsed_seconds": timing["elapsed_seconds"],
        "gnu_time_max_rss_kib": timing["max_rss_kib"],
        "exit_status": return_code,
    }


def aggregation_sql(source, rows):
    return ("SELECT grp, count(*) AS row_count, sum(id) AS id_sum, "
            "min(id) AS min_id, max(id) AS max_id FROM " + source +
            f" WHERE id >= 0 AND id < {rows} GROUP BY grp ORDER BY grp")


def query_args(catalog, output, mode, rows, timeout="5m"):
    native = mode == "native"
    args = ["query", "--config", catalog, "--mode", "native" if native else "federated",
            "--connection" if native else "--sources", "bench" if native else "events_fast",
            "--sql", aggregation_sql("kelvo_bench.fact_events" if native else "events_fast", rows),
            "--out", output, "--max-rows", str(min(rows, 100_000)),
            "--max-bytes", str(32 << 20), "--timeout", timeout,
            "--memory-mb", str(MEMORY_MB), "--threads", str(THREADS), "--temp-mb", "1024"]
    return args


def read_groups(path, selected_rows):
    import pyarrow.ipc as ipc

    with path.open("rb") as source, ipc.open_stream(source) as reader:
        table = reader.read_all()
    require(table.column_names == GROUP_COLUMNS, "aggregate output columns changed")
    require(table.num_rows <= min(selected_rows, 100_000), "aggregate result exceeds validation bound")
    result = table.to_pylist()
    require(result, "aggregate result unexpectedly empty")
    for row in result:
        for key, value in row.items():
            # DuckDB's SUM(BIGINT) may arrive as decimal128(38,0), whereas
            # ClickHouse uses int64. Compare exact integers, never float casts.
            require(isinstance(value, (int, Decimal)) and not isinstance(value, bool),
                    "aggregate output is not an exact integer")
            require(value == int(value), "aggregate output has a fractional value")
            row[key] = int(value)
        require(row["row_count"] > 0, "aggregate count must be positive")
    require(all(left["grp"] < right["grp"] for left, right in zip(result, result[1:])),
            "aggregate groups are not strictly ordered")
    require(sum(row["row_count"] for row in result) == selected_rows,
            "selected fixture row count differs from requested rows")
    require(sum(row["id_sum"] for row in result) == selected_rows * (selected_rows - 1) // 2,
            "selected fixture ID sum is incorrect")
    require(min(row["min_id"] for row in result) == 0 and
            max(row["max_id"] for row in result) == selected_rows - 1,
            "selected fixture ID bounds are incorrect")
    return result


def write_evidence(report):
    EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
    temporary = EVIDENCE.with_name(EVIDENCE.name + ".tmp-" + uuid.uuid4().hex)
    try:
        with temporary.open("x", encoding="utf-8") as output:
            json.dump(report, output, indent=2, sort_keys=True, allow_nan=False)
            output.write("\n")
        temporary.chmod(0o644)
        os.replace(temporary, EVIDENCE)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "bin/kelvo")
    parser.add_argument("--rows", type=int, default=10_000_000,
                        help="Fixture prefix size, 1..100000000; fixed byte/memory limits still apply")
    args = parser.parse_args()
    if not 1 <= args.rows <= 100_000_000:
        parser.error("--rows must be between 1 and 100000000")
    os.umask(0o077)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    directory = private_directory(private_directory(ROOT / "artifacts/acceleration-benchmark") /
                                  (stamp + "-" + uuid.uuid4().hex[:8]))
    report = {
        "benchmark": "kelvo-clickhouse-acceleration-v1",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "passed": False,
        "selected_source_rows": args.rows,
        "source_fixture": "existing kelvo_bench.fact_events; no source DDL or DML",
        "refresh_limits": {"max_rows": args.rows, "max_bytes": REFRESH_BYTES,
                           "timeout_seconds": TIMEOUT_SECONDS, "memory_mb": MEMORY_MB,
                           "threads": THREADS, "max_temp_mb": 1024},
        "query_limits": {"max_rows": min(args.rows, 100_000), "max_bytes": 32 << 20,
                         "timeout_seconds": TIMEOUT_SECONDS, "memory_mb": MEMORY_MB,
                         "threads": THREADS, "max_temp_mb": 1024},
        "trials": [],
        "notes": [
            "Wall timing includes CLI startup, worker execution and durable CLI output; Arrow validation is outside the timed interval.",
            "GNU time max RSS is its reported maximum resident set size for the command and waited-for children; it is not a process-tree summed peak and excludes remote ClickHouse memory.",
            "Memory flags are engine/client budgets, not process-RSS caps.",
            "No caches are flushed. Refresh precedes all trials; warming is included in the individually reported alternating trials. No separate warmup trials are discarded.",
            "Aggregation returns grouped results over the selected subset. These timings do not measure a million-row query-result transfer or prove physical rows scanned.",
            "This fixture and host comparison does not establish that acceleration is always faster; refresh cost is reported separately.",
        ],
    }
    stage = "prerequisites"
    try:
        require(sys.platform.startswith("linux") and Path("/usr/bin/time").is_file(),
                "benchmark requires Linux and GNU time")
        binary = args.binary.resolve()
        require(binary.is_file() and os.access(binary, os.X_OK), "built Kelvo binary is required")
        env = dict(os.environ)
        require(bool(env.get(URL_ENV)), "source URL environment variable is required")
        import pyarrow  # noqa: F401: check before any source query or refresh.

        report["binary_sha256"] = file_digest(binary)
        catalog = directory / "catalog.yaml"
        private_write(catalog, catalog_yaml(args.rows, env))
        catalog_digest = file_digest(catalog)
        stage = "refresh"
        print("Refreshing selected fixture rows into a private Parquet snapshot", flush=True)
        report["refresh"] = timed_cli(binary, directory, "refresh",
            ["accelerate", "refresh", "--config", catalog, "--dataset", "events_fast"], env)
        snapshot = parse_snapshot(directory / "refresh.stdout")
        require(snapshot["ready"] and snapshot["rows"] == args.rows and snapshot["bytes"] > 0,
                "refreshed snapshot has incorrect readiness or row count")
        report["snapshot"] = {"rows": snapshot["rows"], "parquet_bytes": snapshot["bytes"]}

        reference = None
        for trial in range(1, 4):
            stage = "trial-" + str(trial)
            order = ["native", "accelerated"] if trial % 2 else ["accelerated", "native"]
            measurements = {}
            for mode in order:
                print(f"Trial {trial}: {mode} grouped query (cache state uncontrolled)", flush=True)
                label = f"trial-{trial}-{mode}"
                output = directory / (label + ".arrow")
                measured = timed_cli(binary, directory, label,
                                    query_args(catalog, output, mode, args.rows), env)
                values = read_groups(output, args.rows)
                if reference is None:
                    reference = values
                require(values == reference, "native and accelerated aggregate values differ")
                measured["output_groups"] = len(values)
                measured["arrow_output_bytes"] = output.stat().st_size
                measured["exact_result_equal"] = True
                measurements[mode] = measured
            report["trials"].append({"trial": trial, "order": order,
                                     "cache_state": "uncontrolled; warming included",
                                     "measurements": measurements})

        stage = "source-unavailable-proof"
        unavailable_env = dict(env)
        unavailable_env[URL_ENV] = "http://127.0.0.1:1"
        negative_output = directory / "unavailable-native.arrow"
        negative = timed_cli(binary, directory, "unavailable-native",
            query_args(catalog, negative_output, "native", args.rows, timeout="5s"),
            unavailable_env, expected_success=False, timeout=20)
        offline_output = directory / "unavailable-accelerated.arrow"
        offline = timed_cli(binary, directory, "unavailable-accelerated",
            query_args(catalog, offline_output, "accelerated", args.rows), unavailable_env)
        require(read_groups(offline_output, args.rows) == reference,
                "accelerated result changed with source unavailable")
        require(file_digest(catalog) == catalog_digest, "catalog changed during benchmark")
        report["source_unavailable_proof"] = {
            "catalog_definition_unchanged": True,
            "source_endpoint_overridden_only_in_child_environment": True,
            "native_negative_control": negative,
            "accelerated_query": offline,
            "exact_result_equal": True,
            "conclusion": "The accelerated query succeeded with the registered source endpoint unavailable.",
        }
        report["results"] = {
            "groups": len(reference), "selected_rows": args.rows,
            "id_sum": args.rows * (args.rows - 1) // 2,
            "min_id": 0, "max_id": args.rows - 1,
            "group_values_sha256": hashlib.sha256(json.dumps(reference, sort_keys=True,
                separators=(",", ":")).encode()).hexdigest(),
            "all_six_trial_results_exactly_equal": True,
        }
        report["median_wall_seconds"] = {
            mode: statistics.median(trial["measurements"][mode]["wall_seconds"]
                                    for trial in report["trials"])
            for mode in ("native", "accelerated")
        }
        report["passed"] = True
        print("Benchmark comparisons and unavailable-source proof passed", flush=True)
    except Exception:
        private_write(directory / "failure.txt", traceback.format_exc())
        report["failed_stage"] = stage
        report["error"] = "Benchmark failed; private run diagnostics were retained."
        print("Benchmark failed; private run diagnostics were retained", file=sys.stderr)
    finally:
        write_evidence(report)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

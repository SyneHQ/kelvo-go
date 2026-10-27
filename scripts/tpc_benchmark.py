#!/usr/bin/env python3
"""TPC-H / TPC-DS scale benchmark: Kelvo vs DuckDB (Python) vs single-node Trino.

Subcommands (all Linux-only except `compare` helpers used by the unit tests):
  generate     DuckDB dbgen/dsdgen -> one Parquet file per table + dataset manifest
  export-sql   write DuckDB's bundled TPC-H/TPC-DS query texts to benchmarks/tpc/
  run          per-query trials for kelvo / kelvo_accel / duckdb / trino
  driver       internal: the DuckDB Python child process measured by `run`
  concurrency  N concurrent TPC-H streams through `kelvo serve`

Every timed Kelvo/DuckDB trial is a fresh process inside its own transient
systemd scope (MemoryMax, zero swap). Trino runs as one long-lived container
with the same memory/CPU caps; its JVM startup is excluded and recorded as
such. Each answer is compared to the DuckDB Python answer (see `compare`).
Failures, mismatches and timeouts are recorded, never dropped.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import decimal
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import secrets
import shutil
import statistics
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True
REPO = Path(__file__).resolve().parent.parent
SUITES = {"tpch": ("dbgen", "tpch_queries", 22), "tpcds": ("dsdgen", "tpcds_queries", 99)}
MIB = 1 << 20


# ---------------------------------------------------------------- comparison
def _norm(value, scale):
    """Canonical scalar. `scale` is the decimal scale of the column, if any."""
    if isinstance(value, decimal.Decimal):
        return ("num", float(value), scale)
    if isinstance(value, float):
        return ("nan",) if math.isnan(value) else ("num", value, None)
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return ("v", value)
    if isinstance(value, int):
        return ("num", value, 0) if abs(value) < 2**53 else ("v", value)
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return ("v", value.isoformat())
    return ("v", str(value))


def _equal(a, b):
    if a[0] != "num" or b[0] != "num":
        return a == b
    x, y = a[1], b[1]
    if a[2] == 0 and b[2] == 0:
        return x == y  # both integers: exact
    # Decimal vs decimal/float: allow half a unit of the coarser declared scale
    # (Trino rounds AVG(decimal) to the input scale, DuckDB returns DOUBLE).
    scales = [s for s in (a[2], b[2]) if s is not None and s > 0]
    absolute = 0.5 * 10 ** -min(scales) if scales else 0.0
    return abs(x - y) <= max(absolute, 1e-9 * max(1.0, abs(x), abs(y)))


def canonical_rows(columns):
    """columns: list of (scale_or_None, [python values]) -> list of row tuples."""
    if not columns:
        return []
    return [tuple(_norm(v, s) for v, (s, _) in zip(row, columns)) for row in zip(*(c[1] for c in columns))]


def _sort_key(row):
    return tuple((c[0], repr(c[1] if c[0] != "num" else round(c[1], 4))) for c in row)


def compare(reference, observed):
    """Return (verdict, detail). Verdicts: exact_order, matched_unordered, mismatch."""
    if len(reference) != len(observed):
        return "mismatch", f"row_count reference={len(reference)} observed={len(observed)}"
    if reference and len(reference[0]) != len(observed[0]):
        return "mismatch", f"column_count reference={len(reference[0])} observed={len(observed[0])}"

    def first_diff(ref, obs):
        for i, (r, o) in enumerate(zip(ref, obs)):
            for j, (x, y) in enumerate(zip(r, o)):
                if not _equal(x, y):
                    return f"row {i} col {j}: reference={x!r} observed={y!r}"
        return None

    diff = first_diff(reference, observed)
    if diff is None:
        return "exact_order", ""
    # Ties under ORDER BY may legally reorder; never accepted for row-count changes.
    if first_diff(sorted(reference, key=_sort_key), sorted(observed, key=_sort_key)) is None:
        return "matched_unordered", diff
    return "mismatch", diff


def arrow_columns(table):
    import pyarrow as pa
    out = []
    for field, column in zip(table.schema, table.columns):
        scale = field.type.scale if pa.types.is_decimal(field.type) else (0 if pa.types.is_integer(field.type) else None)
        out.append((scale, column.to_pylist()))
    return out


def read_arrow(path):
    import pyarrow as pa
    with pa.memory_map(str(path)) as source:
        try:
            return pa.ipc.open_file(source).read_all()
        except pa.ArrowInvalid:
            source.seek(0)
            return pa.ipc.open_stream(source).read_all()


# ---------------------------------------------------------------- helpers
def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(8 * MIB), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(json.dumps(value, indent=1, sort_keys=True, default=str) + "\n")
    os.replace(tmp, path)


def tables_in(data):
    return sorted(p.stem for p in Path(data).glob("*.parquet"))


def host_facts():
    facts = {"kernel": platform.release(), "machine": platform.machine(), "python": platform.python_version(),
             "nproc": os.cpu_count(), "label": "shared GitHub-hosted cloud VM (noisy neighbours possible)"}
    try:
        facts["cpu_model"] = next(l.split(":", 1)[1].strip() for l in open("/proc/cpuinfo") if l.startswith("model name"))
        facts["mem_total_kib"] = int(next(l.split()[1] for l in open("/proc/meminfo") if l.startswith("MemTotal")))
    except (OSError, StopIteration):
        pass
    for name in ("duckdb", "pyarrow", "trino"):
        try:
            facts[name] = __import__(name).__version__
        except Exception:  # noqa: BLE001 - optional dependency
            facts[name] = None
    return facts


def summary_stats(values):
    if not values:
        return None
    return {"median": statistics.median(values), "min": min(values), "max": max(values), "n": len(values)}


def percentile(values, p):
    ordered = sorted(values)
    if not ordered:
        return None
    k = (len(ordered) - 1) * p / 100
    lo, hi = math.floor(k), math.ceil(k)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (k - lo)


class Sampler(threading.Thread):
    """Polls a cgroup (memory.current/peak, sum of member RSS) and a spill directory."""

    def __init__(self, cgroup, spill_dir, interval=0.1, use_peak=True):
        super().__init__(daemon=True)
        self.use_peak = use_peak  # memory.peak is cgroup-lifetime: only valid for a per-trial scope
        self.cgroup, self.spill_dir, self.interval = cgroup, spill_dir, interval
        self.stop_event = threading.Event()
        self.rss_peak = self.cgroup_peak = self.spill_peak = 0
        self.samples = 0
        self.oom_kills = self.memory_max_events = 0

    def _read(self, name):
        try:
            return int((self.cgroup / name).read_text().split()[0])
        except (OSError, ValueError, IndexError):
            return 0

    def _rss(self):
        total = 0
        try:
            pids = (self.cgroup / "cgroup.procs").read_text().split()
        except OSError:
            return 0
        for pid in pids:
            try:
                for line in open(f"/proc/{pid}/status"):
                    if line.startswith("VmRSS:"):
                        total += int(line.split()[1]) * 1024
            except OSError:
                pass
        return total

    def _spill(self):
        total = 0
        if self.spill_dir:
            for root, _, files in os.walk(self.spill_dir):
                for name in files:
                    try:
                        total += os.lstat(os.path.join(root, name)).st_size
                    except OSError:
                        pass
        return total

    def sample(self):
        self.samples += 1
        self.rss_peak = max(self.rss_peak, self._rss())
        self.cgroup_peak = max(self.cgroup_peak, self._read("memory.peak") if self.use_peak else 0, self._read("memory.current"))
        self.spill_peak = max(self.spill_peak, self._spill())
        try:
            events = dict(line.split() for line in (self.cgroup / "memory.events").read_text().splitlines())
            self.oom_kills = max(self.oom_kills, int(events.get("oom_kill", 0)))
            self.memory_max_events = max(self.memory_max_events, int(events.get("max", 0)))
        except (OSError, ValueError):
            pass

    def run(self):
        while not self.stop_event.wait(self.interval):
            self.sample()


def run_contained(argv, workdir, memory_gib, cpus, timeout, env=None):
    """Run argv in a fresh systemd scope; return measurement dict."""
    unit = "kelvo-tpc-" + secrets.token_hex(6)
    spill = workdir / "tmp"
    spill.mkdir(parents=True, exist_ok=True)
    command = ["sudo", "-n", "systemd-run", "--scope", "--quiet", "--unit=" + unit,
               f"--uid={os.getuid()}", f"--gid={os.getgid()}",
               f"--property=MemoryMax={memory_gib}G", "--property=MemorySwapMax=0",
               f"--property=CPUQuota={cpus * 100}%", "--setenv=TMPDIR=" + str(spill)]
    for key, value in (env or {}).items():
        command.append(f"--setenv={key}={value}")
    command += ["--", *argv]
    cgroup = Path(f"/sys/fs/cgroup/system.slice/{unit}.scope")
    stdout, stderr = open(workdir / "stdout.log", "wb"), open(workdir / "stderr.log", "wb")
    started = time.monotonic()
    process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
    sampler = Sampler(cgroup, spill)
    sampler.start()
    timed_out = False
    try:
        returncode = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        subprocess.run(["sudo", "-n", "systemctl", "kill", "--signal=KILL", unit + ".scope"], check=False)
        returncode = process.wait()
    elapsed = time.monotonic() - started
    sampler.sample()
    sampler.stop_event.set()
    sampler.join()
    stdout.close(), stderr.close()
    shutil.rmtree(spill, ignore_errors=True)
    return {"returncode": returncode, "timed_out": timed_out, "process_seconds": elapsed,
            "peak_sampled_tree_rss_bytes": sampler.rss_peak, "peak_cgroup_memory_bytes": sampler.cgroup_peak,
            "peak_spill_bytes": sampler.spill_peak, "samples": sampler.samples,
            "oom_kill_events": sampler.oom_kills, "memory_max_events": sampler.memory_max_events,
            "note": "events sampled while the scope existed; a kill after the last sample may be missed"}


# ---------------------------------------------------------------- generate / export-sql
def cmd_generate(args):
    import duckdb
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    generator = SUITES[args.suite][0]
    db = out / "generate.duckdb"
    con = duckdb.connect(str(db))
    con.execute(f"SET memory_limit='{args.memory_gib}GiB'; SET threads={args.threads}; SET temp_directory='{out}/gen-tmp'")
    con.execute(f"INSTALL {args.suite}; LOAD {args.suite}")
    started = time.monotonic()
    con.execute(f"CALL {generator}(sf={args.sf})")
    tables = {}
    for (name,) in con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY 1").fetchall():
        path = out / f"{name}.parquet"
        con.execute(f"COPY {name} TO '{path}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 122880)")
        rows = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        tables[name] = {"rows": rows, "bytes": path.stat().st_size, "sha256": sha256(path)}
        con.execute(f"DROP TABLE {name}")
    con.close()
    db.unlink(missing_ok=True)
    shutil.rmtree(out / "gen-tmp", ignore_errors=True)
    write_json(out / "dataset.json", {"suite": args.suite, "sf": args.sf, "generator": f"duckdb {duckdb.__version__} {generator}",
                                      "generation_seconds": time.monotonic() - started, "tables": tables,
                                      "format": "parquet zstd, one file per table, row groups of 122880"})
    print(json.dumps({"suite": args.suite, "sf": args.sf, "tables": len(tables)}))


def cmd_export_sql(args):
    import duckdb
    con = duckdb.connect()
    for suite, (_, function, count) in SUITES.items():
        con.execute(f"INSTALL {suite}; LOAD {suite}")
        target = REPO / "benchmarks" / "tpc" / suite
        target.mkdir(parents=True, exist_ok=True)
        rows = con.execute(f"SELECT query_nr, query FROM {function}() ORDER BY 1").fetchall()
        assert len(rows) == count, (suite, len(rows))
        for number, text in rows:
            (target / f"q{number:02d}.sql").write_text(text.strip().rstrip(";") + "\n")
    print(json.dumps({"exported": {s: SUITES[s][2] for s in SUITES}, "duckdb": duckdb.__version__}))


# ---------------------------------------------------------------- systems
def kelvo_catalog(path, data, accel_dir=None):
    lines = ["sources:"]
    for table in tables_in(data):
        source = f"src_{table}" if accel_dir else table
        lines += [f"  - id: {source}", "    type: parquet", f"    path: {Path(data, table + '.parquet')}"]
    if accel_dir:
        lines += ["acceleration:", "  tenant_id: tpc", f"  directory: {accel_dir}", "  datasets:"]
        for table in tables_in(data):
            lines += [f"    - id: {table}", "      query:", "        mode: federated", f"        sources: [src_{table}]",
                      f"        sql: SELECT * FROM src_{table}", "      authorization_version: tpc-v1",
                      "      refresh_interval: 0s", "      max_age: 720h", "      limits:",
                      "        max_rows: 100000000", "        max_bytes: 1099511627776", "        timeout: 1h",
                      "        memory_mb: 6144", "        threads: 4", "        max_temp_mb: 60000"]
    Path(path).write_text("\n".join(lines) + "\n")


def kelvo_limits(args):
    return ["--memory-mb", str(args.engine_memory_mb), "--threads", str(args.threads), "--temp-mb", "60000",
            "--max-rows", "10000000", "--max-bytes", str(4 << 30), "--timeout", f"{int(args.timeout)}s"]


def cmd_driver(args):
    """DuckDB Python child: same Parquet views, same limits, Arrow IPC file output."""
    t0 = time.monotonic()
    import duckdb
    import pyarrow as pa
    t_import = time.monotonic()
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{args.engine_memory_mb}MB'; SET threads={args.threads}; "
                f"SET temp_directory='{os.environ.get('TMPDIR', '/tmp')}'; SET max_temp_directory_size='60000MB'")
    for table in tables_in(args.data):
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{Path(args.data, table + '.parquet')}')")
    t_setup = time.monotonic()
    result = con.execute(Path(args.sql).read_text()).fetch_arrow_table()
    t_query = time.monotonic()
    with open(args.out, "wb") as sink, pa.ipc.new_file(sink, result.schema) as writer:
        writer.write_table(result)
        sink.flush()
        os.fsync(sink.fileno())
    t_out = time.monotonic()
    print(json.dumps({"import_s": t_import - t0, "setup_s": t_setup - t_import, "query_s": t_query - t_setup,
                      "arrow_write_s": t_out - t_query, "rows": result.num_rows}))


class Trino:
    """One long-lived single-node Trino container over the same Parquet (Hive connector, file metastore)."""

    def __init__(self, args, data):
        import trino
        self.args, self.data, self.name = args, Path(data).resolve(), "kelvo-tpc-trino"
        etc = Path(args.workdir, "trino-etc").resolve()
        (etc / "catalog").mkdir(parents=True, exist_ok=True)
        (etc / "catalog" / "pq.properties").write_text(
            "connector.name=hive\nhive.metastore=file\nhive.metastore.catalog.dir=local:///metastore\n"
            "fs.native-local.enabled=true\nlocal.location=/\nhive.non-managed-table-writes-enabled=false\n")
        (etc / "jvm.config").write_text("\n".join(["-server", f"-Xmx{args.trino_heap_gib}G", "-XX:+UseG1GC",
                                                   "-XX:+ExitOnOutOfMemoryError", "-Djdk.attach.allowAttachSelf=true"]) + "\n")
        (etc / "config.properties").write_text(
            "coordinator=true\nnode-scheduler.include-coordinator=true\nhttp-server.http.port=8080\n"
            "discovery.uri=http://localhost:8080\n"
            f"query.max-memory={args.trino_query_gib}GB\nquery.max-memory-per-node={args.trino_query_gib}GB\n"
            f"task.concurrency={args.threads}\nspill-enabled=false\n")
        (etc / "node.properties").write_text("node.environment=tpc\nnode.id=tpc\nnode.data-dir=/data/trino\n")
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", self.name, f"--memory={args.memory_gib}g", "--memory-swap",
                        f"{args.memory_gib}g", f"--cpus={args.threads}", "-p", "127.0.0.1:18080:8080",
                        "-v", f"{etc}:/etc/trino:ro", "-v", f"{self.data}:/tpcdata:ro",
                        "--tmpfs", "/metastore:rw,mode=1777", args.trino_image], check=True, capture_output=True)
        self.cid = subprocess.run(["docker", "inspect", "-f", "{{.Id}}", self.name], capture_output=True, text=True).stdout.strip()
        self.cgroup = Path(f"/sys/fs/cgroup/system.slice/docker-{self.cid}.scope")
        self.version = None
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            try:
                self.connect().cursor().execute("SELECT 1").fetchall()
                break
            except Exception:  # noqa: BLE001 - container still starting
                time.sleep(3)
        cursor = self.connect().cursor()
        self.version = cursor.execute("SELECT node_version FROM system.runtime.nodes").fetchall()[0][0]
        cursor.execute("CREATE SCHEMA IF NOT EXISTS pq.tpc")
        cursor.fetchall()
        import pyarrow.parquet as pq
        for table in tables_in(self.data):
            schema = pq.read_schema(self.data / f"{table}.parquet")
            columns = ", ".join(f'"{f.name}" {trino_type(f.type)}' for f in schema)
            # Hive external tables point at a directory: one directory per file via a symlink-free layout.
            cursor.execute(f"CREATE TABLE IF NOT EXISTS pq.tpc.{table} ({columns}) WITH (format='PARQUET', "
                           f"external_location='local:///tpcdata/trino/{table}')")
            cursor.fetchall()
        self.trino = trino

    def connect(self):
        import trino
        return trino.dbapi.connect(host="127.0.0.1", port=18080, user="tpc", catalog="pq", schema="tpc",
                                   request_timeout=self.args.timeout)

    def run(self, sql):
        sampler = Sampler(self.cgroup, None, use_peak=False)
        before = sampler._read("memory.current")
        sampler.start()
        started = time.monotonic()
        error, rows, columns = None, None, None
        try:
            cursor = self.connect().cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = cursor.description
        except Exception as exc:  # noqa: BLE001 - recorded verbatim
            error = f"{type(exc).__name__}: {exc}"[:2000]
        elapsed = time.monotonic() - started
        sampler.stop_event.set()
        sampler.join()
        return {"process_seconds": elapsed, "peak_cgroup_memory_bytes": max(sampler.cgroup_peak, before),
                "error": error}, rows, columns

    def stop(self):
        subprocess.run(["docker", "rm", "-f", self.name], capture_output=True)


def trino_type(t):
    import pyarrow as pa
    if pa.types.is_int64(t):
        return "BIGINT"
    if pa.types.is_integer(t):
        return "INTEGER"
    if pa.types.is_decimal(t):
        return f"DECIMAL({t.precision},{t.scale})"
    if pa.types.is_floating(t):
        return "DOUBLE"
    if pa.types.is_date(t):
        return "DATE"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    return "VARCHAR"


def trino_columns(rows, description):
    out = []
    for index, column in enumerate(description or []):
        kind = (column[1] or "").lower()
        scale = column[5] if kind.startswith("decimal") else (0 if kind in ("bigint", "integer", "smallint", "tinyint") else None)
        out.append((scale, [row[index] for row in rows]))
    return out


def trino_layout(data):
    """Hive tables need directories; hard-link each Parquet file into trino/<table>/ (no copy)."""
    for table in tables_in(data):
        target = Path(data, "trino", table)
        target.mkdir(parents=True, exist_ok=True)
        link = target / "data.parquet"
        if not link.exists():
            os.link(Path(data, table + ".parquet"), link)


# ---------------------------------------------------------------- run
def cmd_run(args):
    data, workdir = Path(args.data).resolve(), Path(args.workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    systems = args.systems.split(",")
    queries = sorted(Path(REPO, "benchmarks", "tpc", args.suite).glob("q*.sql"))
    if args.queries:
        wanted = {f"q{int(n):02d}" for n in args.queries.split(",")}
        queries = [q for q in queries if q.stem in wanted]
    dataset = json.loads((data / "dataset.json").read_text())
    report = {"schema_version": 1, "suite": args.suite, "sf": dataset["sf"], "dataset": dataset, "host": host_facts(),
              "started_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "trials": args.trials, "systems": systems,
              "limits": {"cgroup_memory_gib": args.memory_gib, "engine_memory_mb": args.engine_memory_mb,
                         "threads": args.threads, "timeout_s": args.timeout, "trino_heap_gib": args.trino_heap_gib,
                         "trino_query_memory_gib": args.trino_query_gib},
              "kelvo_sha256": sha256(args.kelvo) if "kelvo" in args.systems else None,
              "harness_sha256": sha256(__file__), "setup": {}, "queries": {}}
    catalogs = {}
    if "kelvo" in systems:
        catalogs["kelvo"] = workdir / "catalog.yml"
        kelvo_catalog(catalogs["kelvo"], data)
    if "kelvo_accel" in systems:
        accel = workdir / "acceleration"
        catalogs["kelvo_accel"] = workdir / "catalog-accel.yml"
        kelvo_catalog(catalogs["kelvo_accel"], data, accel)
        refreshes = {}
        for table in tables_in(data):
            trial = workdir / "refresh" / table
            trial.mkdir(parents=True, exist_ok=True)
            m = run_contained([args.kelvo, "accelerate", "refresh", "--config", str(catalogs["kelvo_accel"]), "--dataset", table],
                              trial, args.memory_gib, args.threads, 3600)
            m["stderr_tail"] = (trial / "stderr.log").read_text(errors="replace")[-1500:]
            refreshes[table] = m
        report["setup"]["kelvo_accel_refresh"] = refreshes
    trino = None
    if "trino" in systems:
        trino_layout(data)
        started = time.monotonic()
        trino = Trino(args, data)
        report["setup"]["trino"] = {"image": args.trino_image, "version": trino.version,
                                    "startup_and_ddl_seconds_excluded": time.monotonic() - started,
                                    "connector": "hive, file metastore, native local filesystem over the same Parquet files"}
    path = Path(args.output)
    try:
        for qfile in queries:
            sql = qfile.read_text()
            entry = report["queries"].setdefault(qfile.stem, {"sql_sha256": hashlib.sha256(sql.encode()).hexdigest(), "systems": {}})
            reference = None
            order = ["duckdb"] + [s for s in systems if s != "duckdb"] if "duckdb" in systems else systems
            for trial_index in range(args.trials):
                rotated = order[trial_index % len(order):] + order[:trial_index % len(order)]
                for system in rotated:
                    record = entry["systems"].setdefault(system, {"trials": []})
                    trial = workdir / args.suite / qfile.stem / system / str(trial_index)
                    trial.mkdir(parents=True, exist_ok=True)
                    result_path = trial / "result.arrow"
                    if system == "trino":
                        m, rows, description = trino.run(sql)
                        observed = None if m["error"] else canonical_rows(trino_columns(rows, description))
                    else:
                        if system == "duckdb":
                            argv = [sys.executable, "-I", str(Path(__file__).resolve()), "driver", "--data", str(data),
                                    "--sql", str(qfile), "--out", str(result_path), "--threads", str(args.threads),
                                    "--engine-memory-mb", str(args.engine_memory_mb)]
                        else:
                            argv = [args.kelvo, "query", "--config", str(catalogs[system]), "--sources",
                                    ",".join(tables_in(data)), "--sql", sql, "--out", str(result_path), *kelvo_limits(args)]
                        m = run_contained(argv, trial, args.memory_gib, args.threads, args.timeout)
                        err = (trial / "stderr.log").read_text(errors="replace")
                        out = (trial / "stdout.log").read_text(errors="replace")
                        m["error"] = None
                        if m["returncode"] != 0 or m["timed_out"] or not result_path.exists():
                            m["error"] = ("timeout " if m["timed_out"] else "") + (err.strip()[-2000:] or f"exit {m['returncode']}")
                        else:
                            try:
                                stats_text = out if system == "duckdb" else err
                                m["engine_stats"] = json.loads(stats_text.strip().splitlines()[-1])
                            except (ValueError, IndexError):
                                m["engine_stats"] = None
                        observed = None
                        if m["error"] is None:
                            table = read_arrow(result_path)
                            m["rows"], m["result_sha256"] = table.num_rows, sha256(result_path)
                            observed = canonical_rows(arrow_columns(table))
                    if system == "duckdb" and observed is not None and reference is None:
                        reference = observed
                    if m["error"] is not None:
                        m["verdict"] = "failed"
                    elif reference is None:
                        m["verdict"] = "no_reference"
                    else:
                        m["verdict"], m["verdict_detail"] = compare(reference, observed)
                    m["trial"] = trial_index
                    record["trials"].append(m)
                    if result_path.exists() and not args.keep_results:
                        result_path.unlink()
                    print(json.dumps({"q": qfile.stem, "system": system, "trial": trial_index,
                                      "s": round(m["process_seconds"], 3), "verdict": m["verdict"],
                                      "error": (m["error"] or "")[:160]}), flush=True)
            for system, record in entry["systems"].items():
                good = [t["process_seconds"] for t in record["trials"] if t["verdict"] in ("exact_order", "matched_unordered")]
                record["seconds"] = summary_stats(good)
                record["passed_trials"] = len(good)
                record["peak_sampled_tree_rss_bytes"] = max((t.get("peak_sampled_tree_rss_bytes") or 0) for t in record["trials"])
                record["peak_cgroup_memory_bytes"] = max((t.get("peak_cgroup_memory_bytes") or 0) for t in record["trials"])
                record["peak_spill_bytes"] = max((t.get("peak_spill_bytes") or 0) for t in record["trials"])
                record["state"] = "passed" if len(good) == len(record["trials"]) else "failed"
            report["finished_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
            write_json(path, report)
    finally:
        if trino:
            trino.stop()
    write_json(path, report)


# ---------------------------------------------------------------- concurrency
def cmd_concurrency(args):
    import urllib.error
    import urllib.request
    data, workdir = Path(args.data).resolve(), Path(args.workdir).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    catalog = workdir / "catalog.yml"
    kelvo_catalog(catalog, data)
    queries = sorted(Path(REPO, "benchmarks", "tpc", "tpch").glob("q*.sql"))
    token = secrets.token_hex(24)
    report = {"schema_version": 1, "suite": "tpch", "sf": json.loads((data / "dataset.json").read_text())["sf"],
              "host": host_facts(), "kelvo_sha256": sha256(args.kelvo), "harness_sha256": sha256(__file__),
              "admission": {"concurrency": args.admission, "max_queries": 128, "engine_memory_mb_per_query": args.engine_memory_mb,
                            "threads_per_query": args.threads, "cgroup_memory_gib": args.memory_gib},
              "refusal_policy": "HTTP 429 on results claim is counted, client sleeps 0.25 s and retries the same query; latency includes waits",
              "levels": {}}
    tables = ",".join(tables_in(data))
    for streams in [int(s) for s in args.streams.split(",")]:
        port = 18900 + streams
        level = workdir / f"streams-{streams}"
        level.mkdir(parents=True, exist_ok=True)
        holder = {}

        def serve():
            holder["m"] = run_contained([args.kelvo, "serve", "--config", str(catalog), "--listen", f"127.0.0.1:{port}",
                                         "--concurrency", str(args.admission), *kelvo_limits(args)],
                                        level, args.memory_gib, args.cpus, args.level_timeout, env={"KELVO_TOKEN": token})

        server = threading.Thread(target=serve)
        server.start()
        base = f"http://127.0.0.1:{port}"
        for _ in range(100):
            try:
                urllib.request.urlopen(base + "/health", timeout=2).read()
                break
            except OSError:
                time.sleep(0.2)
        headers = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

        def one(qfile):
            refusals, started = 0, time.monotonic()
            body = json.dumps({"sql": qfile.read_text(), "mode": "federated", "sources": tables.split(",")}).encode()
            while True:
                try:
                    created = json.loads(urllib.request.urlopen(urllib.request.Request(base + "/v1/queries", body, headers), timeout=60).read())
                    payload = urllib.request.urlopen(urllib.request.Request(f"{base}/v1/queries/{created['id']}/results", headers=headers),
                                                     timeout=args.timeout).read()
                    import pyarrow as pa
                    rows = pa.ipc.open_stream(payload).read_all().num_rows
                    return {"q": qfile.stem, "latency_s": time.monotonic() - started, "refusals": refusals, "rows": rows, "error": None}
                except urllib.error.HTTPError as exc:
                    if exc.code == 429:
                        refusals += 1
                        time.sleep(0.25)
                        continue
                    return {"q": qfile.stem, "latency_s": time.monotonic() - started, "refusals": refusals,
                            "error": f"HTTP {exc.code}: {exc.read()[:500].decode(errors='replace')}"}
                except Exception as exc:  # noqa: BLE001 - recorded
                    return {"q": qfile.stem, "latency_s": time.monotonic() - started, "refusals": refusals, "error": repr(exc)[:500]}

        def stream(seed):
            order = queries[:]
            random.Random(seed).shuffle(order)
            return [one(q) for q in order]

        started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(streams) as pool:
            results = [r for batch in pool.map(stream, range(streams)) for r in batch]
        wall = time.monotonic() - started
        subprocess.run(["sudo", "-n", "pkill", "-f", f"127.0.0.1:{port}"], check=False)
        server.join()
        ok = [r["latency_s"] for r in results if r["error"] is None]
        report["levels"][str(streams)] = {
            "streams": streams, "queries": len(results), "succeeded": len(ok), "failed": len(results) - len(ok),
            "refusals": sum(r["refusals"] for r in results), "wall_seconds": wall,
            "throughput_qph": 3600 * len(ok) / wall, "p50_s": percentile(ok, 50), "p95_s": percentile(ok, 95),
            "p99_s": percentile(ok, 99), "server": holder.get("m"), "results": results}
        print(json.dumps({k: v for k, v in report["levels"][str(streams)].items() if k not in ("results", "server")}), flush=True)
        write_json(args.output, report)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--suite", choices=SUITES, required=True)
    g.add_argument("--sf", type=float, required=True)
    g.add_argument("--out", required=True)
    g.add_argument("--memory-gib", type=int, default=10)
    g.add_argument("--threads", type=int, default=4)
    sub.add_parser("export-sql")
    d = sub.add_parser("driver")
    for name in ("--data", "--sql", "--out"):
        d.add_argument(name, required=True)
    d.add_argument("--threads", type=int, default=4)
    d.add_argument("--engine-memory-mb", type=int, default=6144)
    for name in ("run", "concurrency"):
        p = sub.add_parser(name)
        p.add_argument("--data", required=True)
        p.add_argument("--kelvo", required=True)
        p.add_argument("--workdir", required=True)
        p.add_argument("--output", required=True)
        p.add_argument("--memory-gib", type=int, default=8)
        p.add_argument("--engine-memory-mb", type=int, default=6144)
        p.add_argument("--threads", type=int, default=4)
        p.add_argument("--timeout", type=float, default=1800)
        if name == "run":
            p.add_argument("--suite", choices=SUITES, required=True)
            p.add_argument("--systems", default="duckdb,kelvo,kelvo_accel,trino")
            p.add_argument("--trials", type=int, default=3)
            p.add_argument("--queries", default="")
            p.add_argument("--keep-results", action="store_true")
            p.add_argument("--trino-image", default="trinodb/trino:479")
            p.add_argument("--trino-heap-gib", type=int, default=6)
            p.add_argument("--trino-query-gib", type=int, default=4)
        else:
            p.add_argument("--streams", default="1,4,8,16")
            p.add_argument("--admission", type=int, default=4)
            p.add_argument("--cpus", type=int, default=4)
            p.add_argument("--level-timeout", type=float, default=7200)
    args = parser.parse_args()
    {"generate": cmd_generate, "export-sql": cmd_export_sql, "driver": cmd_driver, "run": cmd_run,
     "concurrency": cmd_concurrency}[args.command](args)


if __name__ == "__main__":
    main()

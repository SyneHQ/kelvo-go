#!/usr/bin/env python3
"""Run one local-Parquet analytical baseline in a fresh Linux VM process.

The supervising harness applies the process cgroup and spill-filesystem limits.
This runner never installs packages, builds software, queries a remote database,
or preloads input data outside its reported timing. Compare the complete process
wall time supplied by the supervisor as well as the phases reported here.
"""
from __future__ import annotations

import time

STARTED_NS = time.perf_counter_ns()

import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import resource
import shutil
import sys
import tempfile


EXPECTED_VERSIONS = {"duckdb": "1.5.6", "polars": "1.44.2", "pyarrow": "25.0.1"}
WORKLOADS = {"daily_rolling_kpis", "borough_hour_hotspots", "route_economics", "monthly_zone_momentum"}
MAX_MANIFEST_BYTES = 2 << 20
BATCH_ROWS = 65536


def require(condition, message):
    if not condition:
        raise ValueError(message)


def elapsed(start):
    return (time.perf_counter_ns() - start) / 1e9


def load_manifest(path, workload, scratch_override=None):
    require(path.is_absolute() and path.is_file(), "manifest must be an absolute regular file")
    data = path.read_bytes()
    require(len(data) <= MAX_MANIFEST_BYTES, "manifest exceeds its size limit")
    manifest = json.loads(data)
    require(manifest.get("version") == 1, "unsupported manifest version")
    require(manifest.get("versions") == EXPECTED_VERSIONS, "manifest must pin the agreed library versions")
    require(workload in WORKLOADS and workload in manifest["workloads"], "unknown workload")
    for name, expected_rows in (("trips", 22612607), ("zones", 265)):
        source = manifest["dataset"][name]
        require(source["rows"] == expected_rows, "unexpected dataset row count")
        require(isinstance(source["paths"], list) and 1 <= len(source["paths"]) <= 64, "invalid input file count")
        require(len(source["paths"]) == len(source["sha256"]), "input digest count differs from file count")
        for value, digest in zip(source["paths"], source["sha256"]):
            file = Path(value)
            require(file.is_absolute() and file.is_file() and file.suffix == ".parquet", "input must be a local Parquet file")
            require(re.fullmatch(r"[0-9a-f]{64}", digest) is not None, "invalid input digest")
        require(len(set(source["paths"])) == len(source["paths"]), "duplicate input file")
    limits = manifest["limits"]
    require(limits["threads"] == 2 and limits["memory_mb"] == 1024 and limits["max_temp_mb"] == 2048,
            "baseline budgets must match the agreed two-thread profile")
    require(1 <= limits["max_rows"] <= 1000000 and 1024 <= limits["max_bytes"] <= 256 << 20,
            "invalid output budget")
    require(limits["result_compression"] in ("none", "lz4_frame"), "unsupported result compression")
    temp = scratch_override if scratch_override is not None else Path(limits["temp_directory"])
    require(temp.is_absolute() and temp.is_dir() and not temp.is_symlink(), "temporary parent must be an existing local directory")
    spec = manifest["workloads"][workload]
    columns = spec["output"]["columns"]
    require(isinstance(columns, list) and 1 <= len(columns) <= 64, "invalid output schema")
    require(len({column["name"] for column in columns}) == len(columns), "duplicate output column")
    for column in columns:
        require(re.fullmatch(r"[a-z][a-z0-9_]*", column["name"]) is not None, "invalid output column name")
        require(column["type"] in ("int32", "int64", "string"), "unsupported canonical output type")
        require(isinstance(column.get("nullable", False), bool), "invalid column nullability")
    require(1 <= spec["output"]["max_rows"] <= limits["max_rows"], "workload output exceeds configured row budget")
    return manifest, spec, hashlib.sha256(data).hexdigest()


class BoundedOutput(io.RawIOBase):
    def __init__(self, target, maximum):
        super().__init__()
        self.target = target
        self.maximum = maximum
        self.bytes_written = 0

    def writable(self):
        return True

    def tell(self):
        return self.bytes_written

    def write(self, data):
        require(len(data) <= self.maximum - self.bytes_written, "encoded Arrow output exceeds byte budget")
        written = self.target.write(data)
        require(written == len(data), "short Arrow output write")
        self.bytes_written += written
        return written


def output_schema(pa, spec):
    types = {"int32": pa.int32(), "int64": pa.int64(), "string": pa.string()}
    return pa.schema([pa.field(column["name"], types[column["type"]], nullable=column.get("nullable", False))
                      for column in spec["output"]["columns"]])


def write_arrow(pa, batches, schema, temporary, limits, row_limit, compute_start):
    rows = decoded_bytes = batch_count = 0
    first_batch_seconds = None
    codec = "lz4" if limits["result_compression"] == "lz4_frame" else None
    options = pa.ipc.IpcWriteOptions(compression=codec, use_threads=False)
    with temporary.open("xb") as target:
        bounded = BoundedOutput(target, limits["max_bytes"])
        with pa.ipc.new_stream(bounded, schema, options=options) as writer:
            for batch in batches:
                if first_batch_seconds is None:
                    first_batch_seconds = elapsed(compute_start)
                require(batch.schema.names == schema.names, "result column order differs from contract")
                # Only physical type/nullability normalization happens here.
                # No sorting, filling NULLs, or changing arithmetic is allowed.
                batch = batch.cast(schema, safe=True)
                require(rows + batch.num_rows <= row_limit, "result exceeds row budget")
                require(decoded_bytes + batch.nbytes <= limits["max_bytes"], "decoded Arrow output exceeds byte budget")
                for field, column in zip(schema, batch.columns):
                    require(field.nullable or column.null_count == 0, "non-nullable result contains NULL")
                writer.write_batch(batch)
                rows += batch.num_rows
                decoded_bytes += batch.nbytes
                batch_count += 1
        target.flush()
        os.fsync(target.fileno())
    return {"rows": rows, "batches": batch_count, "arrow_buffer_bytes": decoded_bytes,
            "wire_bytes": bounded.bytes_written, "query_to_first_arrow_batch_seconds": first_batch_seconds}


def polars_lazy(pl, workload, frames, spec):
    """Native LazyFrame equivalents of benchmarks/ride-hailing/*.sql.

    These are explicit equivalent workflows, not claims that Polars parses the
    DuckDB CTE syntax. Each final ordering includes the SQL's tie-break columns.
    """
    trips, zones = frames["trips"], frames["zones"]
    eligible = trips.filter(
        pl.col("source_month").is_between(201901, 201903, closed="both")
        & (pl.col("pickup_unix_us") >= 1546300800000000)
        & (pl.col("pickup_unix_us") < 1554076800000000)
    )
    kpis = ["trips", "measured_amount_trips", "total_amount_cents"]

    def amount_aggregates():
        return [pl.len().cast(pl.Int64).alias("trips"),
                pl.col("fare_cents").count().cast(pl.Int64).alias("measured_amount_trips"),
                pl.col("fare_cents").fill_null(0).sum().cast(pl.Int64).alias("total_amount_cents")]

    if workload == "daily_rolling_kpis":
        daily = eligible.with_columns(
            (pl.col("pickup_unix_us") // 86400000000).cast(pl.Int64).alias("day_bucket")
        ).group_by("day_bucket").agg(amount_aggregates())
        spine = pl.LazyFrame({"day_bucket": list(range(17897, 17987))}, schema={"day_bucket": pl.Int64})
        filled = spine.join(daily, on="day_bucket", how="left").with_columns(
            [pl.col(name).fill_null(0).cast(pl.Int64) for name in kpis]
        ).sort("day_bucket")
        result = filled.with_columns(
            (pl.col("day_bucket") - 17896).clip(upper_bound=7).cast(pl.Int64).alias("window_days"),
            *[pl.col(name).rolling_sum(window_size=7, min_samples=1).cast(pl.Int64).alias("trailing_7d_" + name)
              for name in kpis],
        ).sort("day_bucket")
    elif workload == "borough_hour_hotspots":
        names = zones.select("zone_id", "borough", pl.col("zone").alias("zone_name"))
        hourly = eligible.with_columns(
            (pl.col("pickup_unix_us") // 3600000000).cast(pl.Int64).alias("hour_bucket")
        ).join(names, left_on="pickup_zone_id", right_on="zone_id", how="inner", coalesce=False).group_by(
            "hour_bucket", "borough", "zone_id", "zone_name"
        ).agg(pl.len().cast(pl.Int64).alias("trips"))
        ranked = hourly.with_columns(
            pl.col("trips").sum().over("hour_bucket", "borough").cast(pl.Int64).alias("borough_hour_trips")
        ).sort(["hour_bucket", "borough", "trips", "zone_id"], descending=[False, False, True, False]).with_columns(
            pl.col("zone_id").cum_count().over("hour_bucket", "borough").cast(pl.Int64).alias("zone_rank")
        )
        result = ranked.filter(pl.col("zone_rank") <= 3).sort(["hour_bucket", "borough", "zone_rank", "zone_id"])
    elif workload == "route_economics":
        # SQL conditionally counts/sums usable distances and retains only groups
        # with >=100 usable trips. Filtering that identical cohort before the
        # group-by is equivalent; the raw normalized input remains unchanged.
        usable = eligible.filter(
            pl.col("fare_cents").is_not_null() & (pl.col("fare_cents") >= 0)
            & (pl.col("trip_distance") > 0) & (pl.col("trip_distance") <= 100)
        )
        routes = usable.group_by("pickup_zone_id", "dropoff_zone_id").agg(
            pl.len().cast(pl.Int64).alias("trips"),
            pl.col("fare_cents").sum().cast(pl.Int64).alias("total_amount_cents"),
            (pl.col("trip_distance") < 1).cast(pl.Int64).sum().alias("lt_1_mile_trips"),
            ((pl.col("trip_distance") >= 1) & (pl.col("trip_distance") < 5)).cast(pl.Int64).sum().alias("ge_1_lt_5_mile_trips"),
            (pl.col("trip_distance") >= 5).cast(pl.Int64).sum().alias("ge_5_le_100_mile_trips"),
        ).filter(pl.col("trips") >= 100)
        pickup_names = zones.select(pl.col("zone_id").alias("pickup_zone_id"),
                                    pl.col("borough").alias("pickup_borough"),
                                    pl.col("zone").alias("pickup_zone_name"))
        dropoff_names = zones.select(pl.col("zone_id").alias("dropoff_zone_id"),
                                     pl.col("borough").alias("dropoff_borough"),
                                     pl.col("zone").alias("dropoff_zone_name"))
        named = routes.join(pickup_names, on="pickup_zone_id", how="inner").join(
            dropoff_names, on="dropoff_zone_id", how="inner"
        )
        ranked = named.sort(["pickup_borough", "total_amount_cents", "trips", "pickup_zone_id", "dropoff_zone_id"],
                            descending=[False, True, True, False, False]).with_columns(
            pl.col("pickup_zone_id").cum_count().over("pickup_borough").cast(pl.Int64).alias("route_rank")
        )
        result = ranked.filter(pl.col("route_rank") <= 10).sort(
            ["pickup_borough", "route_rank", "pickup_zone_id", "dropoff_zone_id"]
        )
    elif workload == "monthly_zone_momentum":
        monthly = eligible.with_columns(
            pl.when(pl.col("pickup_unix_us") < 1548979200000000).then(pl.lit(201901, dtype=pl.Int64))
            .when(pl.col("pickup_unix_us") < 1551398400000000).then(pl.lit(201902, dtype=pl.Int64))
            .otherwise(pl.lit(201903, dtype=pl.Int64)).alias("month")
        ).group_by("month", "pickup_zone_id").agg(amount_aggregates())
        months = pl.LazyFrame({"month": [201901, 201902, 201903]}, schema={"month": pl.Int64})
        names = zones.select("zone_id", "borough", pl.col("zone").alias("zone_name"))
        grid = months.join(names, how="cross")
        filled = grid.join(monthly, left_on=["month", "zone_id"], right_on=["month", "pickup_zone_id"], how="left").with_columns(
            [pl.col(name).fill_null(0).cast(pl.Int64) for name in kpis]
        )
        lagged = filled.with_columns(
            pl.col("trips").shift(1).over("zone_id", order_by="month").alias("previous_trips")
        )
        changes = lagged.filter(pl.col("month") > 201901).with_columns(
            (pl.col("trips") - pl.col("previous_trips")).cast(pl.Int64).alias("trip_change")
        )
        ranked = changes.sort(["month", "borough", "trip_change", "zone_id"], descending=[False, False, True, False]).with_columns(
            pl.col("zone_id").cum_count().over("month", "borough").cast(pl.Int64).alias("zone_rank")
        )
        result = ranked.sort(["month", "borough", "zone_rank", "zone_id"])
    else:
        raise ValueError("unknown Polars analytical workflow")
    return result.select([column["name"] for column in spec["output"]["columns"]])


def run(args, metrics):
    require(sys.platform.startswith("linux"), "run analytical baselines on the designated Linux VM")
    manifest, spec, manifest_hash = load_manifest(args.manifest, args.workload, args.scratch)
    require(args.output.is_absolute() and args.metrics.is_absolute(), "output paths must be absolute")
    require(args.output != args.metrics and not args.output.exists() and not args.metrics.exists(), "outputs must be distinct new files")
    require(args.output.parent.is_dir() and args.metrics.parent.is_dir(), "output directories must already exist")
    partial = args.output.with_name(args.output.name + ".partial")
    require(not partial.exists(), "partial output already exists")
    limits = manifest["limits"]
    metrics.update({"engine": args.engine, "workload": args.workload, "manifest_sha256": manifest_hash,
                    "threads": limits["threads"], "memory_budget_profile_mb": limits["memory_mb"],
                    "engine_memory_limit_mb": limits["memory_mb"] if args.engine == "duckdb" else None,
                    "configured_temp_mb": limits["max_temp_mb"], "result_compression": limits["result_compression"],
                    "dataset": manifest["dataset"], "input_hash_verification": "Input hashes are verified once by the staging supervisor. This run records that manifest without re-reading all inputs to warm the filesystem cache.",
                    "source_read_included_in_compute": True,
                    "preload_seconds": 0, "timing_scope": "Fresh process; imports and scan setup reported separately. Compute includes local Parquet reads, execution, Arrow conversion, IPC serialization and output fsync. No input preload or canonical validation is hidden outside these phases. Interpreter startup and final exit require the supervising process wall timer.",
                    "platform": {"system": platform.system(), "machine": platform.machine(), "python": platform.python_version()}})
    # Polars constructs its global pool during import. Set before either library
    # is imported, and keep Arrow IPC compression single-threaded independently.
    os.environ["POLARS_MAX_THREADS"] = str(limits["threads"])
    os.environ["OMP_NUM_THREADS"] = str(limits["threads"])
    os.environ["OPENBLAS_NUM_THREADS"] = str(limits["threads"])
    temp_parent = args.scratch if args.scratch is not None else Path(limits["temp_directory"])
    scratch = Path(tempfile.mkdtemp(prefix="analytics-" + args.engine + "-", dir=temp_parent))
    os.environ["POLARS_TEMP_DIR"] = str(scratch)
    # Pinned Polars 1.44.2 has a separate OOC path; its Linux default is /var/tmp
    # and would bypass the supervisor's shared benchmark filesystem otherwise.
    os.environ["POLARS_OOC_SPILL_DIR"] = str(scratch)
    os.environ["POLARS_OOC_MEMORY_BUDGET_MB"] = str(limits["memory_mb"])
    os.environ["POLARS_OOC_DISK_BUDGET_MB"] = str(limits["max_temp_mb"])
    connection = reader = None
    try:
        import_start = time.perf_counter_ns()
        import pyarrow as pa
        require(pa.__version__ == EXPECTED_VERSIONS["pyarrow"], "unexpected PyArrow version")
        pa.set_cpu_count(limits["threads"])
        pa.set_io_thread_count(limits["threads"])
        if args.engine == "duckdb":
            import duckdb
            require(duckdb.__version__ == EXPECTED_VERSIONS["duckdb"], "unexpected DuckDB version")
            metrics["versions"] = {"duckdb": duckdb.__version__, "pyarrow": pa.__version__}
        else:
            import polars as pl
            require(pl.__version__ == EXPECTED_VERSIONS["polars"], "unexpected Polars version")
            require(pl.thread_pool_size() == limits["threads"], "Polars thread pool differs from configured limit")
            metrics["versions"] = {"polars": pl.__version__, "pyarrow": pa.__version__}
        metrics["import_seconds"] = elapsed(import_start)
        setup_start = time.perf_counter_ns()
        schema = output_schema(pa, spec)
        if args.engine == "duckdb":
            connection = duckdb.connect(":memory:", config={"threads": limits["threads"],
                "memory_limit": str(limits["memory_mb"]) + "MB", "temp_directory": str(scratch),
                "max_temp_directory_size": str(limits["max_temp_mb"]) + "MB",
                "autoinstall_known_extensions": False, "autoload_known_extensions": False})
            for name in ("trips", "zones"):
                connection.read_parquet(manifest["dataset"][name]["paths"]).create_view(name)
            metrics["implementation"] = "duckdb_sql"
        else:
            frames = {name: pl.scan_parquet(manifest["dataset"][name]["paths"])
                      for name in ("trips", "zones")}
            metrics["implementation"] = "polars_lazy"
            metrics["execution_engine_requested"] = "streaming"
            metrics["polars_ooc_memory_budget_mb"] = limits["memory_mb"]
            metrics["polars_ooc_disk_budget_mb"] = limits["max_temp_mb"]
            metrics["polars_ooc_scope"] = "POLARS_OOC_MEMORY_BUDGET_MB=1024 and POLARS_OOC_DISK_BUDGET_MB=2048 are version-specific Polars 1.44.2 OOC settings, not whole-process memory or independent disk guarantees. POLARS_OOC_SPILL_DIR is the owned trial scratch directory."
            metrics["execution_engine_note"] = "Polars may use in-memory operators where streaming is unsupported; this is not a claim that every operator streams. The supervisor enforces a process cgroup and a 2GiB filesystem shared by benchmark spill, output and metadata, not an independent 2GiB allowance per trial."
        metrics["setup_seconds"] = elapsed(setup_start)
        compute_start = time.perf_counter_ns()
        if args.engine == "duckdb":
            sql = spec["sql"]["duckdb"].format(trips="trips", zones="zones")
            require(isinstance(sql, str) and 0 < len(sql) <= 128 << 10, "invalid SQL workload")
            metrics["sql_sha256"] = hashlib.sha256(sql.encode()).hexdigest()
            reader = connection.sql(sql).to_arrow_reader(batch_size=BATCH_ROWS)
            batches = reader
        else:
            frame = polars_lazy(pl, args.workload, frames, spec).collect(engine="streaming")
            table = frame.to_arrow(compat_level=pl.CompatLevel.oldest())
            batches = table.to_batches(max_chunksize=BATCH_ROWS)
        metrics.update(write_arrow(pa, batches, schema, partial, limits,
                                   min(limits["max_rows"], spec["output"]["max_rows"]), compute_start))
        metrics["compute_serialize_seconds"] = elapsed(compute_start)
        partial.replace(args.output)
        metrics["success"] = True
    finally:
        teardown_start = time.perf_counter_ns()
        if reader is not None:
            reader.close()
        if connection is not None:
            connection.close()
        partial.unlink(missing_ok=True)
        shutil.rmtree(scratch)
        metrics["teardown_seconds"] = elapsed(teardown_start)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", required=True, choices=("duckdb", "polars"))
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--workload", required=True, choices=sorted(WORKLOADS))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    parser.add_argument("--scratch", type=Path, help="Existing per-trial scratch directory supplied by the supervisor")
    args = parser.parse_args()
    os.umask(0o077)
    if not args.metrics.is_absolute() or not args.metrics.parent.is_dir() or args.metrics.exists():
        parser.error("metrics must name a new absolute file in an existing directory")
    metrics = {"success": False}
    try:
        run(args, metrics)
    except Exception as error:
        metrics["success"] = False
        metrics["error_type"] = type(error).__name__
        # Operational detail stays in the supervisor's private log. Public
        # metrics contain the error class and never echo configuration content.
        print("Analytical baseline failed: " + str(error), file=sys.stderr)
    usage = resource.getrusage(resource.RUSAGE_SELF)
    metrics.update({"in_process_wall_seconds": elapsed(STARTED_NS),
                    "process_cpu_seconds": usage.ru_utime + usage.ru_stime,
                    "process_peak_rss_kib": usage.ru_maxrss,
                    "max_rss_scope": "Linux process high-water RSS including Python, library setup and query; shared pages may also appear in other processes' RSS. External cgroup metrics are reported separately."})
    with args.metrics.open("x") as output:
        json.dump(metrics, output, indent=2)
        output.write("\n")
    print(json.dumps({"engine": args.engine, "workload": args.workload, "success": metrics["success"]}), flush=True)
    return 0 if metrics["success"] else 1


if __name__ == "__main__":
    sys.exit(main())

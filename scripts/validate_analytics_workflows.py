#!/usr/bin/env python3
"""Validate NYC Taxi analytical outputs after all benchmark timers have stopped.

Run on the designated Linux VM with DuckDB 1.5.6 and PyArrow 25.0.1. This
program never queries the remote source, executes manifest SQL, installs
packages, or launches benchmarks. Independent ClickHouse Arrow references
must already exist. Raw snapshot audits, small independent grouped aggregates,
Python windows/ranks, Arrow decoding, and hashing are all outside trial timing.
Private JSON paths and SQL are never copied to the public report.
"""
from __future__ import annotations

import argparse
from collections import defaultdict, deque
from datetime import datetime, timezone
import hashlib
import heapq
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import stat
import sys
import tempfile
import time

sys.dont_write_bytecode = True

VERSIONS = {"duckdb": "1.5.6", "pyarrow": "25.0.1"}
TOTAL_ROWS = 22612607
MONTH_ROWS = {201901: 7696617, 201902: 7049370, 201903: 7866620}
MONTHS = tuple(MONTH_ROWS)
START_US, FEB_US, MAR_US, END_US = 1546300800000000, 1548979200000000, 1551398400000000, 1554076800000000
INT64_MAX = (1 << 63) - 1
MAX_ARTIFACT_BYTES = 256 << 20
MAX_MANIFEST_BYTES = 8 << 20
LABEL = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,95}\Z")
SHA256 = re.compile(r"[0-9a-f]{64}\Z")
ELIGIBLE = f"source_month BETWEEN 201901 AND 201903 AND pickup_unix_us >= {START_US} AND pickup_unix_us < {END_US}"
PICKUP_MONTH = f"CASE WHEN pickup_unix_us < {FEB_US} THEN 201901 WHEN pickup_unix_us < {MAR_US} THEN 201902 ELSE 201903 END"
USABLE_ROUTE = "fare_cents IS NOT NULL AND fare_cents >= 0 AND trip_distance > 0 AND trip_distance <= 100"
CONTRACTS = {
    "daily_rolling_kpis": {
        "names": ["day_bucket", "trips", "measured_amount_trips", "total_amount_cents", "window_days",
                  "trailing_7d_trips", "trailing_7d_measured_amount_trips", "trailing_7d_total_amount_cents"],
        "strings": set(), "order": ["day_bucket"], "max_rows": 90, "expected_rows": 90,
    },
    "borough_hour_hotspots": {
        "names": ["hour_bucket", "borough", "zone_id", "zone_name", "trips", "borough_hour_trips", "zone_rank"],
        "strings": {"borough", "zone_name"}, "order": ["hour_bucket", "borough", "zone_rank", "zone_id"],
        "max_rows": 100000,
    },
    "route_economics": {
        "names": ["pickup_borough", "pickup_zone_id", "pickup_zone_name", "dropoff_borough", "dropoff_zone_id",
                  "dropoff_zone_name", "trips", "total_amount_cents", "lt_1_mile_trips", "ge_1_lt_5_mile_trips",
                  "ge_5_le_100_mile_trips", "route_rank"],
        "strings": {"pickup_borough", "pickup_zone_name", "dropoff_borough", "dropoff_zone_name"},
        "order": ["pickup_borough", "route_rank", "pickup_zone_id", "dropoff_zone_id"], "max_rows": 1000,
    },
    "monthly_zone_momentum": {
        "names": ["month", "borough", "zone_id", "zone_name", "trips", "previous_trips", "trip_change",
                  "measured_amount_trips", "total_amount_cents", "zone_rank"],
        "strings": {"borough", "zone_name"}, "order": ["month", "borough", "zone_rank", "zone_id"],
        "max_rows": 1000, "expected_rows": 530,
    },
}


class ValidationError(Exception):
    """Only fixed public error codes may be carried by this exception."""


def require(condition, code):
    if not condition:
        raise ValidationError(code)


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def fingerprint(path):
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
            size += len(block)
    return {"sha256": digest.hexdigest(), "bytes": size}


def regular_path(value, code, private=False, must_exist=True):
    require(isinstance(value, str) and "\0" not in value, code)
    path = Path(value)
    require(path.is_absolute() and not path.is_symlink(), code)
    if must_exist:
        info = path.lstat()
        require(stat.S_ISREG(info.st_mode), code)
        if private:
            require(info.st_uid == os.getuid() and not info.st_mode & 0o077, code)
        return path.resolve()
    return path.resolve(strict=False)


def check_label(value):
    require(isinstance(value, str) and LABEL.fullmatch(value) is not None, "invalid_public_label")


def load_manifest(path):
    path = regular_path(str(path), "manifest_must_be_private_regular_file", private=True)
    require(path.stat().st_size <= MAX_MANIFEST_BYTES, "manifest_byte_limit_exceeded")
    data = path.read_bytes()
    manifest = json.loads(data)
    require(isinstance(manifest, dict) and type(manifest.get("schema_version")) is int
            and manifest["schema_version"] == 1, "invalid_manifest_version")
    require(set(manifest["dataset"]) == {"trips", "zones"}, "invalid_dataset_contract")
    for name, count in (("trips", TOTAL_ROWS), ("zones", 265)):
        source = manifest["dataset"][name]
        require(type(source["rows"]) is int and source["rows"] == count, "unexpected_dataset_row_count")
        require(isinstance(source["paths"], list) and 1 <= len(source["paths"]) <= 64, "invalid_dataset_file_count")
        require(isinstance(source["sha256"], list) and len(source["paths"]) == len(source["sha256"]), "invalid_dataset_hash_count")
        normalized = []
        for value, digest in zip(source["paths"], source["sha256"]):
            file = regular_path(value, "invalid_dataset_file")
            require(file.suffix == ".parquet", "dataset_requires_parquet")
            require(isinstance(digest, str) and SHA256.fullmatch(digest) is not None, "invalid_dataset_hash")
            normalized.append(str(file))
        require(len(set(normalized)) == len(normalized), "duplicate_dataset_file")
        source["paths"] = normalized
    require(set(manifest["workflows"]) == set(CONTRACTS), "all_four_workflow_contracts_required")
    references = set()
    for workflow, spec in manifest["workflows"].items():
        contract = CONTRACTS[workflow]
        expected_columns = [{"name": name, "type": "string" if name in contract["strings"] else "int64", "nullable": False}
                            for name in contract["names"]]
        output = spec["output"]
        require(output["columns"] == expected_columns and output["order_by"] == contract["order"], "workflow_schema_or_order_differs_from_frozen_contract")
        require(type(output["max_rows"]) is int and output["max_rows"] == contract["max_rows"], "workflow_row_limit_differs_from_frozen_contract")
        if "expected_rows" in output:
            require(output["expected_rows"] == contract.get("expected_rows"), "workflow_expected_rows_differs_from_frozen_contract")
        for dialect in ("duckdb", "clickhouse"):
            sql = spec["sql"][dialect]
            require(isinstance(sql, str) and 0 < len(sql.encode()) <= 128 << 10, "invalid_workflow_sql_text")
        reference = spec["reference"]
        require(reference["engine"] == "clickhouse", "reference_must_be_direct_clickhouse")
        reference["path"] = str(regular_path(reference["path"], "invalid_reference_file"))
        require(reference["path"] not in references, "duplicate_reference_path")
        references.add(reference["path"])
    require(isinstance(manifest["results"], list) and 1 <= len(manifest["results"]) <= 1000, "nonempty_results_required")
    identities, result_paths = set(), set()
    for result in manifest["results"]:
        require(isinstance(result, dict), "invalid_result_contract")
        for name in ("case", "engine"):
            check_label(result[name])
        require(result["workflow"] in CONTRACTS, "unknown_result_workflow")
        require(type(result["trial"]) is int and 1 <= result["trial"] <= 1000, "invalid_result_trial")
        require(result["execution_state"] in ("succeeded", "failed"), "invalid_result_execution_state")
        identity = (result["case"], result["trial"])
        require(identity not in identities, "duplicate_result_identity")
        identities.add(identity)
        if result.get("path") is None:
            require(result["execution_state"] == "failed", "successful_result_path_required")
        else:
            value = str(regular_path(result["path"], "invalid_result_path", must_exist=False))
            require(value not in result_paths and value not in references, "result_path_reused_or_is_reference")
            result_paths.add(value)
            result["path"] = value
    return manifest, hashlib.sha256(data).hexdigest()


def write_report(path, report):
    temporary = path.with_name(path.name + ".tmp-" + secrets.token_hex(6))
    try:
        with temporary.open("x") as stream:
            json.dump(report, stream, indent=2, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def one(connection, sql):
    cursor = connection.execute(sql)
    names = [field[0] for field in cursor.description]
    row = cursor.fetchone()
    return {name: int(value) if value is not None else None for name, value in zip(names, row)}


def rows(connection, sql):
    cursor = connection.execute(sql)
    while True:
        batch = cursor.fetchmany(4096)
        if not batch:
            return
        yield from batch


def check(checks, name, condition):
    checks[name] = bool(condition)


def verify_dataset_files(manifest, pa, pq, report):
    trip_types = [pa.int64(), pa.int32(), pa.int32(), pa.int64(), pa.int32(), pa.int32(),
                  pa.float64(), pa.float64(), pa.float64(), pa.int64()]
    trip_names = ["trip_id", "source_month", "source_row", "pickup_unix_us", "pickup_zone_id", "dropoff_zone_id",
                  "passenger_count", "trip_distance", "total_amount", "fare_cents"]
    expected = {"trips": list(zip(trip_names, trip_types)),
                "zones": list(zip(["zone_id", "borough", "zone", "service_zone"], [pa.int32(), pa.string(), pa.string(), pa.string()]))}
    for name, source in manifest["dataset"].items():
        count, files = 0, []
        report[name] = {"files": files, "hashes_verified": False, "schema_verified": False}
        for value, expected_hash in zip(source["paths"], source["sha256"]):
            file = Path(value)
            observed = fingerprint(file)
            files.append({**observed, "expected_sha256": expected_hash})
            require(observed["sha256"] == expected_hash, "dataset_fingerprint_mismatch")
            parquet = pq.ParquetFile(file)
            schema = parquet.schema_arrow
            require([(field.name, field.type) for field in schema] == expected[name], "raw_snapshot_schema_mismatch")
            count += parquet.metadata.num_rows
            files[-1]["rows"] = parquet.metadata.num_rows
        require(count == source["rows"], "raw_snapshot_metadata_row_count_mismatch")
        report[name] = {"rows": count, "files": files, "hashes_verified": True, "schema_verified": True}
    return report


def raw_audit(connection):
    """Independent simple aggregates; no CTE workflow SQL is used here."""
    checks = {}
    identity = one(connection, """SELECT COUNT(*) AS rows, COUNT(DISTINCT trip_id) AS distinct_trip_ids,
        COUNT(*) FILTER (WHERE trip_id IS NULL OR source_month IS NULL OR source_row IS NULL) AS identity_nulls,
        COUNT(*) FILTER (WHERE trip_id != (CAST(source_month AS BIGINT)-201900)*100000000+source_row) AS trip_id_formula_mismatches,
        SUM(CAST(trip_id AS HUGEINT)) AS trip_id_sum,
        SUM(COALESCE(CAST(fare_cents AS HUGEINT),0)) AS amount_cents_sum,
        SUM(ABS(COALESCE(CAST(fare_cents AS HUGEINT),0))) AS amount_cents_absolute_sum,
        COUNT(*) FILTER (WHERE fare_cents IS NULL) AS amount_nulls
        FROM audit_trips""")
    monthly_files = []
    for month, count, low, high in rows(connection, "SELECT source_month, COUNT(*), MIN(source_row), MAX(source_row) FROM audit_trips GROUP BY source_month ORDER BY source_month"):
        monthly_files.append({"source_month": int(month) if month is not None else None, "rows": int(count),
                              "source_row_min": int(low) if low is not None else None, "source_row_max": int(high) if high is not None else None})
    check(checks, "exact_source_month_rows", {entry["source_month"]: entry["rows"] for entry in monthly_files} == MONTH_ROWS)
    check(checks, "source_row_ranges", all(entry["source_month"] in MONTH_ROWS and entry["source_row_min"] == 0
          and entry["source_row_max"] == MONTH_ROWS[entry["source_month"]] - 1 for entry in monthly_files))
    check(checks, "trip_id_uniqueness_and_formula", identity["rows"] == identity["distinct_trip_ids"] == TOTAL_ROWS
          and identity["identity_nulls"] == identity["trip_id_formula_mismatches"] == 0)
    check(checks, "normalized_fixture_sentinels", identity["trip_id_sum"] == 4624929309126691
          and identity["amount_cents_sum"] == 40534901730 and identity["amount_nulls"] == 0)
    check(checks, "amount_int64_absolute_sum_bound", 0 <= identity["amount_cents_absolute_sum"] <= INT64_MAX)
    # HUGEINT is used before ABS, so -2^63 is never negated in Int64 arithmetic.
    dimensions = one(connection, """SELECT COUNT(*) AS rows, COUNT(DISTINCT zone_id) AS distinct_zone_ids,
        COUNT(DISTINCT borough) AS borough_labels,
        COUNT(*) FILTER (WHERE zone_id IS NULL OR borough IS NULL OR zone IS NULL OR service_zone IS NULL) AS null_rows
        FROM audit_zones""")
    check(checks, "unique_nonnull_dimension", dimensions == {"rows": 265, "distinct_zone_ids": 265, "borough_labels": 8, "null_rows": 0})
    require(checks["unique_nonnull_dimension"], "raw_dimension_invariant_failed")
    zone_lookup = {int(identifier): (borough, name, service) for identifier, borough, name, service
                   in rows(connection, "SELECT zone_id, borough, zone, service_zone FROM audit_zones ORDER BY zone_id")}
    timestamp_stages = {name: int(count) for name, count in rows(connection, f"""SELECT CASE
        WHEN source_month IS NULL OR source_month NOT BETWEEN 201901 AND 201903 THEN 'invalid_source_month'
        WHEN pickup_unix_us IS NULL THEN 'null_pickup_timestamp'
        WHEN pickup_unix_us < {START_US} THEN 'before_quarter'
        WHEN pickup_unix_us >= {END_US} THEN 'after_quarter'
        ELSE 'eligible' END AS stage, COUNT(*) FROM audit_trips GROUP BY stage""")}
    check(checks, "timestamp_stage_partition", sum(timestamp_stages.values()) == TOTAL_ROWS)
    check(checks, "known_timestamp_anomalies", timestamp_stages.get("invalid_source_month", 0) == 0
          and timestamp_stages.get("null_pickup_timestamp", 0) == 0
          and timestamp_stages.get("before_quarter", 0) + timestamp_stages.get("after_quarter", 0) == 800
          and timestamp_stages.get("eligible", 0) == TOTAL_ROWS - 800)
    eligible = one(connection, f"""SELECT COUNT(*) AS trips, COUNT(fare_cents) AS measured_amount_trips,
        SUM(COALESCE(CAST(fare_cents AS HUGEINT),0)) AS total_amount_cents,
        COUNT(*) FILTER (WHERE source_month != ({PICKUP_MONTH})) AS source_file_pickup_month_mismatches
        FROM audit_trips WHERE {ELIGIBLE}""")
    month_pairs = [{"source_month": int(source), "pickup_month": int(pickup), "rows": int(count)}
                   for source, pickup, count in rows(connection, f"SELECT source_month, ({PICKUP_MONTH}) AS pickup_month, COUNT(*) FROM audit_trips WHERE {ELIGIBLE} GROUP BY source_month, pickup_month ORDER BY source_month, pickup_month")]
    unknown_zones = one(connection, f"""SELECT
        COUNT(*) FILTER (WHERE t.pickup_zone_id IS NULL) AS pickup_null,
        COUNT(*) FILTER (WHERE t.pickup_zone_id IS NOT NULL AND p.zone_id IS NULL) AS pickup_unmatched,
        COUNT(*) FILTER (WHERE t.dropoff_zone_id IS NULL) AS dropoff_null,
        COUNT(*) FILTER (WHERE t.dropoff_zone_id IS NOT NULL AND d.zone_id IS NULL) AS dropoff_unmatched,
        COUNT(*) FILTER (WHERE p.zone_id IS NOT NULL) AS known_pickup_trips,
        COUNT(*) FILTER (WHERE p.zone_id IS NOT NULL AND d.zone_id IS NOT NULL) AS known_both_zone_trips
        FROM (SELECT * FROM audit_trips WHERE {ELIGIBLE}) t
        LEFT JOIN audit_zones p ON t.pickup_zone_id=p.zone_id
        LEFT JOIN audit_zones d ON t.dropoff_zone_id=d.zone_id""")
    quality = one(connection, f"""SELECT
        COUNT(*) FILTER (WHERE fare_cents IS NULL) AS amount_null,
        COUNT(*) FILTER (WHERE fare_cents < 0) AS amount_negative,
        COUNT(*) FILTER (WHERE fare_cents = 0) AS amount_zero,
        COUNT(*) FILTER (WHERE fare_cents > 0) AS amount_positive,
        COUNT(*) FILTER (WHERE trip_distance IS NULL) AS distance_null,
        COUNT(*) FILTER (WHERE isnan(trip_distance)) AS distance_nan,
        COUNT(*) FILTER (WHERE isinf(trip_distance)) AS distance_infinite,
        COUNT(*) FILTER (WHERE isfinite(trip_distance) AND trip_distance <= 0) AS distance_nonpositive_finite,
        COUNT(*) FILTER (WHERE isfinite(trip_distance) AND trip_distance > 100) AS distance_above_100_finite,
        COUNT(*) FILTER (WHERE isfinite(trip_distance) AND trip_distance > 0 AND trip_distance <= 100) AS distance_usable,
        COUNT(*) FILTER (WHERE total_amount IS NULL) AS original_amount_null,
        COUNT(*) FILTER (WHERE isnan(total_amount)) AS original_amount_nan,
        COUNT(*) FILTER (WHERE isinf(total_amount)) AS original_amount_infinite
        FROM audit_trips WHERE {ELIGIBLE}""")
    check(checks, "amount_quality_partition", sum(quality[name] for name in ("amount_null", "amount_negative", "amount_zero", "amount_positive")) == eligible["trips"])
    check(checks, "distance_quality_partition", sum(quality[name] for name in ("distance_null", "distance_nan", "distance_infinite", "distance_nonpositive_finite", "distance_above_100_finite", "distance_usable")) == eligible["trips"])
    route_stages = {}
    for stage, count, amount in rows(connection, f"""SELECT CASE
        WHEN t.fare_cents IS NULL THEN 'amount_null'
        WHEN t.fare_cents < 0 THEN 'amount_negative'
        WHEN t.trip_distance IS NULL THEN 'distance_null'
        WHEN isnan(t.trip_distance) THEN 'distance_nan'
        WHEN isinf(t.trip_distance) THEN 'distance_infinite'
        WHEN t.trip_distance <= 0 THEN 'distance_nonpositive_finite'
        WHEN t.trip_distance > 100 THEN 'distance_above_100_finite'
        WHEN t.pickup_zone_id IS NULL THEN 'pickup_null'
        WHEN t.dropoff_zone_id IS NULL THEN 'dropoff_null'
        WHEN p.zone_id IS NULL THEN 'pickup_unmatched'
        WHEN d.zone_id IS NULL THEN 'dropoff_unmatched'
        ELSE 'usable_known_pair' END AS stage, COUNT(*), SUM(COALESCE(CAST(t.fare_cents AS HUGEINT),0))
        FROM (SELECT * FROM audit_trips WHERE {ELIGIBLE}) t
        LEFT JOIN audit_zones p ON t.pickup_zone_id=p.zone_id
        LEFT JOIN audit_zones d ON t.dropoff_zone_id=d.zone_id GROUP BY stage"""):
        route_stages[stage] = {"trips": int(count), "total_amount_cents": int(amount)}
    check(checks, "route_stage_partition", sum(entry["trips"] for entry in route_stages.values()) == eligible["trips"]
          and sum(entry["total_amount_cents"] for entry in route_stages.values()) == eligible["total_amount_cents"])
    known_months = {}
    for month, count, measured, amount in rows(connection, f"""SELECT ({PICKUP_MONTH}) AS month,
        COUNT(*), COUNT(fare_cents), SUM(COALESCE(CAST(fare_cents AS HUGEINT),0))
        FROM audit_trips t INNER JOIN audit_zones z ON t.pickup_zone_id=z.zone_id
        WHERE {ELIGIBLE} GROUP BY month ORDER BY month"""):
        known_months[int(month)] = (int(count), int(measured), int(amount))
    check(checks, "known_pickup_month_reconciliation", sum(value[0] for value in known_months.values()) == unknown_zones["known_pickup_trips"])
    report = {"state": "passed" if all(checks.values()) else "failed", "checks": checks,
        "identity_and_amount_bounds": identity, "source_months": monthly_files, "dimensions": dimensions,
        "borough_labels": sorted({value[0] for value in zone_lookup.values()}),
        "timestamp_stages": timestamp_stages, "eligible": eligible, "source_file_pickup_month_pairs": month_pairs,
        "zone_quality_within_eligible": unknown_zones, "amount_distance_quality_within_eligible": quality,
        "route_exclusion_stages": route_stages,
        "route_stage_scope": "Disjoint priority stages in displayed SQL order; amounts are recorded for all stages and may be negative in excluded stages.",
        "known_pickup_month_totals": [{"month": month, "trips": value[0], "measured_amount_trips": value[1], "total_amount_cents": value[2]}
                                      for month, value in sorted(known_months.items())]}
    context = {"zones": zone_lookup, "eligible": eligible, "unknown_zones": unknown_zones,
               "route_stages": route_stages, "known_months": known_months}
    return report, context


def independent_expectations(connection, context):
    """Group only in DuckDB; all calendars, windows, joins and ranks below are Python."""
    expected, checks, detail = {}, {}, {}
    zones = context["zones"]
    daily = {int(day): (int(count), int(measured), int(amount)) for day, count, measured, amount in rows(connection, f"""SELECT
        pickup_unix_us // 86400000000 AS day, COUNT(*), COUNT(fare_cents), SUM(COALESCE(CAST(fare_cents AS HUGEINT),0))
        FROM audit_trips WHERE {ELIGIBLE} GROUP BY day""")}
    window, day_rows = deque(maxlen=7), []
    for day in range(17897, 17987):
        values = daily.get(day, (0, 0, 0))
        window.append(values)
        day_rows.append((day, *values, len(window), *(sum(value[index] for value in window) for index in range(3))))
    expected["daily_rolling_kpis"] = day_rows
    check(checks, "daily_raw_reconciliation", tuple(sum(value[index] for value in daily.values()) for index in range(3))
          == tuple(context["eligible"][name] for name in ("trips", "measured_amount_trips", "total_amount_cents")))

    # Keep only each partition's top three plus its full denominator. A Python
    # dictionary of every raw trip or all zone/hour groups is unnecessary.
    hours, known_count, hourly_groups = {}, 0, 0
    for hour, zone, count in rows(connection, f"SELECT pickup_unix_us // 3600000000 AS hour, pickup_zone_id, COUNT(*) FROM audit_trips WHERE {ELIGIBLE} GROUP BY hour, pickup_zone_id"):
        if zone is None or int(zone) not in zones:
            continue
        hour, zone, count = int(hour), int(zone), int(count)
        borough, zone_name, _ = zones[zone]
        partition = hours.setdefault((hour, borough), {"total": 0, "active_zones": 0, "top": []})
        partition["total"] += count
        partition["active_zones"] += 1
        candidate = (count, -zone, zone, zone_name)
        if len(partition["top"]) < 3:
            heapq.heappush(partition["top"], candidate)
        elif candidate > partition["top"][0]:
            heapq.heapreplace(partition["top"], candidate)
        known_count += count
        hourly_groups += 1
    hourly_rows = []
    for (hour, borough), partition in sorted(hours.items()):
        for rank, (count, _, zone, zone_name) in enumerate(sorted(partition["top"], key=lambda item: (-item[0], item[2])), 1):
            hourly_rows.append((hour, borough, zone, zone_name, count, partition["total"], rank))
    expected["borough_hour_hotspots"] = hourly_rows
    check(checks, "hourly_known_pickup_reconciliation", known_count == context["unknown_zones"]["known_pickup_trips"])
    detail["hourly"] = {"unranked_known_zone_hour_groups": hourly_groups, "borough_hour_partitions": len(hours),
                        "unranked_known_pickup_trips": known_count, "top_three_rows": len(hourly_rows)}

    route_partitions = defaultdict(list)
    route_totals = {"raw_known_route_count": 0, "raw_known_trips": 0, "raw_known_amount_cents": 0,
                    "below_threshold_route_count": 0, "below_threshold_trips": 0, "below_threshold_amount_cents": 0,
                    "qualified_route_count": 0, "qualified_trips": 0, "qualified_amount_cents": 0}
    for pickup, dropoff, count, amount, short, medium, long in rows(connection, f"""SELECT pickup_zone_id, dropoff_zone_id,
        COUNT(*), SUM(CAST(fare_cents AS HUGEINT)),
        COUNT(*) FILTER (WHERE trip_distance < 1),
        COUNT(*) FILTER (WHERE trip_distance >= 1 AND trip_distance < 5),
        COUNT(*) FILTER (WHERE trip_distance >= 5)
        FROM audit_trips WHERE {ELIGIBLE} AND {USABLE_ROUTE} GROUP BY pickup_zone_id, dropoff_zone_id"""):
        if pickup is None or dropoff is None or int(pickup) not in zones or int(dropoff) not in zones:
            continue
        pickup, dropoff, count, amount, short, medium, long = map(int, (pickup, dropoff, count, amount, short, medium, long))
        route_totals["raw_known_route_count"] += 1
        route_totals["raw_known_trips"] += count
        route_totals["raw_known_amount_cents"] += amount
        prefix = "qualified" if count >= 100 else "below_threshold"
        route_totals[prefix + "_route_count"] += 1
        route_totals[prefix + "_trips"] += count
        route_totals[prefix + "_amount_cents"] += amount
        if count < 100:
            continue
        pickup_borough, pickup_name, _ = zones[pickup]
        dropoff_borough, dropoff_name, _ = zones[dropoff]
        route_partitions[pickup_borough].append((pickup_borough, pickup, pickup_name, dropoff_borough, dropoff, dropoff_name,
                                                count, amount, short, medium, long))
    route_rows = []
    for borough, candidates in sorted(route_partitions.items()):
        ordered = sorted(candidates, key=lambda item: (-item[7], -item[6], item[1], item[4]))
        route_rows.extend((*item, rank) for rank, item in enumerate(ordered[:10], 1))
    expected["route_economics"] = route_rows
    usable_stage = context["route_stages"].get("usable_known_pair", {"trips": 0, "total_amount_cents": 0})
    check(checks, "route_raw_reconciliation", route_totals["raw_known_trips"] == usable_stage["trips"]
          and route_totals["raw_known_amount_cents"] == usable_stage["total_amount_cents"])
    route_totals.update({"top_ten_route_count": len(route_rows), "top_ten_trips": sum(item[6] for item in route_rows),
                         "top_ten_amount_cents": sum(item[7] for item in route_rows)})
    for suffix in ("route_count", "trips", "amount_cents"):
        route_totals["qualified_not_top_ten_" + suffix] = route_totals["qualified_" + suffix] - route_totals["top_ten_" + suffix]
    detail["routes"] = route_totals

    monthly = {}
    for month, zone, count, measured, amount in rows(connection, f"""SELECT ({PICKUP_MONTH}) AS month,
        pickup_zone_id, COUNT(*), COUNT(fare_cents), SUM(COALESCE(CAST(fare_cents AS HUGEINT),0))
        FROM audit_trips WHERE {ELIGIBLE} GROUP BY month, pickup_zone_id"""):
        if zone is not None and int(zone) in zones:
            monthly[(int(month), int(zone))] = (int(count), int(measured), int(amount))
    monthly_partitions = defaultdict(list)
    for month in (201902, 201903):
        for zone, (borough, name, _) in zones.items():
            count, measured, amount = monthly.get((month, zone), (0, 0, 0))
            previous = monthly.get((month - 1, zone), (0, 0, 0))[0]
            monthly_partitions[(month, borough)].append((month, borough, zone, name, count, previous, count - previous, measured, amount))
    month_rows = []
    for partition, candidates in sorted(monthly_partitions.items()):
        month_rows.extend((*item, rank) for rank, item in enumerate(sorted(candidates, key=lambda item: (-item[6], item[2])), 1))
    expected["monthly_zone_momentum"] = month_rows
    check(checks, "monthly_raw_reconciliation", all(tuple(sum(value[index] for (key_month, _), value in monthly.items() if key_month == month)
          for index in range(3)) == context["known_months"].get(month, (0, 0, 0)) for month in MONTHS))
    context["monthly"] = monthly
    return expected, {"state": "passed" if all(checks.values()) else "failed", "checks": checks, "details": detail,
                      "expected_result_rows": {name: len(value) for name, value in expected.items()}}


def arrow_rows(path, workflow, pa):
    contract = CONTRACTS[workflow]
    info = path.stat()
    require(8 <= info.st_size <= MAX_ARTIFACT_BYTES, "arrow_artifact_byte_limit_or_empty")
    with path.open("rb") as stream:
        stream.seek(-8, os.SEEK_END)
        require(stream.read() == b"\xff\xff\xff\xff\0\0\0\0", "arrow_eos_missing")
    result, decoded_bytes, physical = [], 0, []
    with pa.memory_map(str(path), "r") as source:
        with pa.ipc.open_stream(source) as reader:
            require(reader.schema.names == contract["names"], "arrow_column_order_mismatch")
            for field in reader.schema:
                compatible = (pa.types.is_string(field.type) or pa.types.is_large_string(field.type)) if field.name in contract["strings"] else pa.types.is_signed_integer(field.type)
                require(compatible, "arrow_logical_type_mismatch")
                physical.append({"name": field.name, "type": str(field.type), "nullable": field.nullable})
            for batch in reader:
                require(len(result) + batch.num_rows <= contract["max_rows"], "arrow_row_limit_exceeded")
                decoded_bytes += batch.nbytes
                require(decoded_bytes <= MAX_ARTIFACT_BYTES, "arrow_decoded_byte_limit_exceeded")
                require(all(column.null_count == 0 for column in batch.columns), "unexpected_arrow_null")
                result.extend(zip(*(column.to_pylist() for column in batch.columns)))
            require(source.tell() == info.st_size, "trailing_bytes_after_arrow_stream")
    require(len(result) > 0, "empty_workflow_result")
    return result, {"physical_schema": physical, "decoded_arrow_bytes": decoded_bytes, "arrow_eos_present": True}


def invariants(workflow, values, expected, context):
    contract = CONTRACTS[workflow]
    checks = {}
    records = [dict(zip(contract["names"], row)) for row in values]
    keys = [tuple(record[name] for name in contract["order"]) for record in records]
    check(checks, "strict_complete_output_order", keys == sorted(keys) and len(set(keys)) == len(keys))
    check(checks, "exact_independent_raw_aggregate_result", values == expected)
    if workflow == "daily_rolling_kpis":
        check(checks, "calendar_spine", [item["day_bucket"] for item in records] == list(range(17897, 17987)))
        check(checks, "daily_reconciliation", tuple(sum(item[name] for item in records) for name in ("trips", "measured_amount_trips", "total_amount_cents"))
              == tuple(context["eligible"][name] for name in ("trips", "measured_amount_trips", "total_amount_cents")))
        check(checks, "measured_counts", all(0 <= item["measured_amount_trips"] <= item["trips"] for item in records))
        window, valid = deque(maxlen=7), True
        for item in records:
            window.append(item)
            valid &= item["window_days"] == len(window) and all(item["trailing_7d_" + name] == sum(day[name] for day in window)
                for name in ("trips", "measured_amount_trips", "total_amount_cents"))
        check(checks, "independent_deque_windows", valid)
    elif workflow == "borough_hour_hotspots":
        groups = defaultdict(list)
        for item in records:
            groups[(item["hour_bucket"], item["borough"])].append(item)
        check(checks, "top_three_rank_and_denominator", all(1 <= len(group) <= 3
            and [item["zone_rank"] for item in group] == list(range(1, len(group) + 1))
            and len({item["borough_hour_trips"] for item in group}) == 1
            and group[0]["borough_hour_trips"] >= sum(item["trips"] for item in group)
            and group == sorted(group, key=lambda item: (-item["trips"], item["zone_id"])) for group in groups.values()))
        check(checks, "known_zone_hour_cohort", all(429528 <= item["hour_bucket"] < 431688 and item["trips"] > 0
            and context["zones"].get(item["zone_id"], (None, None))[:2] == (item["borough"], item["zone_name"]) for item in records))
    elif workflow == "route_economics":
        groups = defaultdict(list)
        for item in records:
            groups[item["pickup_borough"]].append(item)
        bands = ("lt_1_mile_trips", "ge_1_lt_5_mile_trips", "ge_5_le_100_mile_trips")
        check(checks, "usable_route_counts_and_bands", all(item["trips"] >= 100 and item["total_amount_cents"] >= 0
            and all(item[name] >= 0 for name in bands) and sum(item[name] for name in bands) == item["trips"] for item in records))
        check(checks, "known_route_dimensions", all(context["zones"].get(item[side + "_zone_id"], (None, None))[:2]
            == (item[side + "_borough"], item[side + "_zone_name"]) for item in records for side in ("pickup", "dropoff")))
        check(checks, "top_ten_deterministic_rank", all(1 <= len(group) <= 10 and [item["route_rank"] for item in group] == list(range(1, len(group) + 1))
            and group == sorted(group, key=lambda item: (-item["total_amount_cents"], -item["trips"], item["pickup_zone_id"], item["dropoff_zone_id"])) for group in groups.values()))
    else:
        expected_keys = {(month, zone) for month in (201902, 201903) for zone in context["zones"]}
        check(checks, "full_two_month_zone_grid", len(records) == 530 and {(item["month"], item["zone_id"]) for item in records} == expected_keys)
        check(checks, "previous_month_and_signed_delta", all(item["previous_trips"] == context["monthly"].get((item["month"] - 1, item["zone_id"]), (0, 0, 0))[0]
            and item["trip_change"] == item["trips"] - item["previous_trips"] and 0 <= item["measured_amount_trips"] <= item["trips"] for item in records))
        groups = defaultdict(list)
        for item in records:
            groups[(item["month"], item["borough"])].append(item)
        check(checks, "monthly_deterministic_rank", all([item["zone_rank"] for item in group] == list(range(1, len(group) + 1))
            and group == sorted(group, key=lambda item: (-item["trip_change"], item["zone_id"])) for group in groups.values()))
        check(checks, "monthly_known_pickup_reconciliation", all(tuple(sum(item[name] for item in records if item["month"] == month)
            for name in ("trips", "measured_amount_trips", "total_amount_cents")) == context["known_months"].get(month, (0, 0, 0)) for month in (201902, 201903)))
    return {"state": "passed" if all(checks.values()) else "failed", "checks": checks}


def inspect_artifact(path_value, workflow, expected, context, pa, canonical_arrow):
    report = {"state": "failed", "errors": []}
    try:
        require(path_value is not None, "artifact_missing")
        path = regular_path(path_value, "artifact_must_be_regular_file")
        require(path.stat().st_size <= MAX_ARTIFACT_BYTES, "arrow_artifact_byte_limit_exceeded")
        report["file"] = fingerprint(path)
        values, metadata = arrow_rows(path, workflow, pa)
        report.update(metadata)
        canonical = canonical_arrow(path)
        require(canonical["rows"] == len(values), "canonical_row_count_disagrees_with_decoder")
        report["canonical"] = canonical
        if expected is None or context is None:
            report["errors"].append("independent_raw_expectation_unavailable")
        else:
            report["invariants"] = invariants(workflow, values, expected, context)
            if report["invariants"]["state"] != "passed":
                report["errors"].append("workflow_invariants_failed")
        report["state"] = "passed" if not report["errors"] else "failed"
    except ValidationError as error:
        report["errors"].append(str(error))
    except FileNotFoundError:
        report["errors"].append("artifact_missing")
    except Exception:
        report["errors"].append("artifact_decode_or_canonicalization_failed")
    return report


def run(args, manifest, report):
    require(sys.platform.startswith("linux"), "validator_requires_designated_linux_vm")
    # Imports happen only after the platform guard. No Mac Arrow decode.
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    require(duckdb.__version__ == VERSIONS["duckdb"] and pa.__version__ == VERSIONS["pyarrow"], "validator_library_version_mismatch")
    from federation_capacity import canonical_arrow
    report["versions"] = dict(VERSIONS)
    report["canonical_implementation_sha256"] = fingerprint(Path(__file__).with_name("federation_capacity.py"))["sha256"]
    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    report["dataset"] = {}
    verify_dataset_files(manifest, pa, pq, report["dataset"])
    scratch = Path(tempfile.mkdtemp(prefix="analytics-validation-", dir=args.scratch))
    connection, expected, context = None, {}, None
    try:
        connection = duckdb.connect(":memory:", config={"threads": 2, "memory_limit": str(args.memory_mb) + "MiB",
            "temp_directory": str(scratch), "max_temp_directory_size": str(args.max_temp_mb) + "MiB",
            "autoinstall_known_extensions": False, "autoload_known_extensions": False})
        for name in ("trips", "zones"):
            connection.read_parquet(manifest["dataset"][name]["paths"]).create_view("audit_" + name)
        raw_started = time.monotonic()
        try:
            report["raw_audit"], context = raw_audit(connection)
            if report["raw_audit"]["state"] == "passed":
                expected, report["independent_expectations"] = independent_expectations(connection, context)
                if report["independent_expectations"]["state"] != "passed":
                    report["errors"].append("independent_raw_expectations_failed")
            else:
                report["errors"].append("raw_snapshot_invariants_failed")
        except ValidationError as error:
            report["raw_audit"] = {"state": "failed", "errors": [str(error)]}
            report["errors"].append("raw_snapshot_audit_failed")
        except Exception:
            report["raw_audit"] = {"state": "failed", "errors": ["raw_aggregate_or_expectation_query_failed"]}
            report["errors"].append("raw_snapshot_audit_failed")
        report["raw_audit_and_expectation_seconds_outside_timing"] = time.monotonic() - raw_started
    finally:
        if connection is not None:
            connection.close()
        shutil.rmtree(scratch)
    for workflow, spec in manifest["workflows"].items():
        checked = inspect_artifact(spec["reference"]["path"], workflow, expected.get(workflow), context, pa, canonical_arrow)
        checked["declared_engine"] = "clickhouse"
        checked["sql_sha256"] = {dialect: hashlib.sha256(spec["sql"][dialect].encode()).hexdigest() for dialect in ("duckdb", "clickhouse")}
        report["references"][workflow] = checked
        write_report(args.output, report)
    for result in manifest["results"]:
        checked = {key: result[key] for key in ("case", "workflow", "engine", "trial", "execution_state")}
        artifact = inspect_artifact(result.get("path"), result["workflow"], expected.get(result["workflow"]), context, pa, canonical_arrow)
        checked["artifact"] = artifact
        checked["errors"] = []
        if result["execution_state"] != "succeeded":
            checked["errors"].append("benchmark_execution_failed")
        reference = report["references"][result["workflow"]]
        canonical = artifact.get("canonical")
        ref_canonical = reference.get("canonical")
        match = bool(canonical and ref_canonical and canonical["rows"] == ref_canonical["rows"]
                     and canonical["canonical_value_sha256"] == ref_canonical["canonical_value_sha256"])
        checked["matches_direct_clickhouse_canonical_values"] = match
        if not match:
            checked["errors"].append("direct_clickhouse_canonical_mismatch_or_unavailable")
        if reference["state"] != "passed":
            checked["errors"].append("independent_clickhouse_reference_not_validated")
        if artifact["state"] != "passed":
            checked["errors"].append("result_artifact_validation_failed")
        checked["state"] = "passed" if not checked["errors"] else "failed"
        report["results"].append(checked)
        write_report(args.output, report)
    observed = {result["workflow"] for result in manifest["results"]}
    report["result_workflow_coverage"] = {name: name in observed for name in CONTRACTS}
    if observed != set(CONTRACTS):
        report["errors"].append("result_workflow_coverage_incomplete")
    report["all_requested_results_passed"] = bool(report["results"]) and all(result["state"] == "passed" for result in report["results"])
    report["all_references_passed"] = len(report["references"]) == 4 and all(result["state"] == "passed" for result in report["references"].values())
    report["all_passed"] = (not report["errors"] and report["all_requested_results_passed"] and report["all_references_passed"]
        and report.get("raw_audit", {}).get("state") == "passed" and report.get("independent_expectations", {}).get("state") == "passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Private manifest schema_version 1:
dataset: {trips: {paths: [ABS.parquet], rows: 22612607, sha256: [HEX]},
          zones: {paths: [ABS.parquet], rows: 265, sha256: [HEX]}}
workflows: {ID: {sql: {duckdb: SQL_TEXT, clickhouse: SQL_TEXT},
                output: {columns: [{name: NAME, type: int64|string, nullable: false}],
                         order_by: [NAME], max_rows: N},
                reference: {path: ABS.arrow, engine: clickhouse}}}
results: [{case: PUBLIC_LABEL, workflow: ID, engine: PUBLIC_LABEL, trial: 1,
           path: ABS.arrow, execution_state: succeeded|failed}]
All four frozen workflows are required. SQL is hashed, never executed.
Failed trials may use path: null; they remain failed even if an artifact matches.
At least one result is required, and overall success requires all four workflow
IDs among results. It covers only listed attempts, not undeclared campaign runs.
--check reads manifest metadata only; it does not import Arrow/DuckDB or decode
data. Full validation requires Linux and the pinned installed libraries.
''')
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--scratch", required=True, type=Path)
    parser.add_argument("--memory-mb", type=int, default=1024)
    parser.add_argument("--max-temp-mb", type=int, default=512)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    os.umask(0o077)
    try:
        require(args.output.is_absolute() and args.output.parent.is_dir() and not args.output.exists()
                and not args.output.is_symlink(), "output_must_be_new_absolute_file")
        require(args.scratch.is_absolute() and args.scratch.is_dir() and not args.scratch.is_symlink(), "scratch_must_be_existing_absolute_directory")
        require(256 <= args.memory_mb <= 2048 and 64 <= args.max_temp_mb <= 2048, "validator_resource_limit_out_of_bounds")
        manifest, manifest_hash = load_manifest(args.manifest)
    except ValidationError as error:
        parser.error(str(error))
    except (OSError, ValueError, TypeError, KeyError):
        parser.error("invalid_manifest_or_paths")
    if args.check:
        print('{"input_check":"passed","validation":"not_started"}')
        return 0
    report = {"schema_version": 1, "started_at": utc_now(), "state": "running", "all_passed": False,
        "manifest_sha256": manifest_hash, "validator_sha256": fingerprint(Path(__file__))["sha256"],
        "validation_scope": "All snapshot hashes, raw audits, independent aggregate/window/rank expectations, Arrow decoding, canonical checks and invariants are outside benchmark trial timing.",
        "reference_scope": "Pre-existing references declared to originate from direct ClickHouse; this validator independently checks their full values against the raw normalized snapshot.",
        "resource_limits": {"threads": 2, "duckdb_memory_mib": args.memory_mb, "duckdb_temp_mib": args.max_temp_mb,
            "scope": "DuckDB settings only; the caller must provide any whole-process cgroup or filesystem cap."},
        "errors": [], "references": {}, "results": []}
    started = time.monotonic()
    previous = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        write_report(args.output, report)
        run(args, manifest, report)
    except KeyboardInterrupt:
        report["errors"].append("validation_interrupted")
        report["all_passed"] = False
    except ValidationError as error:
        report["errors"].append(str(error))
        report["all_passed"] = False
    except Exception:
        report["errors"].append("validator_runtime_or_io_failed")
        report["all_passed"] = False
    finally:
        signal.signal(signal.SIGTERM, previous)
        report["finished_at"] = utc_now()
        report["validation_seconds_outside_timing"] = time.monotonic() - started
        report["state"] = "passed" if report["all_passed"] else "failed"
        write_report(args.output, report)
    print(json.dumps({"state": report["state"], "validated_results": len(report["results"]),
                      "passed_results": sum(item["state"] == "passed" for item in report["results"]),
                      "all_passed": report["all_passed"]}))
    return 0 if report["all_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())

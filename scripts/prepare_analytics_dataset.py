#!/usr/bin/env python3
"""Stage identical normalized NYC Taxi Parquet inputs for analytical baselines.

Run on the benchmark VM before timing. Original files must already be present;
their published evidence hashes are checked before any output is created.
Reuses the exact conversion that populated the ClickHouse capacity fixture.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from federation_capacity import BATCH_ROWS, FILES, RAW_COLUMNS, SCHEMA, TOTAL_ROWS, convert


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-directory", required=True, type=Path)
    parser.add_argument("--provenance", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    source = args.source_directory.resolve(strict=True)
    provenance = json.loads(args.provenance.read_text())
    originals = provenance["source_files"] + [provenance["zone_lookup"]]
    expected_files = {filename for _, filename, _, _ in FILES} | {"taxi_zone_lookup.csv"}
    supplied_files = [entry["url"].rsplit("/", 1)[1] for entry in originals]
    if len(supplied_files) != len(expected_files) or set(supplied_files) != expected_files:
        raise ValueError("Provenance must identify every original file exactly once")
    for entry in originals:
        path = source / entry["url"].rsplit("/", 1)[1]
        if path.is_symlink() or path.stat().st_size != entry["bytes"] or digest(path) != entry["sha256"]:
            raise ValueError("Original dataset fingerprint mismatch")
    destination = args.output_directory.absolute()
    destination.mkdir(mode=0o700, parents=False, exist_ok=False)
    trips = destination / "trips.parquet"
    partial = destination / "trips.parquet.partial"
    rows = 0
    totals = {"trip_id_sum": 0, "fare_cents_sum": 0, "fare_cents_nulls": 0,
              "timestamp_nulls": 0, "outside_quarter_timestamps": 0}
    lower, upper = 1546300800000000, 1554076800000000
    with pq.ParquetWriter(partial, SCHEMA, compression="zstd", compression_level=3,
                          use_dictionary=True, write_statistics=True) as writer:
        for month, filename, expected_rows, _ in FILES:
            start = 0
            for batch in pq.ParquetFile(source / filename).iter_batches(BATCH_ROWS, columns=RAW_COLUMNS):
                normalized = convert(batch, month, start)
                writer.write_batch(normalized, row_group_size=BATCH_ROWS)
                columns = {field.name: column for field, column in zip(normalized.schema, normalized.columns)}
                for column, key in (("trip_id", "trip_id_sum"), ("fare_cents", "fare_cents_sum")):
                    totals[key] += pc.sum(columns[column]).as_py() or 0
                totals["fare_cents_nulls"] += columns["fare_cents"].null_count
                timestamps = columns["pickup_unix_us"]
                totals["timestamp_nulls"] += timestamps.null_count
                outside = pc.or_(pc.less(timestamps, lower), pc.greater_equal(timestamps, upper))
                totals["outside_quarter_timestamps"] += pc.sum(pc.cast(outside, pa.int64())).as_py() or 0
                start += batch.num_rows
            if start != expected_rows:
                raise ValueError("Original monthly row count mismatch")
            rows += start
    if rows != TOTAL_ROWS or pq.ParquetFile(partial).metadata.num_rows != rows:
        raise ValueError("Normalized row count mismatch")
    with partial.open("rb") as stream:
        os.fsync(stream.fileno())
    partial.rename(trips)
    with (source / "taxi_zone_lookup.csv").open(newline="") as stream:
        records = list(csv.DictReader(stream))
    if len(records) != 265 or len({int(row["LocationID"]) for row in records}) != 265:
        raise ValueError("Zone key/count mismatch")
    zone_schema = pa.schema([pa.field("zone_id", pa.int32(), nullable=False),
                            *[pa.field(name, pa.string(), nullable=False)
                              for name in ("borough", "zone", "service_zone")]])
    zones = pa.Table.from_arrays([pa.array([int(row["LocationID"]) for row in records], pa.int32()),
        *[pa.array([row[key] for row in records]) for key in ("Borough", "Zone", "service_zone")]],
        schema=zone_schema)
    zone_path = destination / "zones.parquet"
    pq.write_table(zones, zone_path, compression="zstd", compression_level=3)
    with zone_path.open("rb") as stream:
        os.fsync(stream.fileno())
    report = {"schema_version": 1, "prepared_at": datetime.now(timezone.utc).isoformat(),
              "pyarrow_version": pa.__version__, "source_provenance_sha256": digest(args.provenance),
              "source_files": originals, "normalization": "federation_capacity.convert",
              "conversion_script_sha256": digest(Path(__file__).with_name("federation_capacity.py")),
              "query_time_preloading": False, "reference_aggregates": totals, "datasets": {}}
    for name, path, count, schema in (("trips", trips, rows, SCHEMA), ("zones", zone_path, 265, zone_schema)):
        report["datasets"][name] = {"file": path.name, "rows": count, "bytes": path.stat().st_size,
                                     "sha256": digest(path), "schema": str(schema)}
    (destination / "provenance.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

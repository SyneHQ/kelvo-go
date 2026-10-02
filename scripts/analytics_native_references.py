#!/usr/bin/env python3
"""Prepare direct ClickHouse correctness references on the designated Linux VM.

This is reference preparation, never a capacity benchmark. The private JSON
manifest contains environment_file, database and exactly four workflows with
inline sql.clickhouse and output contracts. No credentials, SQL or host paths
are copied to the public report. Requires the existing VM PyArrow environment.
"""
from __future__ import annotations

import argparse
import base64
import http.client
import json
import os
from pathlib import Path
import re
import signal
import stat
import sys
import traceback
from urllib.parse import parse_qsl, urlencode, urlsplit

sys.dont_write_bytecode = True

MAX_BYTES = 64 << 20
TIMEOUT = 180
WORKFLOWS = {"daily_rolling_kpis", "borough_hour_hotspots", "route_economics", "monthly_zone_momentum"}
SETTINGS = {"readonly": "1", "default_format": "ArrowStream", "max_threads": "2",
            "max_memory_usage": str(512 << 20), "max_execution_time": str(TIMEOUT),
            "max_result_bytes": str(MAX_BYTES), "result_overflow_mode": "throw",
            "output_format_arrow_compression_method": "lz4_frame", "wait_end_of_query": "0",
            "cancel_http_readonly_queries_on_client_close": "1"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def private_json(path):
    require(path.is_absolute(), "input path must be absolute")
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
            and not info.st_mode & 0o077 and info.st_size <= 2 << 20,
            "input must be an owned private regular JSON file within size limit")
    return json.loads(path.read_text())


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def save(path, data):
    with path.open("xb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    sync_directory(path.parent)


def timeout_handler(_signal, _frame):
    raise TimeoutError("reference query exceeded wall timeout")


def download(endpoint, auth, database, sql, row_limit, path):
    settings = {**SETTINGS, "database": database, "max_result_rows": str(row_limit)}
    target = (endpoint.path or "/") + "?" + urlencode(settings)
    connection_type = http.client.HTTPSConnection if endpoint.scheme == "https" else http.client.HTTPConnection
    connection = connection_type(endpoint.hostname, endpoint.port, timeout=TIMEOUT)
    previous = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(TIMEOUT)
    try:
        # http.client follows no redirects and uses no environment-configured proxy.
        connection.request("POST", target, body=sql.encode(), headers={
            "Authorization": auth, "Content-Type": "text/plain; charset=utf-8",
            "Accept": "application/vnd.apache.arrow.stream", "Accept-Encoding": "identity"})
        with connection.getresponse() as response, path.open("xb") as output:
            if response.status != 200:
                raise RuntimeError("ClickHouse HTTP status %d: %r" % (response.status, response.read(4096)))
            require(response.getheader("Content-Encoding", "identity") == "identity", "unexpected HTTP encoding")
            total = 0
            while block := response.read(65536):
                total += len(block)
                require(total <= MAX_BYTES, "encoded Arrow result exceeds byte limit")
                output.write(block)
            output.flush()
            os.fsync(output.fileno())
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)
        connection.close()


def verify(path, spec):
    import pyarrow as pa
    from federation_capacity import canonical_arrow

    columns = spec["columns"]
    expected = [(column["name"], column["type"]) for column in columns]
    rows = decoded = 0
    with path.open("rb") as stream, pa.ipc.open_stream(stream) as reader:
        require([(field.name, str(field.type)) for field in reader.schema] == expected, "result schema differs from contract")
        for batch in reader:
            rows += batch.num_rows
            decoded += batch.nbytes
            require(rows <= spec["max_rows"] and decoded <= MAX_BYTES, "decoded result exceeds limits")
            require(all(column["nullable"] or values.null_count == 0
                        for column, values in zip(columns, batch.columns)), "unexpected result NULL")
        require(not stream.read(1), "bytes follow Arrow end marker")
    require("expected_rows" not in spec or rows == spec["expected_rows"], "result row count differs from contract")
    # Shared canonical decoder additionally requires the successful Arrow EOS.
    return {**canonical_arrow(path), "decoded_arrow_bytes": decoded}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    parser.add_argument("--report", required=True, type=Path)
    args = parser.parse_args()
    require(sys.platform.startswith("linux"), "run reference preparation on the designated Linux VM")
    os.umask(0o077)
    manifest = private_json(args.manifest)
    database = manifest["database"]
    require(isinstance(database, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", database), "invalid database identifier")
    workflows = manifest["workflows"]
    require(set(workflows) == WORKFLOWS, "manifest must contain exactly the four analytical workflows")
    for spec in workflows.values():
        require(isinstance(spec["sql"]["clickhouse"], str) and 0 < len(spec["sql"]["clickhouse"]) <= 65536, "invalid inline SQL")
        output = spec["output"]
        require(type(output["max_rows"]) is int and 0 < output["max_rows"] <= 100000, "invalid output row limit")
        columns = output["columns"]
        require(isinstance(columns, list) and 0 < len(columns) <= 64, "invalid output columns")
        require(len({column["name"] for column in columns}) == len(columns), "duplicate output columns")
        for column in columns:
            require(re.fullmatch(r"[a-z][a-z0-9_]{0,63}", column["name"])
                    and column["type"] in ("int32", "int64", "string")
                    and type(column["nullable"]) is bool, "invalid output column")
    environment = private_json(Path(manifest["environment_file"]))
    endpoint = urlsplit(environment["KELVO_SOURCE_WORKFLOW_URL"])
    require(endpoint.scheme in ("http", "https") and endpoint.hostname and endpoint.username is None
            and endpoint.password is None and not endpoint.fragment, "invalid source URL")
    parameters = parse_qsl(endpoint.query, keep_blank_values=True, strict_parsing=True)
    require(not parameters or parameters == [("database", database)], "unexpected source URL parameters")
    username, password = (environment["KELVO_SOURCE_WORKFLOW_" + key] for key in ("USER", "PASSWORD"))
    require(isinstance(username, str) and username and ":" not in username and isinstance(password, str), "invalid credentials")
    auth = "Basic " + base64.b64encode((username + ":" + password).encode()).decode()
    directory = args.output_directory
    require(directory.is_absolute() and not directory.exists() and not directory.is_symlink(), "output directory must be new and absolute")
    require(args.report.is_absolute() and not args.report.exists() and not args.report.is_symlink(), "report must be new and absolute")
    directory.mkdir(mode=0o700)
    report = {"schema_version": 1, "purpose": "direct_clickhouse_correctness_reference_preparation",
              "timed_capacity_evidence": False, "state": "failed", "all_references_verified": False,
              "source_limits": SETTINGS, "wire_and_decoded_byte_limit": MAX_BYTES, "workflows": {}}
    for name in sorted(WORKFLOWS):
        partial = directory / (name + ".partial")
        try:
            spec = workflows[name]
            sql = spec["sql"]["clickhouse"].format(trips=database + ".trips", zones=database + ".zones")
            download(endpoint, auth, database, sql, spec["output"]["max_rows"], partial)
            values = verify(partial, spec["output"])
            partial.rename(directory / (name + ".arrow"))
            sync_directory(directory)
            report["workflows"][name] = {"state": "verified", **values}
        except Exception as error:
            save(directory / (name + ".error.log"), traceback.format_exc().encode()[-65536:])
            report["workflows"][name] = {"state": "failed", "error_type": type(error).__name__}
    report["all_references_verified"] = all(value["state"] == "verified" for value in report["workflows"].values())
    report["state"] = "verified" if report["all_references_verified"] else "failed"
    save(args.report, (json.dumps(report, indent=2, allow_nan=False) + "\n").encode())
    print(json.dumps({"state": report["state"], "all_references_verified": report["all_references_verified"]}))
    return 0 if report["all_references_verified"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"state": "failed", "error_type": type(error).__name__}), file=sys.stderr)
        raise SystemExit(1)

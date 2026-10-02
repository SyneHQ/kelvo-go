#!/usr/bin/env python3
"""Real SQL Server + ClickHouse + CSV federation acceptance on the Linux VM.

Actions: prepare, run, cleanup, status. All credentials/certificates live under
a mode-0700 private root. SQL Server has a unique Docker network, loopback
port, generated TLS certificate, SELECT-only login and bounded resources. The
existing ClickHouse container is never restarted or reconfigured; only a new
fixture database and reader are created. The private trust wrapper changes no
host trust files. A successful run removes only this script's owned resources.

Uses repository federation_relational_acceptance helpers and the VM's PyArrow
environment. This script never compiles or installs packages.
"""

from __future__ import annotations

import argparse
import copy
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time
import traceback
from urllib.parse import quote, urlencode

import pyarrow as pa

from federation_relational_acceptance import (
    digest, literal, private_directory, private_write, require, wait_until,
    write_trust_wrapper,
)


ROOT = Path(__file__).resolve().parents[1]
DOCKER = ["sudo", "-n", "docker"]
IMAGE = "mcr.microsoft.com/mssql/server:2022-CU20-ubuntu-22.04"
LABEL = "org.synehq.kelvo.sql-federation-fixture"
EOS = b"\xff\xff\xff\xff\0\0\0\0"
READER = "kelvo_reader"
DSN_KEY = "KELVO_SOURCE_SQL_ACCEPTANCE_DSN"
PRIVATE_DEFAULT = ROOT.parent / "private-sqlserver"


class Fixture:
    def __init__(self, base, directory=None):
        self.base = Path(base).resolve()
        if directory is None:
            directory = json.loads((self.base / "current.json").read_text())["directory"]
        self.directory = Path(directory).resolve()
        require(self.directory.is_relative_to(self.base) and self.directory.name.startswith("run-"),
                "fixture directory is outside its private root")
        self.manifest = json.loads((self.directory / "manifest.json").read_text())
        environment = self.directory / "environment.json"
        self.environment = json.loads(environment.read_text()) if environment.exists() else {}

    def save(self):
        private_write(self.directory / "manifest.json", json.dumps(self.manifest, indent=2))

    def run(self, command, *, data=None, env=None, timeout=40, check=True):
        result = subprocess.run(command, input=data, env=env, capture_output=True, timeout=timeout)
        if check and result.returncode:
            private_write(self.directory / "fixture-command-error.log", result.stdout + b"\n" + result.stderr)
            raise RuntimeError("owned fixture command failed; private diagnostic retained")
        return result

    def docker(self, *args, **kwargs):
        return self.run(DOCKER + list(args), **kwargs)

    def inspect_optional(self, kind, name):
        result = self.docker(*(["network"] if kind == "network" else []), "inspect", name, check=False)
        if result.returncode:
            message = result.stderr.decode(errors="replace").lower()
            if "no such" in message or "not found" in message:
                return None
            raise RuntimeError("could not inspect owned Docker resource")
        return json.loads(result.stdout)[0]

    def admin_command(self, database=None):
        command = DOCKER + ["exec", "-i", "--env-file", str(self.directory / "admin-client.env"), self.manifest["container"],
            "/opt/mssql-tools18/bin/sqlcmd", "-S", "localhost", "-U", "sa", "-N", "-C",
            "-b", "-h", "-1", "-W", "-r", "1", "-l", "5"]
        if database:
            command += ["-d", database]
        return command

    def admin(self, sql, *, database=None, check=True, timeout=40):
        # -C is only for the local administrator's bootstrap. Kelvo readers
        # require verified TLS; unknown CA/name controls test that separately.
        return self.run(self.admin_command(database), data=("SET NOCOUNT ON;\nGO\n" + sql).encode(),
                        check=check, timeout=timeout)

    def value(self, sql, **kwargs):
        return self.admin(sql, **kwargs).stdout.decode().strip()

    def ch(self, sql, *, check=True):
        return self.docker("exec", "-i", self.manifest["clickhouse"], "clickhouse-client", "--multiquery",
                           data=sql.encode(), check=check)

    def ch_rows(self):
        return int(self.ch("SELECT coalesce(sum(rows),0) FROM system.parts WHERE active "
            "AND database='kelvo_bench' AND table='fact_events'").stdout.decode().strip())

    def source(self, table="fact", *, max_rows=100000, max_bytes=32 << 20):
        return {"id": "sql", "type": "sqlserver", "dsn_env": DSN_KEY, "federation": {
            "tables": [{"name": table, "database": self.manifest["database"], "schema": "dbo", "table": table}],
            "max_scan_rows": max_rows, "max_scan_bytes": max_bytes}}

    def configuration(self, directory, *, table="fact", max_rows=100000, max_bytes=32 << 20):
        sources = [self.source(table, max_rows=max_rows, max_bytes=max_bytes),
            {"id": "ch", "type": "clickhouse", "url_env": "KELVO_SOURCE_SQL_CH_URL",
             "username_env": "KELVO_SOURCE_SQL_CH_USER", "password_env": "KELVO_SOURCE_SQL_CH_PASSWORD",
             "federation": {"tables": [{"name": "dimensions", "database": self.manifest["database"],
                                         "table": "dimensions"}], "max_scan_rows": 100000,
                            "max_scan_bytes": 32 << 20}},
            {"id": "labels", "type": "csv", "path": str(self.directory / "labels.csv")}]
        hidden = copy.deepcopy(sources[0])
        hidden["id"], hidden["dsn_env"] = "unselected", "KELVO_SOURCE_SQL_UNSELECTED_DSN"
        sources.append(hidden)
        path = directory / ("catalog-" + secrets.token_hex(5) + ".json")
        private_write(path, json.dumps({"sources": sources}, indent=2))
        return path


def prepare(args):
    require(sys.platform.startswith("linux") and os.geteuid() != 0, "run as the ordinary designated VM user")
    require(re.fullmatch(r"[A-Za-z0-9_.-]+", args.clickhouse), "invalid ClickHouse container name")
    os.umask(0o077)
    base = private_directory(args.private_root.resolve())
    suffix = secrets.token_hex(6)
    directory = private_directory(base / ("run-" + suffix))
    manifest = {"suffix": suffix, "container": "kelvo-sql-fed-" + suffix,
        "network": "kelvo-sql-fed-" + suffix, "database": "kelvo_sql_" + suffix,
        "clickhouse": args.clickhouse, "clickhouse_reader": "kelvo_sql_" + suffix + "_reader",
        "created_container": False, "created_network": False, "created_ch_database": False,
        "created_ch_reader": False, "host_mount_namespace": os.readlink("/proc/self/ns/mnt"),
        "host_ca_sha256": digest("/etc/ssl/certs/ca-certificates.crt"), "image_tag": args.image,
        "container_limits": {"memory_bytes": 3 << 30, "cpus": 2, "pids": 512}}
    private_write(directory / "manifest.json", json.dumps(manifest))
    fixture = Fixture(base, directory)
    fixture.manifest = manifest
    private_write(base / "current.json", json.dumps({"directory": str(directory)}))
    try:
        image = json.loads(fixture.docker("image", "inspect", args.image).stdout)[0]
        manifest["image_id"] = image["Id"]
        existing = json.loads(fixture.docker("inspect", args.clickhouse).stdout)[0]
        require(existing["State"]["Running"], "existing ClickHouse is not running")
        manifest["clickhouse_id"], manifest["clickhouse_started_at"] = existing["Id"], existing["State"]["StartedAt"]
        addresses = [n["IPAddress"] for n in existing["NetworkSettings"]["Networks"].values() if n.get("IPAddress")]
        require(addresses, "existing ClickHouse has no reachable address")
        manifest["existing_clickhouse_rows"] = fixture.ch_rows()
        fixture.save()
        certs = directory / "certs"
        certs.mkdir(mode=0o755)
        certs.chmod(0o755)
        fixture.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "3",
            "-subj", "/CN=Kelvo SQL acceptance CA", "-keyout", str(directory / "ca.key"), "-out", str(certs / "ca.crt")])
        private_write(directory / "server.ext", "subjectAltName=IP:127.0.0.1,DNS:localhost\nextendedKeyUsage=serverAuth\n")
        fixture.run(["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
            "-keyout", str(certs / "server.key"), "-out", str(directory / "server.csr")])
        fixture.run(["openssl", "x509", "-req", "-days", "3", "-in", str(directory / "server.csr"),
            "-CA", str(certs / "ca.crt"), "-CAkey", str(directory / "ca.key"), "-CAcreateserial",
            "-extfile", str(directory / "server.ext"), "-out", str(certs / "server.crt")])
        (certs / "server.key").chmod(0o600)
        fixture.run(["sudo", "-n", "chown", "10001:0", str(certs / "server.key")])
        for name in ("ca.crt", "server.crt"):
            (certs / name).chmod(0o644)
        for path in (directory / "ca.key", directory / "server.ext", directory / "server.csr", certs / "ca.srl"):
            path.unlink(missing_ok=True)
        private_write(directory / "combined-ca-bundle.pem",
            Path("/etc/ssl/certs/ca-certificates.crt").read_bytes() + b"\n" + (certs / "ca.crt").read_bytes())
        manifest["fixture_ca_sha256"] = digest(directory / "combined-ca-bundle.pem")
        write_trust_wrapper(fixture)
        admin_password = "K1!" + secrets.token_hex(24)
        reader_password = "R2!" + secrets.token_hex(24)
        ch_password = secrets.token_hex(32)
        private_write(directory / "admin-password", admin_password)
        private_write(directory / "admin-client.env", "SQLCMDPASSWORD=" + admin_password + "\n")
        private_write(directory / "server.env", "ACCEPT_EULA=Y\nMSSQL_PID=Developer\nMSSQL_SA_PASSWORD=" + admin_password + "\n")
        private_write(directory / "mssql.conf", "[network]\ntlscert = /etc/kelvo-sql/server.crt\n"
            "tlskey = /etc/kelvo-sql/server.key\ntlsprotocols = 1.2\nforceencryption = 1\n"
            "[memory]\nmemorylimitmb = 2048\n")
        (directory / "mssql.conf").chmod(0o644)
        manifest["created_network"] = True
        fixture.save()
        fixture.docker("network", "create", "--label", LABEL + "=" + suffix, manifest["network"])
        manifest["created_container"] = True
        fixture.save()
        fixture.docker("create", "--pull=never", "--name", manifest["container"], "--label", LABEL + "=" + suffix,
            "--network", manifest["network"], "--publish", "127.0.0.1::1433", "--memory", "3g", "--memory-swap", "3g",
            "--cpus", "2", "--pids-limit", "512", "--security-opt", "no-new-privileges:true",
            "--env-file", str(directory / "server.env"),
            "--mount", "type=bind,src=" + str(certs) + ",dst=/etc/kelvo-sql,readonly",
            "--mount", "type=bind,src=" + str(directory / "mssql.conf") + ",dst=/var/opt/mssql/mssql.conf,readonly", args.image)
        fixture.docker("start", manifest["container"])
        deadline = time.monotonic() + 180
        while fixture.admin("SELECT 1", check=False, timeout=10).returncode:
            if time.monotonic() >= deadline:
                logs = fixture.docker("logs", manifest["container"], check=False)
                private_write(directory / "server-startup.log", logs.stdout + logs.stderr)
                raise RuntimeError("SQL Server did not become ready; private log retained")
            time.sleep(1)
        info = json.loads(fixture.docker("inspect", manifest["container"]).stdout)[0]
        ports = info["NetworkSettings"]["Ports"]["1433/tcp"]
        require(ports is not None and len(ports) == 1 and ports[0]["HostIp"] == "127.0.0.1", "fixture port is not loopback-only")
        manifest["endpoint"] = "127.0.0.1:" + ports[0]["HostPort"]
        manifest["server_version"] = fixture.value("SELECT CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR(30))")
        database = manifest["database"]
        fixture.admin("CREATE DATABASE [" + database + "];\nGO\nCREATE LOGIN [" + READER + "] WITH PASSWORD=" + literal(reader_password) + ",CHECK_POLICY=OFF;")
        fixture.admin("CREATE USER [" + READER + "] FOR LOGIN [" + READER + "];\nGRANT SELECT ON SCHEMA::dbo TO [" + READER + "];", database=database)
        fixture.admin("CREATE TABLE dbo.fact (row_id INT NOT NULL PRIMARY KEY, i64 BIGINT NULL, amount DECIMAL(30,10) NULL,"
            " enabled BIT NULL, tiny TINYINT NULL, label NVARCHAR(80) NULL, day DATE NULL, wall DATETIME2(7) NULL, payload VARCHAR(4096) NULL);"
            " INSERT INTO dbo.fact VALUES"
            " (1,-9223372036854775808,-12345678901234567890.0123456789,1,0,N'first','1969-12-31','2026-10-01 12:30:00.1234567',REPLICATE('a',4096)),"
            " (2,9223372036854775807,12345678901234567890.0123456789,0,255,N'second','2026-10-02','2026-10-02 00:00:00.7654321',REPLICATE('b',4096)),"
            " (3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,REPLICATE('c',4096)),"
            " (5,9007199254740993,1.2500000000,1,7,N'fifth','2020-01-01','2020-01-01',REPLICATE('d',4096));"
            " CREATE TABLE dbo.hidden(marker VARCHAR(20)); INSERT INTO dbo.hidden VALUES('unregistered');"
            " CREATE TABLE dbo.slow(row_id INT NOT NULL, payload VARCHAR(4096));"
            " INSERT INTO dbo.slow VALUES(1,REPLICATE('x',4096));", database=database)
        require(fixture.ch("SELECT count() FROM system.databases WHERE name=" + literal(database)).stdout.strip() == b"0", "fixture database already exists")
        manifest["created_ch_database"] = True
        fixture.save()
        fixture.ch("CREATE DATABASE `" + database + "`")
        require(fixture.ch("SELECT count() FROM system.users WHERE name=" + literal(manifest["clickhouse_reader"])).stdout.strip() == b"0", "fixture reader already exists")
        manifest["created_ch_reader"] = True
        fixture.save()
        fixture.ch("CREATE USER `" + manifest["clickhouse_reader"] + "` IDENTIFIED WITH sha256_password BY " + literal(ch_password))
        fixture.ch("GRANT SELECT ON `" + database + "`.* TO `" + manifest["clickhouse_reader"] + "`;"
            "CREATE TABLE `" + database + "`.dimensions(row_id Int32,weight Int32) ENGINE=TinyLog;"
            "INSERT INTO `" + database + "`.dimensions VALUES(1,10),(2,20),(3,30),(4,40);")
        private_write(directory / "labels.csv", "row_id,region\n1,east\n2,east\n3,west\n5,west\n")
        fixture.environment = {DSN_KEY: "sqlserver://" + READER + ":" + quote(reader_password, safe="") + "@" + manifest["endpoint"] + "?" +
            urlencode({"database": database, "encrypt": "true", "TrustServerCertificate": "false", "connection timeout": "5", "app name": "kelvo-sql-acceptance"}),
            "KELVO_SOURCE_SQL_CH_URL": "http://" + addresses[0] + ":8123/",
            "KELVO_SOURCE_SQL_CH_USER": manifest["clickhouse_reader"], "KELVO_SOURCE_SQL_CH_PASSWORD": ch_password}
        private_write(directory / "environment.json", json.dumps(fixture.environment, indent=2))
        generated = fixture.configuration(directory)
        generated.replace(directory / "catalog.json")
        manifest["prepared"] = True
        fixture.save()
        status(fixture)
    except BaseException:
        fixture.save()
        private_write(directory / "prepare-failure.log", traceback.format_exc())
        raise RuntimeError("SQL Server fixture preparation failed; owned resources retained for diagnosis") from None


def status(fixture):
    m = fixture.manifest
    print(json.dumps({"directory": str(fixture.directory), "prepared": m.get("prepared", False),
        "stopped": m.get("stopped", False), "container": m["container"], "endpoint": m.get("endpoint"),
        "server_version": m.get("server_version"), "catalog_file": str(fixture.directory / "catalog.json"),
        "trust_wrapper": str(fixture.directory / "with-fixture-trust.sh")}, indent=2))


def cleanup(fixture):
    m = fixture.manifest
    require(re.fullmatch(r"[0-9a-f]{12}", m["suffix"]), "invalid owned fixture suffix")
    require(m["container"] == "kelvo-sql-fed-" + m["suffix"] and m["network"] == m["container"], "owned fixture name mismatch")
    require(m["database"] == "kelvo_sql_" + m["suffix"] and m["clickhouse_reader"] == m["database"] + "_reader", "owned database name mismatch")
    issues = []
    # Reconcile persisted creation intent; missing resources are harmless on retry.
    # Independent SQL cleanup must work even if the existing CH is unavailable.
    for kind in ("container", "network"):
        if not m.get("created_" + kind):
            continue
        try:
            info = fixture.inspect_optional(kind, m[kind])
            if info is not None:
                labels = info["Labels"] if kind == "network" else info["Config"]["Labels"]
                require(labels.get(LABEL) == m["suffix"], "Docker ownership label mismatch")
                fixture.docker(*(["network", "rm"] if kind == "network" else ["rm", "-f", "-v"]), m[kind])
            m["created_" + kind] = False
            fixture.save()
        except Exception:
            issues.append("owned_" + kind + "_cleanup_failed")
    ch_unchanged = False
    try:
        ch = fixture.inspect_optional("container", m["clickhouse"])
        if m.get("clickhouse_id"):
            require(ch is not None and ch["Id"] == m["clickhouse_id"] and ch["State"]["Running"], "existing ClickHouse unavailable or replaced")
            ch_unchanged = ch["State"]["StartedAt"] == m["clickhouse_started_at"] and fixture.ch_rows() == m["existing_clickhouse_rows"]
        else:
            require(not m.get("created_ch_reader") and not m.get("created_ch_database"), "ClickHouse fixture identity unavailable")
        if m.get("created_ch_reader"):
            fixture.ch("KILL QUERY WHERE user=" + literal(m["clickhouse_reader"]) + " SYNC; DROP USER IF EXISTS " + chr(96) + m["clickhouse_reader"] + chr(96) + ";")
            m["created_ch_reader"] = False
            fixture.save()
        if m.get("created_ch_database"):
            fixture.ch("DROP DATABASE IF EXISTS " + chr(96) + m["database"] + chr(96) + " SYNC")
            m["created_ch_database"] = False
            fixture.save()
        if m.get("clickhouse_id") and not ch_unchanged:
            issues.append("existing_clickhouse_lifecycle_or_rows_changed")
    except Exception:
        issues.append("owned_clickhouse_cleanup_or_preservation_check_failed")
    trust_unchanged = digest("/etc/ssl/certs/ca-certificates.crt") == m["host_ca_sha256"]
    if not trust_unchanged:
        issues.append("host_trust_changed")
    for path in (fixture.directory / "certs").glob("*"):
        path.unlink()
    for name in ("admin-password", "admin-client.env", "environment.json", "server.env", "combined-ca-bundle.pem", "with-fixture-trust.sh", "exec-selected-environment.py", "mssql.conf", "ca.key", "server.csr", "server.ext"):
        (fixture.directory / name).unlink(missing_ok=True)
    m["stopped"] = not any(m.get(key) for key in ("created_container", "created_network", "created_ch_database", "created_ch_reader"))
    fixture.save()
    return {"cleanup_complete": m["stopped"] and not issues, "issues": issues,
            "owned_container_network_volumes_removed": not m.get("created_container") and not m.get("created_network"),
            "owned_clickhouse_database_reader_removed": not m.get("created_ch_database") and not m.get("created_ch_reader"),
            "fixture_certificates_credentials_removed": True, "host_trust_unchanged": trust_unchanged,
            "existing_clickhouse_running_unchanged": ch_unchanged, "existing_clickhouse_rows": m.get("existing_clickhouse_rows")}


class Acceptance:
    def __init__(self, fixture, args):
        self.fixture, self.args = fixture, args
        self.directory = private_directory(fixture.directory / ("acceptance-" + secrets.token_hex(5)))
        self.binary = args.binary.resolve()
        self.config = fixture.configuration(self.directory)
        self.environment = dict(os.environ, **fixture.environment, GOMAXPROCS="2",
            SSL_CERT_FILE="/nonexistent/ambient-ca", KELVO_SOURCE_SQL_UNSELECTED_DSN="unselected-canary")
        self.checks, self.children = [], []
        self.stage, self.counter = "prerequisites", 0

    def record(self, name, **data):
        self.checks.append({"test": name, "passed": True, **data})
        print(name + ": passed", flush=True)

    def start(self, sql, *, sources="sql", native=False, config=None, environment=None, timeout="20s"):
        self.counter += 1
        output = self.directory / ("query-" + str(self.counter) + ".arrow")
        selection = ["--mode", "native", "--connection", sources] if native else ["--sources", sources]
        command = [str(self.binary), "query", "--config", str(config or self.config), *selection, "--sql", sql,
            "--out", str(output), "--threads", "2", "--memory-mb", "256", "--temp-mb", "64", "--timeout", timeout,
            "--max-rows", "100000", "--max-bytes", str(32 << 20)]
        process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, env=environment or self.environment, start_new_session=True)
        self.children.append(process)
        return process, output, self.counter, native

    def finish(self, running, success=True, expected_error=None):
        process, output, index, native = running
        try:
            stdout, stderr = process.communicate(timeout=35)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
            raise AssertionError("query exceeded bounded acceptance deadline") from None
        private_write(self.directory / ("query-" + str(index) + ".log"), stdout + b"\n" + stderr)
        require((process.returncode == 0) == success, "CLI query returned unexpected status")
        if not success:
            require(not output.exists(), "failed query published an Arrow result")
            if expected_error:
                require(expected_error in (stdout + stderr).decode(errors="replace"), "query failed for an unexpected reason")
            return None, {"nonzero_exit": True, "no_export": True, "expected_error": expected_error}
        wire = output.read_bytes()
        require(wire.endswith(EOS), "Arrow stream is incomplete")
        table = pa.ipc.open_stream(wire).read_all()
        stats = json.loads(stderr)
        require(stats.get("backend") == ("sqlserver" if native else "duckdb"), "query used wrong backend")
        if not native:
            require(stats.get("federation"), "query omitted custom federation statistics")
        return table, {"rows": table.num_rows, "backend": stats["backend"], "federation": stats.get("federation", [])}

    def query(self, sql, *, success=True, expected_error=None, **kwargs):
        return self.finish(self.start(sql, **kwargs), success, expected_error)

    def native_and_federated_types(self):
        self.stage = "native_and_federated_scalar_fidelity"
        columns = "row_id,i64,amount,enabled,tiny,label,day,wall"
        native, detail = self.query("SELECT " + columns + " FROM dbo.fact ORDER BY row_id", native=True)
        types = {field.name: field.type for field in native.schema}
        require(types["row_id"] == pa.int32() and types["i64"] == pa.int64() and types["tiny"] == pa.uint8(), "native integer widths changed")
        require(types["enabled"] == pa.bool_() and types["amount"] == pa.decimal128(30, 10) and types["day"] == pa.date32(), "native scalar schema changed")
        require(types["wall"] == pa.timestamp("ns"), "native datetime2 precision changed")
        rows = native.drop(["wall"]).to_pylist()
        require([r["i64"] for r in rows] == [-(1 << 63), (1 << 63) - 1, None, 9007199254740993], "int64 precision changed")
        require([r["amount"] for r in rows] == [Decimal("-12345678901234567890.0123456789"), Decimal("12345678901234567890.0123456789"), None, Decimal("1.2500000000")], "decimal precision changed")
        require([r["enabled"] for r in rows] == [True, False, None, True] and [r["tiny"] for r in rows] == [0, 255, None, 7], "BIT/TINYINT/NULL changed")
        require(rows[0]["day"] == date(1969, 12, 31) and all(v is None for k, v in rows[2].items() if k != "row_id"), "NULL/date values changed")
        epoch = int(datetime(2026, 10, 1, 12, 30, tzinfo=timezone.utc).timestamp()) * 1_000_000_000 + 123456700
        require(native.column("wall").cast(pa.int64())[0].as_py() == epoch, "datetime2 submicrosecond digits changed")
        self.record("native_scalar_fidelity", arrow_types={k: str(v) for k, v in types.items()}, **detail)
        federated, detail = self.query("SELECT " + columns + " FROM sql.fact ORDER BY row_id")
        require({f.name: f.type for f in federated.schema} == types, "federation changed scalar Arrow types")
        require(federated.drop(["wall"]).to_pylist() == rows, "federated values differ from exact native values")
        require(federated.column("wall").cast(pa.int64()).to_pylist() == native.column("wall").cast(pa.int64()).to_pylist(), "federation changed datetime2 integer values")
        self.record("federated_scalar_fidelity", arrow_types={f.name: str(f.type) for f in federated.schema}, **detail)
        for name, predicate, expected in [
            ("int64_min", "i64=(-9223372036854775807::BIGINT-1)", [1]),
            ("int64_max", "i64=9223372036854775807::BIGINT", [2]),
            ("bit_true", "enabled=true", [1, 5]), ("bit_false", "enabled=false", [2]),
            ("null", "i64 IS NULL", [3]), ("range", "row_id>=2 AND row_id<5", [2, 3]),
        ]:
            table, detail = self.query("SELECT row_id FROM sql.fact WHERE " + predicate + " ORDER BY row_id")
            require(table.column(0).to_pylist() == expected, "typed predicate changed result")
            self.record(name + "_predicate", **detail)
        table, detail = self.query("SELECT count(*) AS n FROM sql.fact")
        require(table.to_pylist() == [{"n": 4}], "count-only scan changed rows")
        self.record("count_only_scan", **detail)
        table, detail = self.query("SELECT a.row_id AS id,b.i64 FROM sql.fact a JOIN sql.fact b ON a.row_id=b.row_id ORDER BY id")
        require(table.column("id").to_pylist() == [1, 2, 3, 5] and sum(s["scans"] for s in detail["federation"]) >= 2,
                "self-join did not preserve independent scans")
        self.record("independent_self_join_scans", **detail)

    def three_source_query(self):
        self.stage = "sqlserver_clickhouse_csv_cte_join_window"
        sql = """WITH matched AS (
            SELECT s.row_id,c.weight,l.region,
                CASE WHEN s.enabled IS NULL THEN 'missing' WHEN s.enabled THEN 'enabled' ELSE 'disabled' END AS state
            FROM sql.fact s JOIN ch.dimensions c ON s.row_id=c.row_id
            JOIN labels l ON l.row_id=s.row_id
        ) SELECT region,row_id,weight,state,
            row_number() OVER (PARTITION BY region ORDER BY row_id) AS rn,
            sum(weight) OVER (PARTITION BY region) AS region_weight
        FROM matched ORDER BY row_id"""
        table, detail = self.query(sql, sources="sql,ch,labels")
        expected = [{"region": "east", "row_id": 1, "weight": 10, "state": "enabled", "rn": 1, "region_weight": 30},
                    {"region": "east", "row_id": 2, "weight": 20, "state": "disabled", "rn": 2, "region_weight": 30},
                    {"region": "west", "row_id": 3, "weight": 30, "state": "missing", "rn": 1, "region_weight": 30}]
        require(table.to_pylist() == expected, "three-source CTE/join/window results changed")
        require({s["source"] for s in detail["federation"]} == {"sql", "ch"}, "three-source query skipped a native source")
        self.record("sqlserver_clickhouse_csv_cte_join_window", expected_rows=expected, **detail)

    def boundaries(self):
        self.stage = "readonly_tls_and_limits"
        denied = self.fixture.admin("EXECUTE AS LOGIN=" + literal(READER) + "; INSERT INTO dbo.hidden VALUES('forbidden'); REVERT;",
                                    database=self.fixture.manifest["database"], check=False)
        require(denied.returncode != 0 and self.fixture.value("SELECT count(*) FROM dbo.hidden", database=self.fixture.manifest["database"]) == "1", "reader can write source data")
        self.record("database_reader_write_denied")
        for name, sql in [("unregistered_table", "SELECT marker FROM sql.hidden"),
                          ("unselected_source", "SELECT row_id FROM unselected.fact")]:
            _, detail = self.query(sql, success=False)
            self.record(name + "_denied", **detail)
        for name, sql in [("write", "DELETE FROM dbo.fact"), ("multiple_statements", "SELECT 1; SELECT 2")]:
            _, detail = self.query(sql, native=True, success=False)
            self.record("native_" + name + "_denied", **detail)
        for name, replacement in [("unverified_tls", {"TrustServerCertificate=false": "TrustServerCertificate=true"}),
                                  ("hostname_mismatch", {})]:
            env = dict(self.environment)
            if replacement:
                for old, new in replacement.items():
                    env[DSN_KEY] = env[DSN_KEY].replace(old, new)
            else:
                env[DSN_KEY] += "&hostNameInCertificate=wrong.invalid"
            _, detail = self.query("SELECT row_id FROM sql.fact", environment=env, success=False)
            self.record(name + "_rejected", **detail)
        config = self.fixture.configuration(self.directory, max_rows=2)
        _, detail = self.query("SELECT count(*) AS n FROM sql.fact", config=config, success=False)
        self.record("source_row_budget_fails_closed", **detail)
        config = self.fixture.configuration(self.directory, max_bytes=1024)
        _, detail = self.query("SELECT sum(length(payload)) FROM sql.fact", config=config, success=False)
        self.record("source_byte_budget_fails_closed", **detail)

    def active_reader(self):
        return int(self.fixture.value("SELECT count(*) FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s "
            "ON r.session_id=s.session_id WHERE s.login_name=" + literal(READER)))

    def blocked_reader(self):
        return self.fixture.value("SELECT r.wait_type FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s "
            "ON r.session_id=s.session_id WHERE s.login_name=" + literal(READER) + " AND r.wait_type LIKE 'LCK_M_%'")

    def worker(self, process):
        pids = set()
        for path in Path("/proc/" + str(process.pid) + "/task").glob("*/children"):
            try:
                pids.update(int(v) for v in path.read_text().split())
            except FileNotFoundError:
                pass
        for pid in pids:
            try:
                parts = Path("/proc/" + str(pid) + "/cmdline").read_bytes().split(b"\0")
                if len(parts) > 1 and parts[1] == b"worker":
                    names = {v.split(b"=", 1)[0].decode() for v in Path("/proc/" + str(pid) + "/environ").read_bytes().split(b"\0") if b"=" in v}
                    return pid, names
            except FileNotFoundError:
                pass
        return None

    def cancellation(self):
        self.stage = "live_blocked_scan_cancellation"
        marker = secrets.token_hex(12)
        sql = "SET NOCOUNT ON; SET CONTEXT_INFO 0x" + marker + ";\nGO\nBEGIN TRAN; SELECT count(*) FROM dbo.slow WITH(TABLOCKX,HOLDLOCK); WAITFOR DELAY '00:00:45'; ROLLBACK;"
        locker = subprocess.Popen(self.fixture.admin_command(self.fixture.manifest["database"]),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        locker.stdin.write(sql.encode())
        locker.stdin.close()
        locker.stdin = None
        lock_query = "SELECT count(*) FROM sys.dm_exec_sessions s JOIN sys.dm_tran_locks l ON s.session_id=l.request_session_id WHERE SUBSTRING(s.context_info,1,12)=0x" + marker + " AND l.request_mode='X' AND l.request_status='GRANT'"
        try:
            require(wait_until(lambda: int(self.fixture.value(lock_query)) > 0, 10), "owned blocking lock was not acquired")
            config = self.fixture.configuration(self.directory, table="slow")
            running = self.start("SELECT sum(length(payload)) AS n FROM sql.slow", config=config, timeout="30s")
            require(wait_until(lambda: bool(self.blocked_reader()), 10), "blocked source scan was not observed")
            wait_type = self.blocked_reader()
            observed = self.worker(running[0])
            require(observed is not None, "active query worker was not observed")
            pid, names = observed
            require({n for n in names if n.startswith("KELVO_SOURCE_")} == {DSN_KEY}, "worker received unrelated source credentials")
            require("SSL_CERT_FILE" not in names, "worker inherited ambient TLS overrides")
            transport = self.fixture.value("SELECT c.encrypt_option FROM sys.dm_exec_connections c JOIN sys.dm_exec_sessions s ON c.session_id=s.session_id WHERE s.login_name=" + literal(READER))
            require(transport == "TRUE", "SQL Server reader connection was not encrypted")
            started = time.monotonic()
            running[0].send_signal(signal.SIGINT)
            _, detail = self.finish(running, success=False, expected_error="Query cancelled")
            client_seconds = time.monotonic() - started
            require(client_seconds < 3, "client cancellation exceeded cleanup bound")
            require(wait_until(lambda: not Path("/proc/" + str(pid)).exists(), 3), "cancelled worker remained alive")
            require(wait_until(lambda: self.active_reader() == 0, 5), "cancelled source query remained active")
            self.record("live_cancellation_verified_tls_selected_environment", client_seconds=client_seconds,
                source_cleanup_seconds=time.monotonic() - started, source_encrypted=True,
                observed_source_wait_type=wait_type,
                worker_environment_names=sorted(names), source_query_stopped=True, worker_stopped=True, **detail)
            deadline_run = self.start("SELECT sum(length(payload)) AS n FROM sql.slow", config=config, timeout="3s")
            require(wait_until(lambda: bool(self.blocked_reader()), 2), "deadline invocation never reached blocked source")
            _, detail = self.finish(deadline_run, success=False, expected_error="Query deadline exceeded")
            require(wait_until(lambda: self.active_reader() == 0, 5), "timed-out source query remained active")
            self.record("source_deadline_cleanup", blocked_source_observed=True, **detail)
        finally:
            raw = self.fixture.value("SELECT session_id FROM sys.dm_exec_sessions WHERE SUBSTRING(context_info,1,12)=0x" + marker)
            for value in raw.splitlines():
                if value.strip().isdigit():
                    self.fixture.admin("KILL " + value.strip(), check=False)
            try:
                stdout, stderr = locker.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                locker.kill()
                stdout, stderr = locker.communicate()
            private_write(self.directory / "owned-locker.log", stdout + b"\n" + stderr)
        table, detail = self.query("SELECT count(*) AS n FROM sql.fact")
        require(table.to_pylist() == [{"n": 4}], "query did not recover after cancellation")
        self.record("post_cancellation_query_recovery", **detail)

    def verify(self):
        require(os.geteuid() != 0, "acceptance must run unprivileged")
        require(os.readlink("/proc/self/ns/mnt") != self.fixture.manifest["host_mount_namespace"], "private trust namespace missing")
        require(digest("/etc/ssl/certs/ca-certificates.crt") == self.fixture.manifest["fixture_ca_sha256"], "fixture CA not mounted")
        inspected = json.loads(self.fixture.docker("inspect", self.fixture.manifest["container"]).stdout)[0]
        settings = inspected["HostConfig"]
        require(settings["Memory"] == 3 << 30 and settings["MemorySwap"] == 3 << 30 and
                settings["NanoCpus"] == 2_000_000_000 and settings["PidsLimit"] == 512,
                "actual SQL Server fixture resource limits differ")
        ports = inspected["NetworkSettings"]["Ports"]["1433/tcp"]
        require(len(ports) == 1 and ports[0]["HostIp"] == "127.0.0.1", "fixture port is not loopback-only")
        self.record("fixture_resource_limits_and_loopback", memory_bytes=settings["Memory"],
                    memory_and_swap_bytes=settings["MemorySwap"], cpus=2, pids=settings["PidsLimit"],
                    host_ip=ports[0]["HostIp"])
        self.native_and_federated_types()
        self.three_source_query()
        self.boundaries()
        self.cancellation()

    def close(self):
        for process in self.children:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)


def run(args, fixture):
    require(args.binary is not None and args.binary.is_file(), "a built bridge binary is required")
    require(not fixture.manifest.get("stopped"), "fixture has already been removed")
    require(digest("/etc/ssl/certs/ca-certificates.crt") == fixture.manifest["host_ca_sha256"], "run outside private trust namespace")
    control = Acceptance(fixture, args)
    try:
        _, detail = control.query("SELECT row_id FROM sql.fact", success=False)
        control.record("unknown_ca_rejected", **detail)
        private_write(fixture.directory / "unknown-ca-control.json", json.dumps(control.checks))
    finally:
        control.close()
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run([str(fixture.directory / "with-fixture-trust.sh"), sys.executable, str(Path(__file__).resolve()),
        "_verify", "--private-root", str(fixture.base), "--fixture", str(fixture.directory), "--binary", str(args.binary.resolve()),
        "--output", str(output)])
    require(result.returncode == 0, "live SQL Server acceptance failed; private fixture retained")
    evidence = json.loads(output.read_text())
    if not args.keep_fixture:
        evidence["cleanup"] = cleanup(fixture)
        private_write(output, json.dumps(evidence, indent=2))
        require(evidence["cleanup"]["cleanup_complete"], "fixture cleanup or source preservation checks failed")
    print(json.dumps({"passed": True, "evidence": str(output), "fixture_removed": not args.keep_fixture}, indent=2))


def verify(args, fixture):
    acceptance = Acceptance(fixture, args)
    evidence = {"passed": False, "scope": "Live SQL Server 2022 CU20, existing ClickHouse and local CSV through tagged Kelvo CLI/workers. Synthetic correctness fixtures; not a throughput benchmark or cloud/Oracle acceptance.",
        "binary_sha256": digest(args.binary), "sqlserver_version": fixture.manifest["server_version"],
        "sqlserver_image_id": fixture.manifest["image_id"], "container_limits": fixture.manifest["container_limits"],
        "tls_scope": "Force-encrypted SQL Server with generated server identity trusted only inside a private mount namespace. CLI/worker run as ordinary VM user; host trust store unchanged.",
        "checks": json.loads((fixture.directory / "unknown-ca-control.json").read_text())}
    try:
        acceptance.verify()
        evidence["passed"] = True
    except BaseException:
        private_write(acceptance.directory / "failure.log", traceback.format_exc())
        evidence["failed_stage"] = acceptance.stage
        raise RuntimeError("live SQL Server acceptance failed at " + acceptance.stage + "; private diagnostic retained") from None
    finally:
        acceptance.close()
        evidence["checks"] += acceptance.checks
        private_write(args.output, json.dumps(evidence, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "run", "cleanup", "status", "_verify"))
    parser.add_argument("--private-root", type=Path, default=PRIVATE_DEFAULT)
    parser.add_argument("--fixture", type=Path)
    parser.add_argument("--clickhouse", default="kelvo-clickhouse")
    parser.add_argument("--image", default=IMAGE, help="already cached VM image; never pulled by this script")
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--output", type=Path, default=ROOT.parent / "evidence/sqlserver-federation.json")
    parser.add_argument("--keep-fixture", action="store_true")
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args)
        return
    fixture = Fixture(args.private_root, args.fixture)
    if args.action == "run":
        run(args, fixture)
    elif args.action == "_verify":
        verify(args, fixture)
    elif args.action == "cleanup":
        outcome = cleanup(fixture)
        print(json.dumps(outcome, indent=2))
        require(outcome["cleanup_complete"], "fixture cleanup or preservation checks failed")
    else:
        status(fixture)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise SystemExit(1)

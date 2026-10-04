#!/usr/bin/env python3
"""Bounded Date32 acceptance against a fresh, SELECT-only ClickHouse fixture.

Requires an already-built native test binary and installed pyarrow. Nothing is
built or downloaded here. Cleanup is also an independent action so the owning
service controller can remove the fixture after killing a timed-out test unit.
"""
from __future__ import annotations

import argparse
import base64
from datetime import date, datetime, timedelta, timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import signal
import stat
import subprocess
import sys
import time
from urllib.parse import urlencode, urlsplit

import pyarrow as pa
from federation_acceptance import Observer, private_directory, private_write, require, wait_until

ROOT = Path(__file__).resolve().parents[1]
LIVE_ROOT = "TestDate32LiveClickHouse"
MAX_BODY = 8 << 20
DAYS = ["1900-01-01", "1969-12-30", "1969-12-31", "1970-01-01", "1970-01-02",
        "1999-12-31", "2000-02-28", "2000-02-29", "2000-03-01", "2024-02-29",
        "2299-12-31", None, None, "1970-01-01"]


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_private(path):
    info=path.lstat()
    require(stat.S_ISREG(info.st_mode) and not path.is_symlink() and info.st_nlink==1 and info.st_uid==os.getuid()
            and stat.S_IMODE(info.st_mode)==0o600 and info.st_size<65536,"private bounded manifest required")
    return json.loads(path.read_text())


def immutable_receipt(path,value,identity):
    if path.exists() or path.is_symlink():
        previous=read_private(path)
        require(all(previous.get(k)==v for k,v in identity.items()),"receipt identity differs")
        return previous
    with path.open("x") as output:
        os.chmod(path,0o600);json.dump(value,output,indent=2);output.write("\n");output.flush();os.fsync(output.fileno())
    descriptor=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)
    return value


def cases():
    result = []
    def add(name, sql, reference, *, date_push=False):
        result.append({"name": name, "sql": sql, "reference": reference, "date_push": date_push})
    for name, op in (("eq", "="), ("ne", "<>"), ("lt", "<"), ("le", "<="), ("gt", ">"), ("ge", ">=")):
        add("epoch_" + name, f"SELECT id FROM warehouse.events WHERE day {op} DATE '1970-01-01' ORDER BY id",
            f"SELECT id FROM {{events}} WHERE toInt32(day) {op} 0 ORDER BY id", date_push=True)
    for name, literal, days in (("leap", "DATE '2000-02-29'", 11016),
                               ("finite_min", "DATE '1970-01-01' - 2147483646", -2147483646),
                               ("finite_max", "DATE '1970-01-01' + 2147483646", 2147483646),
                               ("negative_infinity", "DATE '-infinity'", -2147483647),
                               ("positive_infinity", "DATE 'infinity'", 2147483647),
                               ("before_source_calendar", "DATE '1970-01-01' - 4000000", -4000000),
                               ("after_source_calendar", "DATE '1970-01-01' + 4000000", 4000000)):
        add(name, f"SELECT id FROM warehouse.events WHERE day = ({literal}) ORDER BY id",
            f"SELECT id FROM {{events}} WHERE toInt32(day) = {days} ORDER BY id", date_push=True)
    add("classic_date", "SELECT id FROM warehouse.events WHERE classic >= DATE '2000-02-29' ORDER BY id",
        "SELECT id FROM {events} WHERE toInt32(classic) >= 11016 ORDER BY id", date_push=True)
    add("null_and_or", "SELECT id FROM warehouse.events WHERE (day < DATE '1970-01-01' OR day IS NULL) AND id >= 1 ORDER BY id",
        "SELECT id FROM {events} WHERE (toInt32(day) < 0 OR day IS NULL) AND id >= 1 ORDER BY id", date_push=True)
    add("not_null", "SELECT id FROM warehouse.events WHERE day IS NOT NULL ORDER BY id",
        "SELECT id FROM {events} WHERE day IS NOT NULL ORDER BY id")
    add("projection_reorder", "SELECT classic,day,id,label FROM warehouse.events WHERE day >= DATE '1970-01-01' ORDER BY id",
        "SELECT classic,day,id,label FROM {events} WHERE toInt32(day) >= 0 ORDER BY id", date_push=True)
    add("count_only", "SELECT count(*) AS n FROM warehouse.events WHERE day = DATE '2000-02-29'",
        "SELECT CAST(count() AS Int64) AS n FROM {events} WHERE toInt32(day) = 11016", date_push=True)
    add("mixed_residual", "SELECT id,label FROM warehouse.events WHERE day >= DATE '1970-01-01' AND length(label)>1 ORDER BY id",
        "SELECT id,label FROM {events} WHERE toInt32(day) >= 0 AND length(label)>1 ORDER BY id", date_push=True)
    add("self_join", "SELECT a.id AS left_id,b.id AS right_id FROM warehouse.events a JOIN warehouse.events b ON a.id=b.id WHERE a.day >= DATE '1970-01-01' AND b.classic <= DATE '2000-02-29' ORDER BY left_id,right_id",
        "SELECT a.id AS left_id,b.id AS right_id FROM {events} a JOIN {events} b ON a.id=b.id WHERE toInt32(a.day)>=0 AND toInt32(b.classic)<=11016 ORDER BY left_id,right_id", date_push=True)
    add("independent_aliases", "SELECT a.id AS left_id,b.id AS right_id FROM warehouse.events a JOIN warehouse.peer b ON a.id=b.id WHERE a.day >= DATE '1970-01-01' AND b.day <= DATE '2000-02-29' ORDER BY left_id,right_id",
        "SELECT a.id AS left_id,b.id AS right_id FROM {events} a JOIN {events} b ON a.id=b.id WHERE toInt32(a.day)>=0 AND toInt32(b.day)<=11016 ORDER BY left_id,right_id", date_push=True)
    add("outer_join", "SELECT a.id AS left_id,b.id AS right_id FROM warehouse.events a LEFT JOIN warehouse.peer b ON a.id=b.id AND b.day < DATE '1970-01-01' WHERE a.day >= DATE '1970-01-01' ORDER BY left_id,right_id",
        "SELECT a.id AS left_id,b.id AS right_id FROM {events} a LEFT JOIN {events} b ON a.id=b.id AND toInt32(b.day)<0 WHERE toInt32(a.day)>=0 ORDER BY left_id,right_id", date_push=True)
    add("cte_window", "WITH selected AS (SELECT id,amount FROM warehouse.events WHERE day >= DATE '1970-01-01') SELECT id,CAST(sum(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS BIGINT) AS running_amount FROM selected ORDER BY id",
        "WITH selected AS (SELECT id,amount FROM {events} WHERE toInt32(day)>=0) SELECT id,CAST(sum(amount) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Int64) AS running_amount FROM selected ORDER BY id", date_push=True)
    add("repeated_scan", "SELECT id FROM warehouse.events WHERE day < DATE '1970-01-01' UNION ALL SELECT id FROM warehouse.peer WHERE day > DATE '2000-02-29' ORDER BY id",
        "SELECT id FROM {events} WHERE toInt32(day)<0 UNION ALL SELECT id FROM {events} WHERE toInt32(day)>11016 ORDER BY id", date_push=True)
    return result


def normalized(table):
    kinds = {pa.int32(): "INTEGER", pa.int64(): "BIGINT", pa.date32(): "DATE", pa.string(): "VARCHAR"}
    types = []
    for field in table.schema:
        require(field.type in kinds, "unexpected reference Arrow type")
        types.append(kinds[field.type])
    rows = []
    for index in range(table.num_rows):
        row = []
        for column in table.columns:
            value = column[index].as_py()
            row.append(value.isoformat() if isinstance(value, date) else value)
        rows.append(row)
    return {"names": table.schema.names, "types": types, "rows": rows}


def same_typed_rows(left, right):
    # JSON preserves bool versus numeric spelling, unlike Python's True == 1.
    return all(json.dumps(left[key], sort_keys=True, separators=(",", ":")) ==
               json.dumps(right[key], sort_keys=True, separators=(",", ":")) for key in ("names", "types", "rows"))


def filters(plan):
    for item in plan.get("filters") or []:
        stack = [item]
        while stack:
            item = stack.pop()
            yield item
            stack.extend(item.get("children", []))


def validate_phase(case, mode, report, reference, events):
    require(report.get("passed") is True and report.get("name") == case["name"] and report.get("mode") == mode,
            "missing or mismatched successful live result")
    require(same_typed_rows(report, reference), "typed live/source result mismatch")
    require(report.get("active_readers") == 0 and report.get("seconds", -1) >= 0, "live reader ownership or timing missing")
    require(set(report.get("stats", {})) == {"events", "peer"}, "live table counters missing")
    require(report.get("plans"), "actual bound scan plans missing")
    require(all(report.get("schema",{}).get(alias+"."+column)=="date32" for alias in ("events","peer") for column in ("day","classic")),"discovered Date32 schema missing")
    found = [value for entry in report["plans"] for value in filters(entry["plan"])]
    if mode == "disabled":
        require(not found, "disabled factory unexpectedly pushed predicates")
        require(all(" WHERE " not in event["sql"] for event in events), "disabled source scan contains predicates")
    elif case["date_push"]:
        require(any(item.get("kind") == "comparison" and item.get("type") == "date32" for item in found),
                "required actual Date32 bound predicate missing")
        require(any("toInt32(" in event["sql"] for event in events), "Date32 predicate did not reach ClickHouse")
    require(events and all(event.get("status") == 200 and event.get("complete") and event.get("finished") and
                          event.get("arrow_rows") is not None and not event.get("arrow_decode_failed") for event in events),
            "source Arrow stream observation incomplete")
    stats = list(report["stats"].values())
    require(sum(s["rows"] for s in stats) == sum(e["arrow_rows"] for e in events), "source row counters differ")
    require(sum(s.get("source_wire_bytes", 0) for s in stats) == sum(e["wire_bytes"] for e in events), "source wire counters differ")
    require(sum(s["scans"] for s in stats) == len(events), "source scan counters differ")
    return {"mode": mode, "rows": sum(s["rows"] for s in stats), "arrow_buffer_bytes": sum(s["bytes"] for s in stats),
            "observed_arrow_buffer_bytes": sum(e["arrow_buffer_bytes"] for e in events),
            "source_http_body_bytes": sum(e["wire_bytes"] for e in events), "scans": len(events),
            "setup_query_close_seconds": report["seconds"], "output_rows": len(report["rows"]),
            "result_sha256": hashlib.sha256(json.dumps({key: report[key] for key in ("names", "types", "rows")}, sort_keys=True).encode()).hexdigest(),
            "plans": report["plans"]}


class Fixture:
    def __init__(self, args, *, cleanup=False):
        self.args, self.directory = args, Path(args.directory).absolute()
        self.observer, self.child = None, None
        if cleanup:
            require(self.directory.is_dir() and not self.directory.is_symlink(), "private fixture directory missing")
            require(stat.S_IMODE(self.directory.stat().st_mode)==0o700 and self.directory.stat().st_uid==os.getuid(),"owned private fixture directory required")
            self.manifest = read_private(self.directory / "fixture.json")
        else:
            require(not self.directory.exists(), "fresh fixture directory required; closed fixtures cannot restart")
            self.directory.mkdir(mode=0o700)
            self.manifest = {"id": secrets.token_hex(12), "container": args.container, "creation_intent": False}
        self.database = "kelvo_date32_" + self.manifest["id"]
        self.username = self.database + "_reader"
        require(re.fullmatch(r"[a-f0-9]{24}", self.manifest["id"]) and self.manifest["container"] == args.container,
                "owned fixture identity required")
        require(type(self.manifest.get("creation_intent")) is bool,"boolean fixture intent required")
        if self.manifest["creation_intent"]:
            require(self.manifest.get("container_identity")==args.expected_container_identity and
                    self.manifest.get("container_id")==args.expected_container_identity.split()[0] and
                    self.manifest.get("source_version")==args.expected_source_version,"frozen fixture baseline differs")
        self.command = 0
        self.attempt = secrets.token_hex(6)
        self.env = {key: value for key, value in os.environ.items() if key in ("PATH", "HOME", "LANG", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR")}
        self.env.update(GOMAXPROCS="2", GOTOOLCHAIN="local", GOENV="off", GOWORK="off")
        if not cleanup:self.save()

    def save(self):
        private_write(self.directory / "fixture.tmp", json.dumps(self.manifest, indent=2)+"\n")
        with (self.directory / "fixture.tmp").open("rb") as handle: os.fsync(handle.fileno())
        (self.directory / "fixture.tmp").replace(self.directory / "fixture.json")
        descriptor = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(descriptor)
        finally: os.close(descriptor)

    def redact(self, data):
        if getattr(self,"password",None): return data.replace(self.password.encode(),b"[REDACTED]")
        return data

    def command_result(self, argv, *, data=None, timeout=15):
        self.command += 1
        prefix = self.directory / ("command-"+self.attempt+"-"+str(self.command))
        try:
            result = subprocess.run(argv, input=data, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            private_write(prefix.with_suffix(".stdout"), self.redact(error.stdout or b""))
            private_write(prefix.with_suffix(".stderr"), self.redact(error.stderr or b""))
            private_write(prefix.with_suffix(".timeout"), "deadline exceeded\n")
            raise AssertionError("owned command exceeded deadline") from None
        private_write(prefix.with_suffix(".stdout"), self.redact(result.stdout))
        private_write(prefix.with_suffix(".stderr"), self.redact(result.stderr))
        require(result.returncode == 0, "owned ClickHouse command failed")
        return result.stdout.decode().strip()

    def admin(self, sql, *, cleanup=False):
        target = self.manifest.get("container_id", self.args.container)
        marker = "cleanup" if cleanup else "admin"
        identifier = self.database+"_"+marker+"_"+self.attempt+"_"+str(self.command+1)
        return self.command_result(["sudo", "-n", "docker", "exec", "-i", target, "clickhouse-client",
            "--query_id", identifier, "--max_threads", "1", "--max_memory_usage", "134217728",
            "--max_execution_time", "10", "--multiquery"], data=sql.encode(), timeout=15)

    def identity(self):
        target = self.manifest.get("container_id", self.args.container)
        return self.command_result(["sudo", "-n", "docker", "inspect", "--format", "{{.Id}} {{.Image}} {{.State.Running}} {{.State.StartedAt}} {{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}", target], timeout=10)

    def source(self, sql):
        endpoint = urlsplit(self.args.endpoint)
        require(endpoint.scheme == "http" and endpoint.hostname and not endpoint.username and not endpoint.query and not endpoint.fragment,
                "fixed dedicated HTTP endpoint required")
        connection = http.client.HTTPConnection(endpoint.hostname, endpoint.port or 8123, timeout=15)
        settings = {"readonly": 1, "max_execution_time": 10, "max_threads": 1, "max_memory_usage": 134217728,
                    "max_result_rows": 10000, "max_result_bytes": MAX_BODY, "result_overflow_mode": "throw",
                    "join_use_nulls": 1, "cancel_http_readonly_queries_on_client_close": 1}
        try:
            credentials = base64.b64encode((self.username+":"+self.password).encode()).decode()
            connection.request("POST", (endpoint.path or "/")+"?"+urlencode(settings), body=sql.encode(), headers={"Authorization":"Basic "+credentials})
            response = connection.getresponse(); data = response.read(MAX_BODY+1)
            if response.status != 200 or len(data)>MAX_BODY:
                private_write(self.directory/("source-failure-"+secrets.token_hex(6)+".bin"), data)
                private_write(self.directory/("source-failure-status-"+secrets.token_hex(6)+".json"), json.dumps({"status":response.status,"bytes":len(data)}))
            require(response.status == 200 and len(data)<=MAX_BODY, "source reference query failed or exceeded bound")
            return data
        finally: connection.close()

    def setup(self):
        identity = self.identity()
        require(identity==self.args.expected_container_identity,"frozen ClickHouse baseline differs")
        self.manifest.update(container_identity=identity, container_id=identity.split()[0])
        require(re.fullmatch(r"[a-f0-9]{64}", self.manifest["container_id"]), "immutable container identity required")
        self.manifest["source_version"]=self.admin("SELECT version()")
        require(self.manifest["source_version"]==self.args.expected_source_version,"frozen source version differs")
        require(self.admin("SELECT timezone()") == "UTC", "fixture expiry requires verified UTC server timezone")
        self.manifest.update(preserved_rows=self.admin("SELECT coalesce(sum(rows),0) FROM system.parts WHERE active AND database='kelvo_bench' AND table='fact_events'"))
        require(self.admin(f"SELECT count() FROM system.databases WHERE name='{self.database}'") == "0" and
                self.admin(f"SELECT count() FROM system.users WHERE name='{self.username}'") == "0", "fresh namespace collision")
        self.manifest.update(creation_intent=True, created_at=datetime.now(timezone.utc).isoformat(),
                             expires_at=(datetime.now(timezone.utc)+timedelta(minutes=65)).strftime("%Y-%m-%d %H:%M:%S"))
        self.save()  # Durable exact namespace ownership precedes any CREATE.
        self.password = secrets.token_hex(32)
        self.admin(f"CREATE DATABASE `{self.database}`")
        self.admin(f"CREATE TABLE `{self.database}`.events (id Int32,day Nullable(Date32),classic Nullable(Date),label String,amount Int32) ENGINE=MergeTree ORDER BY id")
        values=[]
        for i, day in enumerate(DAYS):
            day_sql = "NULL" if day is None else "'"+day+"'"
            classic = "NULL" if i in (2, 11) else ("'1970-01-01'" if i%2 else "'2000-02-29'")
            values.append(f"({i},{day_sql},{classic},'{('x' if i%3==0 else 'keep')}',{i*10})")
        self.admin(f"INSERT INTO `{self.database}`.events VALUES "+",".join(values))
        self.admin(f"CREATE USER `{self.username}` IDENTIFIED WITH sha256_password BY '{self.password}' VALID UNTIL '{self.manifest['expires_at']}' SETTINGS max_execution_time=10")
        self.admin(f"GRANT SELECT ON `{self.database}`.* TO `{self.username}`")
        require(self.admin("SELECT version()")==self.manifest["source_version"],"source version changed during fixture setup")
        self.save()
        self.observer = Observer(self, self.args.endpoint)
        self.env.update(KELVO_SOURCE_DATE32_URL=f"http://127.0.0.1:{self.observer.server_port}/",
                        KELVO_SOURCE_DATE32_USER=self.username, KELVO_SOURCE_DATE32_PASSWORD=self.password)
        config = "sources:\n  - id: warehouse\n    type: clickhouse\n    url_env: KELVO_SOURCE_DATE32_URL\n    username_env: KELVO_SOURCE_DATE32_USER\n    password_env: KELVO_SOURCE_DATE32_PASSWORD\n    federation:\n      max_scan_rows: 10000\n      max_scan_bytes: 8388608\n      tables:\n"
        for name in ("events", "peer"):
            config += f"        - name: {name}\n          database: {self.database}\n          table: events\n"
        self.config = self.directory / "kelvo.yml"; private_write(self.config, config)
        probe = self.source("SELECT toInt32(toDate('1970-01-01')) AS date_epoch,toInt32(toDate32('1969-12-31')) AS before_epoch,toTypeName(toInt32(CAST(NULL AS Nullable(Date32)))) AS nullable_type,isNull(toInt32(CAST(NULL AS Nullable(Date32)))) AS preserved_null FORMAT JSONEachRow")
        actual = json.loads(probe)
        require(actual == {"date_epoch":0,"before_epoch":-1,"nullable_type":"Nullable(Int32)","preserved_null":1}, "installed date conversion contract differs")
        self.manifest["conversion_probe"] = actual; self.save()
        count = json.loads(self.source(f"SELECT count() AS n FROM `{self.database}`.events FORMAT JSONEachRow"))["n"]
        require(int(count) == len(DAYS), "complete fixture row count differs")

    def phase(self, case, mode):
        name = case["name"]+"-"+mode
        case_path, report_path = self.directory/(name+"-case.json"), self.directory/(name+"-result.json")
        private_write(case_path, json.dumps({"name":case["name"],"sql":case["sql"]}))
        env = dict(self.env, KELVO_TEST_DATE32_CONFIG=str(self.config), KELVO_TEST_DATE32_CASE=str(case_path),
                   KELVO_TEST_DATE32_REPORT=str(report_path), KELVO_TEST_DATE32_MODE=mode)
        mark = self.observer.mark()
        self.child = subprocess.Popen([str(self.args.go), "tool", "test2json", "-p", "github.com/SYNEHQ/kelvo-go/internal/federation", "-t", str(self.args.test_binary), "-test.v=test2json", "-test.run", "^"+LIVE_ROOT+"$", "-test.count=1", "-test.timeout=30s"], env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        try:
            return self.finish_phase(case, mode, name, report_path, mark)
        finally:
            idle = wait_until(self.observer.idle, 10)
            observed = self.observer.since(mark)
            private_write(self.directory/(name+"-observed.json"),json.dumps(observed,indent=2)+"\n")
            private_write(self.directory/(name+"-observer-state.json"),json.dumps({"idle":idle})+"\n")

    def finish_phase(self, case, mode, name, report_path, mark):
        try: out, err = self.child.communicate(timeout=40)
        except subprocess.TimeoutExpired:
            os.killpg(self.child.pid, signal.SIGKILL);out,err=self.child.communicate(timeout=10)
            private_write(self.directory/(name+".log"),out+b"\n"+err)
            raise AssertionError("live phase exceeded deadline") from None
        private_write(self.directory/(name+".log"),out+b"\n"+err)
        require(self.child.returncode == 0, "live native test failed")
        events = [json.loads(line) for line in out.splitlines() if line.startswith(b"{")]
        require(any(e.get("Action")=="pass" and e.get("Test")==LIVE_ROOT for e in events) and
                not any(e.get("Action") in ("fail","skip") for e in events), "live native required root absent, failed or skipped")
        require(wait_until(self.observer.idle, 10), "live source stream remained active")
        report=json.loads(report_path.read_text());scans=self.observer.since(mark)
        return report, scans

    def cleanup(self):
        self.attempt=secrets.token_hex(6)
        local_errors=[]
        try:
            if self.child is not None and self.child.poll() is None:
                os.killpg(self.child.pid,signal.SIGKILL);self.child.wait(timeout=10)
        except Exception as error:local_errors.append(type(error).__name__)
        try:
            if self.observer is not None:
                self.observer.stop()
                private_write(self.directory/("all-observed-"+self.attempt+".json"),json.dumps(self.observer.since(0,False),indent=2)+"\n")
        except Exception as error:local_errors.append(type(error).__name__)
        if not self.manifest.get("creation_intent"):
            require(not local_errors,"local cleanup failed before source creation")
            return {"passed":True,"created":False}
        require(self.manifest["container_identity"]==self.args.expected_container_identity and
                self.manifest["source_version"]==self.args.expected_source_version and
                self.identity()==self.manifest["container_identity"],"existing ClickHouse identity changed; cleanup refused")
        identity={"id":self.manifest["id"],"manifest_sha256":sha(self.directory/"fixture.json")}
        immutable_receipt(self.directory/"launch-closed.json",{**identity,"closed_at":datetime.now(timezone.utc).isoformat()},identity)
        # Even if local process/proxy teardown failed, release the owned source
        # resources before reporting that failure. Query IDs never repeat.
        self.admin(f"KILL QUERY WHERE startsWith(query_id, '{self.database}_admin_') SYNC",cleanup=True)
        self.admin(f"KILL QUERY WHERE user='{self.username}' SYNC",cleanup=True)
        self.admin(f"DROP USER IF EXISTS `{self.username}`",cleanup=True)
        self.admin(f"DROP DATABASE IF EXISTS `{self.database}` SYNC",cleanup=True)
        require(self.admin(f"SELECT count() FROM system.users WHERE name='{self.username}'",cleanup=True)=="0" and
                self.admin(f"SELECT count() FROM system.databases WHERE name='{self.database}'",cleanup=True)=="0","owned Date32 fixture remains")
        rows=self.admin("SELECT coalesce(sum(rows),0) FROM system.parts WHERE active AND database='kelvo_bench' AND table='fact_events'",cleanup=True)
        require(rows==self.manifest["preserved_rows"] and self.identity()==self.manifest["container_identity"],"existing ClickHouse data or lifecycle changed")
        receipt={**identity,"passed":not local_errors,"owned_database_removed":True,"owned_reader_removed":True,
                 "existing_clickhouse_unchanged":True,"launch_closed":True,"local_cleanup_errors":local_errors,
                 "observed_at":datetime.now(timezone.utc).isoformat()}
        private_write(self.directory/("cleanup-attempt-"+self.attempt+".json"),json.dumps(receipt,indent=2)+"\n")
        canonical=immutable_receipt(self.directory/"cleanup.json",receipt,identity)
        require(not local_errors,"local cleanup failed after source resources were removed")
        return {**receipt,"canonical_cleanup_sha256":sha(self.directory/"cleanup.json"),"canonical_passed":canonical["passed"]}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action",choices=("run","cleanup"),default="run")
    parser.add_argument("--directory",type=Path,required=True)
    parser.add_argument("--container",default="kelvo-clickhouse")
    parser.add_argument("--endpoint",required=True)
    parser.add_argument("--expected-container-identity",required=True)
    parser.add_argument("--expected-source-version",required=True)
    parser.add_argument("--go",type=Path)
    parser.add_argument("--test-binary",type=Path)
    args=parser.parse_args();os.umask(0o077)
    require(sys.platform.startswith("linux") and re.fullmatch(r"[A-Za-z0-9_.-]+",args.container),"Linux fixture and plain container identifier required")
    if args.action == "cleanup" and not args.directory.exists():
        print(json.dumps({"passed":True,"created":False,"directory_absent":True}));return 0
    if args.action == "cleanup" and args.directory.is_dir() and not args.directory.is_symlink() and not list(args.directory.iterdir()):
        info=args.directory.stat()
        require(info.st_uid==os.getuid() and stat.S_IMODE(info.st_mode)==0o700,"owned empty private directory required")
        print(json.dumps({"passed":True,"created":False,"empty_directory":True}));return 0
    fixture=Fixture(args,cleanup=args.action=="cleanup")
    if args.action=="cleanup": print(json.dumps(fixture.cleanup()));return 0
    require(args.go and args.go.is_file() and args.test_binary and args.test_binary.is_file(),"pinned Go and native test binary required")
    binary_hash=sha(args.test_binary)
    report={"passed":False,"cases":[],"failure":None,"binary_sha256":binary_hash,"fixture_rows":len(DAYS),"expected_cases":len(cases()),
            "measurement_scope":"Fresh synthetic source; phase timing includes source discovery, DuckDB setup, query and all closure. Warm uncontrolled caches; no throughput or pruning claim. Source wire bytes are dechunked HTTP bodies, not TCP bytes."}
    try:
        fixture.setup();report["source_version"]=fixture.manifest["source_version"];report["conversion_probe"]=fixture.manifest["conversion_probe"]
        for case in cases():
            entry={"name":case["name"],"sql":case["sql"],"phases":[],"passed":False};report["cases"].append(entry)
            raw=fixture.source(case["reference"].replace("{events}",f"`{fixture.database}`.events")+" FORMAT ArrowStream")
            private_write(fixture.directory/(case["name"]+"-reference.arrow"),raw)
            reference=normalized(pa.ipc.open_stream(raw).read_all())
            private_write(fixture.directory/(case["name"]+"-reference.json"),json.dumps(reference,indent=2)+"\n")
            entry["reference_arrow_bytes"]=len(raw)
            phases={}
            for mode in ("pushed","disabled"):
                result,scans=fixture.phase(case,mode);phases[mode]=result
                private_write(fixture.directory/(case["name"]+"-"+mode+"-observed.json"),json.dumps(scans,indent=2)+"\n")
                entry["phases"].append(validate_phase(case,mode,result,reference,scans))
            require(same_typed_rows(phases["pushed"],phases["disabled"]),"pushed and disabled typed result mismatch")
            entry["passed"]=True
            private_write(fixture.directory/"acceptance-progress.json",json.dumps(report,indent=2)+"\n")
        selective=next(c for c in report["cases"] if c["name"]=="epoch_eq")["phases"]
        require(selective[0]["rows"]<selective[1]["rows"] and selective[0]["source_http_body_bytes"]<selective[1]["source_http_body_bytes"],"selective Date32 predicate did not reduce fetched rows and body bytes")
        require(sha(args.test_binary)==binary_hash,"native test binary changed")
        report["passed"]=True
    except Exception as error:
        report["failure"]={"exception_type":type(error).__name__,"case":report["cases"][-1]["name"] if report["cases"] else "setup"}
        private_write(fixture.directory/"failure.txt",str(error))
    finally:
        try: report["cleanup"]=fixture.cleanup()
        except Exception as error:
            report["passed"]=False;report["cleanup"]={"passed":False,"exception_type":type(error).__name__}
            private_write(fixture.directory/"cleanup-failure.txt",str(error))
        private_write(fixture.directory/"acceptance.json",json.dumps(report,indent=2)+"\n")
    print(json.dumps({"passed":report["passed"],"cases":len(report["cases"]),"failure":report["failure"]}))
    return 0 if report["passed"] else 1

if __name__=="__main__": raise SystemExit(main())

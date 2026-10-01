#!/usr/bin/env python3
"""Real binary acceptance. Requires pyarrow; runs only against the local fixture."""
import argparse
import concurrent.futures
import decimal
import io
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import pyarrow.ipc as ipc

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", default="bin/kelvo")
    parser.add_argument("--output")
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    root = Path(__file__).resolve().parents[1]
    results = {}
    with tempfile.TemporaryDirectory(prefix="kelvo-acceptance-") as work:
        work = Path(work)
        output = work / "result.arrow"
        query = "SELECT region, SUM(amount::DECIMAL(18,2)) AS revenue FROM sales GROUP BY region ORDER BY region"
        command = [binary, "query", "--config", "examples/kelvo.json", "--sources", "sales", "--sql", query, "--out", str(output)]
        result = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr
        expected = [{"region":"east","revenue":decimal.Decimal("25.00")},{"region":"west","revenue":decimal.Decimal("19.75")}]
        with ipc.open_stream(output) as reader:
            assert reader.read_all().to_pylist() == expected
        results["cli_decimal_values"] = "passed"
        results["relative_quickstart_configuration"] = "passed"
        before = output.read_bytes()
        failed = subprocess.run(command + ["--max-rows","1"], cwd=root, capture_output=True, text=True, timeout=30)
        assert failed.returncode != 0 and output.read_bytes() == before
        results["failed_export_preserves_existing_file"] = "passed"

        token = secrets.token_hex(32)
        env = dict(os.environ, KELVO_ACCEPTANCE_TOKEN=token)
        with socket.socket() as address:
            address.bind(("127.0.0.1",0))
            port = address.getsockname()[1]
        log = (work/"server.log").open("w+")
        server = subprocess.Popen([binary,"serve","--config","examples/kelvo.json","--listen",f"127.0.0.1:{port}","--token-env","KELVO_ACCEPTANCE_TOKEN","--max-rows","3","--timeout","10s"],cwd=root,env=env,stdout=log,stderr=log)
        url = f"http://127.0.0.1:{port}"
        def request(path, value=None, auth=True):
            data = json.dumps(value).encode() if value is not None else None
            headers = {"Content-Type":"application/json"}
            if auth: headers["Authorization"] = "Bearer "+token
            return urllib.request.urlopen(urllib.request.Request(url+path,data=data,headers=headers),timeout=15)
        def state(identifier):
            with request("/v1/queries/"+identifier) as response:
                return json.load(response)
        def submit(sql, sources=None):
            with request("/v1/queries",{"mode":"federated","sources":sources or [],"sql":sql}) as response:
                return json.load(response)["id"]
        def wait(predicate, seconds=5):
            deadline = time.monotonic()+seconds
            while time.monotonic()<deadline:
                value = predicate()
                if value: return value
                time.sleep(.02)
            raise AssertionError("condition did not become ready")
        def children():
            if not Path("/proc").exists(): return set()
            found = set()
            for p in Path(f"/proc/{server.pid}/task").glob("*/children"):
                try: found.update(int(x) for x in p.read_text().split())
                except FileNotFoundError: pass
            return found
        def alive(pid):
            try: return Path(f"/proc/{pid}/stat").read_text().split(") ",1)[1].split()[0] not in ("Z","X")
            except FileNotFoundError: return False
        try:
            def ready():
                if server.poll() is not None:
                    log.seek(0)
                    raise AssertionError(log.read())
                try:
                    with request("/health",auth=False) as r: return r.status == 200
                except OSError: return False
            wait(ready)
            try: request("/v1/queries",{"mode":"federated","sql":"SELECT 1"},auth=False)
            except urllib.error.HTTPError as e: assert e.code == 401
            else: raise AssertionError("unauthenticated request accepted")
            results["authentication"] = "passed"
            identifier = submit(query,["sales"])
            with request("/v1/queries/"+identifier+"/results") as response:
                with ipc.open_stream(response) as reader: assert reader.read_all().to_pylist() == expected
            wait(lambda: state(identifier)["state"] == "succeeded")
            try: request("/v1/queries/"+identifier+"/results")
            except urllib.error.HTTPError as e: assert e.code == 409
            else: raise AssertionError("result handle reused")
            results["http_arrow_and_single_consumer"] = "passed"
            identifier = submit("SELECT * FROM sales",["sales"])
            try:
                with request("/v1/queries/"+identifier+"/results") as response: response.read()
            except urllib.error.HTTPError: pass
            wait(lambda: state(identifier)["state"] == "failed")
            assert state(identifier)["error"]["code"] == "RESOURCE_EXHAUSTED"
            results["output_limit_is_failure"] = "passed"
            identifier = submit("SELECT 1")
            with request("/v1/queries/"+identifier+"/cancel",{}) as response:
                assert json.load(response)["state"] == "cancelled"
            results["queued_cancellation"] = "passed"

            long_sql = "SELECT sum(i::DOUBLE*j::DOUBLE) FROM range(1000000000) a(i), range(1000000000) b(j)"
            def consume(identifier):
                try:
                    with request("/v1/queries/"+identifier+"/results") as response: response.read()
                except (OSError, ValueError): pass
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                identifier = submit(long_sql)
                future = pool.submit(consume, identifier)
                wait(lambda: state(identifier)["state"] == "running")
                live = wait(children) if Path("/proc").exists() else set()
                started = time.monotonic()
                with request("/v1/queries/"+identifier+"/cancel",{}) as response:
                    assert json.load(response)["state"] == "cancelled"
                future.result(timeout=5)
                wait(lambda: not any(alive(pid) for pid in live))
                results["active_cancel_seconds"] = time.monotonic()-started
                if Path("/proc").exists():
                    identifier = submit(long_sql)
                    future = pool.submit(consume, identifier)
                    live = wait(children)
                    server.kill()
                    server.wait(timeout=5)
                    future.result(timeout=5)
                    wait(lambda: not any(alive(pid) for pid in live))
                    results["linux_parent_death_cleanup"] = "passed"
        finally:
            if server.poll() is None:
                server.terminate()
                server.wait(timeout=8)
            log.close()
    text = json.dumps(results,indent=2)
    print(text)
    if args.output: Path(args.output).write_text(text+"\n")

if __name__ == "__main__":
    main()

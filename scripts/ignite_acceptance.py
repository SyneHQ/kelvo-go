#!/usr/bin/env python3
"""Run bounded Ignite 2 acceptance on a disposable test host.

Requires Docker, OpenSSL and Go. The authenticated database is bound to loopback;
a disposable local TLS proxy verifies the production connector's HTTPS path.
Only this fixture container is removed. Credentials never enter evidence.
"""
import argparse
import hashlib
import http.server
import json
import os
from pathlib import Path
import secrets
import shutil
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

IMAGE = "apacheignite/ignite:2.17.0"
CONTAINER = "kelvo-ignite-native-test"
BACKEND = "http://127.0.0.1:18080/ignite"
ORIGIN = "https://localhost:18281"
CACHE = "kelvo_fixture"
ROWS = 1205

CONFIG = '''<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd">
 <bean class="org.apache.ignite.configuration.IgniteConfiguration">
  <property name="authenticationEnabled" value="true"/>
  <property name="peerClassLoadingEnabled" value="false"/>
  <property name="dataStorageConfiguration">
   <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
    <property name="walSegments" value="2"/>
    <property name="walSegmentSize" value="8388608"/>
    <property name="defaultDataRegionConfiguration">
     <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
      <property name="initialSize" value="67108864"/>
      <property name="maxSize" value="134217728"/>
      <property name="persistenceEnabled" value="true"/>
     </bean>
    </property>
   </bean>
  </property>
  <property name="connectorConfiguration">
   <bean class="org.apache.ignite.configuration.ConnectorConfiguration">
    <property name="idleQueryCursorTimeout" value="300000"/>
    <property name="idleQueryCursorCheckFrequency" value="10000"/>
   </bean>
  </property>
  <property name="cacheConfiguration">
   <list><bean class="org.apache.ignite.configuration.CacheConfiguration">
    <property name="name" value="kelvo_fixture"/>
    <property name="sqlSchema" value="PUBLIC"/>
   </bean></list>
  </property>
  <property name="discoverySpi">
   <bean class="org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi">
    <property name="ipFinder"><bean class="org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder">
     <property name="addresses"><list><value>127.0.0.1:47500</value></list></property>
    </bean></property>
   </bean>
  </property>
 </bean>
</beans>
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default="go")
    parser.add_argument("--output", default="docs/evidence/ignite-native.json")
    parser.add_argument("--sudo", action="store_true")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    docker = (["sudo", "-n"] if args.sudo else []) + ["docker"]
    if subprocess.run(docker + ["inspect", CONTAINER], capture_output=True).returncode == 0:
        raise SystemExit("Refusing to replace an existing fixture container")
    scratch = Path(tempfile.mkdtemp(prefix="kelvo-ignite-"))
    scratch.chmod(0o755)
    started = False
    proxy = None
    password = secrets.token_hex(24)
    try:
        (scratch / "ignite.xml").write_text(CONFIG)
        subprocess.run(docker + ["run", "-d", "--name", CONTAINER, "--memory=2g", "--cpus=1",
            "-p", "127.0.0.1:18080:8080", "-e", "CONFIG_URI=/fixture/ignite.xml",
            "-e", "OPTION_LIBS=ignite-rest-http,ignite-json,ignite-spring,ignite-indexing",
            "-e", "JVM_OPTS=-Xms256m -Xmx512m -XX:ActiveProcessorCount=2 -Djava.net.preferIPv4Stack=true",
            "-v", str(scratch / "ignite.xml") + ":/fixture/ignite.xml:ro", IMAGE],
            check=True, stdout=subprocess.DEVNULL)
        started = True
        login = {"ignite.login": "ignite", "ignite.password": "ignite"}

        def api(form, check=True):
            data = urllib.parse.urlencode(dict(login, **form)).encode()
            req = urllib.request.Request(BACKEND, data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"})
            with urllib.request.urlopen(req, timeout=15) as response:
                result = json.load(response)
            if check and result.get("successStatus") != 0:
                message = str(result.get("error", "unknown error")).replace(password, "<redacted>")
                raise RuntimeError("Ignite fixture command failed: " + message)
            return result

        deadline = time.monotonic() + 150
        while True:
            try:
                info = api({"cmd": "version"})
                break
            except (urllib.error.URLError, OSError, RuntimeError):
                state = subprocess.check_output(docker + ["inspect", "--format", "{{.State.Running}}", CONTAINER], text=True).strip()
                if state != "true":
                    raise RuntimeError("Ignite fixture exited before becoming ready")
                if time.monotonic() > deadline:
                    raise RuntimeError("Ignite fixture did not become ready")
                time.sleep(1)
        api({"cmd": "activate"})

        def sql(text):
            result = api({"cmd": "qryfldexe", "cacheName": CACHE, "pageSize": "1000", "qry": text})
            page = result.get("response", {})
            if isinstance(page, dict) and not page.get("last", True):
                api({"cmd": "qrycls", "qryId": str(page["queryId"])})
            return result

        api({"cmd": "updateuser", "user": "ignite", "password": password})
        login["ignite.password"] = password
        sql("CREATE TABLE KELVO_NATIVE_ACCEPTANCE (id BIGINT PRIMARY KEY, label VARCHAR)")
        values = []
        for i in range(ROWS):
            label = "NULL" if i % 10 == 0 else "'item-" + str(i) + "'"
            values.append("(" + str(9007199254740993 + i) + "," + label + ")")
        sql("INSERT INTO KELVO_NATIVE_ACCEPTANCE (id, label) VALUES " + ",".join(values))

        def openssl(*argv):
            subprocess.run(["openssl", *argv], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-sha256",
            "-subj", "/CN=Kelvo disposable test CA", "-keyout", str(scratch / "ca.key"), "-out", str(scratch / "ca.crt"))
        openssl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
            "-keyout", str(scratch / "server.key"), "-out", str(scratch / "server.csr"))
        (scratch / "extensions").write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
        openssl("x509", "-req", "-in", str(scratch / "server.csr"), "-CA", str(scratch / "ca.crt"),
            "-CAkey", str(scratch / "ca.key"), "-CAcreateserial", "-days", "2", "-sha256",
            "-extfile", str(scratch / "extensions"), "-out", str(scratch / "server.crt"))
        (scratch / "ca.key").chmod(0o600)
        (scratch / "server.key").chmod(0o600)
        observed = []
        observation_lock = threading.Lock()

        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                if self.path != "/ignite":
                    self.send_error(404)
                    return
                size = int(self.headers.get("Content-Length", "0"))
                if size < 1 or size > 1 << 20:
                    self.send_error(413)
                    return
                body = self.rfile.read(size)
                form = urllib.parse.parse_qs(body.decode())
                req = urllib.request.Request(BACKEND, data=body,
                    headers={"Content-Type": "application/x-www-form-urlencoded"})
                try:
                    with urllib.request.urlopen(req, timeout=15) as response:
                        payload = response.read(32 << 20)
                    decoded = json.loads(payload)
                    page = decoded.get("response")
                    event = {"command": form.get("cmd", [""])[0], "status": decoded.get("successStatus")}
                    if "qryId" in form:
                        event["query_id"] = int(form["qryId"][0])
                    if isinstance(page, dict):
                        event.update({k: page[k] for k in ("queryId", "last") if k in page})
                    with observation_lock:
                        observed.append(event)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (urllib.error.URLError, OSError):
                    self.send_error(502)

        proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 18281), Proxy)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.minimum_version = ssl.TLSVersion.TLSv1_2
        tls.load_cert_chain(scratch / "server.crt", scratch / "server.key")
        proxy.socket = tls.wrap_socket(proxy.socket, server_side=True)
        threading.Thread(target=proxy.serve_forever, daemon=True).start()
        client_config = scratch / "client.json"
        client_config.write_text(json.dumps({"URL": ORIGIN, "Username": "ignite", "Password": password,
            "CA": str(scratch / "ca.crt"), "Cache": CACHE}))
        client_config.chmod(0o600)
        env = os.environ.copy()
        env.update({"KELVO_TEST_IGNITE_CONFIG": str(client_config), "GOMAXPROCS": "2"})
        result = subprocess.run([args.go, "test", "-p", "1", "-count=1", "-json", "-timeout", "90s",
            "./internal/sources/ignite", "-run", "^TestLiveIgnite"], cwd=repo, env=env,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        events = []
        for line in result.stdout.splitlines():
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        checks = [{"test": e["Test"], "status": e["Action"]} for e in events
            if "Test" in e and e.get("Action") in ("pass", "fail", "skip")]
        query_ids = sorted({e["queryId"] for e in observed if "queryId" in e})
        released = all(api({"cmd": "qryfetch", "qryId": str(qid), "pageSize": "1"}, check=False)
            .get("successStatus") != 0 for qid in query_ids)
        closes = sum(e["command"] == "qrycls" and e["status"] == 0 for e in observed)
        fetches = sum(e["command"] == "qryfetch" and e["status"] == 0 for e in observed)
        digest = subprocess.check_output(docker + ["inspect", "--format", "{{.Image}}", CONTAINER], text=True).strip()
        report = {"provider": "ignite", "server_version": info["response"], "image": IMAGE, "image_id": digest,
            "rows": ROWS, "transport": "verified TLS to disposable loopback proxy; loopback HTTP to Ignite",
            "authentication": "Ignite native authentication with generated password; not an authorization/RBAC test",
            "scope": "live Go connector metadata, types, pagination, cleanup and failure limits; not throughput or cluster validation",
            "checks": checks, "successful_fetch_requests": fetches, "successful_close_requests": closes,
            "all_observed_cursors_released": released, "observed_cursor_count": len(query_ids),
            "go_mod_sha256": hashlib.sha256((repo / "go.mod").read_bytes()).hexdigest(),
            "passed": result.returncode == 0 and len(checks) == 5 and all(c["status"] == "pass" for c in checks)
                and fetches >= 1 and closes >= 2 and bool(query_ids) and released}
        output = repo / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2), flush=True)
        if not report["passed"]:
            print(result.stdout.replace(password, "<redacted>").replace(str(scratch), "<fixture>"))
            raise SystemExit(1)
    finally:
        if proxy:
            proxy.shutdown()
            proxy.server_close()
        if started:
            subprocess.run(docker + ["rm", "-f", CONTAINER], check=False, stdout=subprocess.DEVNULL)
        shutil.rmtree(scratch)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Run DynamoDB Local acceptance on a designated test VM, never a workstation.

Requires Docker, OpenSSL, Python and Go. Creates a loopback-only, resource-bounded
container, terminates TLS with a disposable CA, and removes its own fixture.
DynamoDB Local does not validate AWS IAM or real SigV4 signatures.
"""
import argparse
import hashlib
import http.client
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
import urllib.request

IMAGE = "amazon/dynamodb-local:3.1.0"
CONTAINER = "kelvo-dynamodb-native-test"
ROWS = 1205


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default="go")
    parser.add_argument("--output", default="docs/evidence/dynamodb-native.json")
    parser.add_argument("--sudo", action="store_true")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    docker = (["sudo", "-n"] if args.sudo else []) + ["docker"]
    if subprocess.run(docker+["inspect", CONTAINER], capture_output=True).returncode == 0:
        raise SystemExit("Refusing to replace an existing fixture container")
    scratch = Path(tempfile.mkdtemp(prefix="kelvo-dynamodb-"))
    started, server = False, None
    counts = {"partiql_requests": 0, "continuation_requests": 0, "empty_pages_with_token": 0}
    lock = threading.Lock()
    try:
        def openssl(*argv):
            subprocess.run(["openssl", *argv], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        openssl("req", "-x509", "-newkey", "rsa:2048", "-sha256", "-nodes", "-days", "2",
                "-subj", "/CN=Kelvo disposable DynamoDB test CA", "-keyout", str(scratch/"ca.key"), "-out", str(scratch/"ca.crt"))
        openssl("req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=localhost",
                "-keyout", str(scratch/"server.key"), "-out", str(scratch/"server.csr"))
        (scratch/"extensions").write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
        openssl("x509", "-req", "-in", str(scratch/"server.csr"), "-CA", str(scratch/"ca.crt"),
                "-CAkey", str(scratch/"ca.key"), "-CAcreateserial", "-days", "2", "-sha256",
                "-extfile", str(scratch/"extensions"), "-out", str(scratch/"server.crt"))
        for file in scratch.iterdir(): file.chmod(0o600)
        subprocess.run(docker+["run", "-d", "--name", CONTAINER, "--memory=512m", "--cpus=1", "--pids-limit=128",
            "-p", "127.0.0.1::8000", IMAGE, "-jar", "DynamoDBLocal.jar", "-inMemory", "-sharedDb", "-disableTelemetry"],
            check=True, stdout=subprocess.DEVNULL)
        started = True
        port = int(subprocess.check_output(docker+["inspect", "--format", '{{(index (index .NetworkSettings.Ports "8000/tcp") 0).HostPort}}', CONTAINER], text=True).strip())

        class Proxy(http.server.BaseHTTPRequestHandler):
            def log_message(self, *unused): pass
            def do_POST(self):
                if self.path != "/": self.send_error(404); return
                size = int(self.headers.get("Content-Length", "0"))
                if size < 0 or size > 256*1024: self.send_error(413); return
                raw = self.rfile.read(size)
                headers = {k: self.headers[k] for k in ("Content-Type", "Authorization", "X-Amz-Date", "X-Amz-Target", "X-Amz-Security-Token") if k in self.headers}
                connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
                try:
                    connection.request("POST", "/", raw, headers)
                    response = connection.getresponse()
                    data = response.read(8*1024*1024+1)
                    if len(data) > 8*1024*1024: self.send_error(502); return
                    if headers.get("X-Amz-Target") == "DynamoDB_20120810.ExecuteStatement":
                        request_body, response_body = json.loads(raw), json.loads(data)
                        with lock:
                            counts["partiql_requests"] += 1
                            counts["continuation_requests"] += bool(request_body.get("NextToken"))
                            counts["empty_pages_with_token"] += bool(response_body.get("NextToken")) and not response_body.get("Items")
                    self.send_response(response.status)
                    self.send_header("Content-Type", "application/x-amz-json-1.0")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except (ConnectionError, TimeoutError, OSError):
                    self.send_error(502)
                finally:
                    connection.close()

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.minimum_version = ssl.TLSVersion.TLSv1_2
        tls.load_cert_chain(scratch/"server.crt", scratch/"server.key")
        server.socket = tls.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        origin = "https://localhost:"+str(server.server_port)
        trust = ssl.create_default_context(cafile=str(scratch/"ca.crt"))
        # Local accepts arbitrary credentials. These are dummy fixture values;
        # the actual connector still generates its requests with the SDK signer.
        local_id, local_secret = "kelvolocaltest", secrets.token_hex(24)
        authorization = "AWS4-HMAC-SHA256 Credential="+local_id+"/20261001/us-east-1/dynamodb/aws4_request, SignedHeaders=host;x-amz-date;x-amz-target, Signature="+"0"*64
        def api(operation, body):
            request = urllib.request.Request(origin+"/", data=json.dumps(body).encode(), method="POST", headers={
                "Content-Type": "application/x-amz-json-1.0", "X-Amz-Target": "DynamoDB_20120810."+operation,
                "X-Amz-Date": "20261001T000000Z", "Authorization": authorization})
            with urllib.request.urlopen(request, context=trust, timeout=12) as response: return json.load(response)
        deadline = time.monotonic()+90
        while True:
            try:
                api("ListTables", {})
                break
            except (urllib.error.URLError, TimeoutError):
                if time.monotonic() > deadline: raise RuntimeError("DynamoDB Local did not become ready")
                time.sleep(0.5)
        for table in ("kelvo_native_types", "kelvo_native_pages"):
            api("CreateTable", {"TableName": table, "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}], "BillingMode": "PAY_PER_REQUEST"})
        document = {"pk": {"S": "typed"}, "large": {"N": "9007199254740993"},
            "decimal": {"N": "12345678901234567890123456789.123456789"}, "nil": {"NULL": True}, "flag": {"BOOL": False},
            "binary": {"B": "AP8="}, "text": {"S": ""}, "strings": {"SS": ["one", "two"]},
            "numbers": {"NS": ["1", "2.0000000000000000000000000000000000001"]}, "binaries": {"BS": ["AQ==", "Ag=="]},
            "list": {"L": [{"M": {"nested": {"S": "value"}}}, {"NULL": True}]}, "map": {"M": {}}}
        api("PutItem", {"TableName": "kelvo_native_types", "Item": document})
        for offset in range(0, ROWS, 25):
            batch = [{"PutRequest": {"Item": {"pk": {"S": "row-"+str(i)}, "kind": {"S": "present"}, "large": {"N": str(9007199254740993+i)}}}} for i in range(offset, min(offset+25, ROWS))]
            result = api("BatchWriteItem", {"RequestItems": {"kelvo_native_pages": batch}})
            if result.get("UnprocessedItems"): raise RuntimeError("Fixture seeding left unprocessed items")
        config = scratch/"client.json"
        config.write_text(json.dumps({"URL": origin, "ID": local_id, "Secret": local_secret, "CA": str(scratch/"ca.crt")}))
        config.chmod(0o600)
        env = os.environ.copy()
        env.update({"KELVO_TEST_DYNAMODB_CONFIG": str(config), "GOMAXPROCS": "2"})
        result = subprocess.run([args.go, "test", "-p", "1", "-count=1", "-timeout", "90s", "-json", "./internal/sources/dynamodb", "-run", "^TestLiveDynamoDB"],
            cwd=repo, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=150)
        events=[]
        for line in result.stdout.splitlines():
            try: events.append(json.loads(line))
            except json.JSONDecodeError: pass
        checks=[{"test": e["Test"], "status": e["Action"]} for e in events if "Test" in e and e.get("Action") in ("pass", "fail", "skip")]
        digest=subprocess.check_output(docker+["inspect", "--format", "{{.Image}}", CONTAINER], text=True).strip()
        report={"provider": "dynamodb-local", "server_version": "3.1.0", "image": IMAGE, "image_id": digest,
            "rows": ROWS, "typed_documents": 1, "transport": "verified TLS with disposable CA; loopback-only HTTP backend",
            "resource_limits": {"memory_mb": 512, "cpus": 1, "pids": 128},
            "authentication": "dummy local credentials; AWS IAM enforcement and cloud SigV4 interoperability are not validated",
            "scope": "real DynamoDB Local PartiQL through the native Go connector; not AWS cloud, cluster, snapshot, or throughput validation",
            "checks": checks, "observed_protocol": counts,
            "go_mod_sha256": hashlib.sha256((repo/"go.mod").read_bytes()).hexdigest(),
            "passed": result.returncode == 0 and len(checks) == 5 and all(c["status"] == "pass" for c in checks) and counts["continuation_requests"] >= 2 and counts["empty_pages_with_token"] >= 1}
        output=repo/args.output; output.parent.mkdir(parents=True, exist_ok=True); output.write_text(json.dumps(report, indent=2)+"\n")
        print(json.dumps(report, indent=2))
        if not report["passed"]:
            print(result.stdout.replace(str(scratch), "<fixture>").replace(local_secret, "<redacted>"))
            raise SystemExit(1)
    finally:
        if server: server.shutdown(); server.server_close()
        if started: subprocess.run(docker+["rm", "-f", CONTAINER], check=False, stdout=subprocess.DEVNULL)
        shutil.rmtree(scratch)

if __name__ == "__main__": main()

#!/usr/bin/env python3
"""Disposable secured Elasticsearch acceptance fixture. Run only on a test host.

Requires Docker, OpenSSL and Go. Binds loopback, generates temporary TLS keys and
a restricted API key, runs the live Go connector tests, and removes its container.
No credentials are printed or included in the public evidence JSON.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.19.0"
CONTAINER = "kelvo-elasticsearch-native-test"
ORIGIN = "https://localhost:19220"

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--go", default="go")
    parser.add_argument("--output", default="docs/evidence/elasticsearch-native.json")
    parser.add_argument("--sudo", action="store_true")
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    docker = (["sudo", "-n"] if args.sudo else []) + ["docker"]
    existing = subprocess.run(docker+["inspect", CONTAINER], capture_output=True)
    if existing.returncode == 0:
        raise SystemExit("Refusing to replace an existing fixture container")
    scratch = Path(tempfile.mkdtemp(prefix="kelvo-elasticsearch-"))
    started = False
    try:
        certs = scratch/"certs"
        certs.mkdir(mode=0o755)
        def openssl(*argv):
            subprocess.run(["openssl",*argv],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        openssl("req","-x509","-newkey","rsa:2048","-sha256","-nodes","-days","2",
                "-subj","/CN=Kelvo disposable test CA","-keyout",str(certs/"ca.key"),"-out",str(certs/"ca.crt"))
        openssl("req","-new","-newkey","rsa:2048","-nodes","-subj","/CN=localhost",
                "-keyout",str(certs/"server.key"),"-out",str(certs/"server.csr"))
        (certs/"extensions").write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
        openssl("x509","-req","-in",str(certs/"server.csr"),"-CA",str(certs/"ca.crt"),
                "-CAkey",str(certs/"ca.key"),"-CAcreateserial","-days","2","-sha256",
                "-extfile",str(certs/"extensions"),"-out",str(certs/"server.crt"))
        for file in certs.iterdir(): file.chmod(0o644)
        # The CA private key is not mounted into the database container.
        shutil.move(certs/"ca.key",scratch/"ca.key")
        (scratch/"ca.key").chmod(0o600)
        password = secrets.token_urlsafe(32)
        envfile = scratch/"server.env"
        envfile.write_text("\n".join([
            "ELASTIC_PASSWORD="+password,
            "discovery.type=single-node",
            "ES_JAVA_OPTS=-Xms512m -Xmx512m",
            "xpack.ml.enabled=false",
            "xpack.security.enabled=true",
            "xpack.security.http.ssl.enabled=true",
            "xpack.security.http.ssl.key=certs/server.key",
            "xpack.security.http.ssl.certificate=certs/server.crt",
            "xpack.security.http.ssl.certificate_authorities=certs/ca.crt",
        ])+"\n")
        envfile.chmod(0o600)
        subprocess.run(docker+["run","-d","--name",CONTAINER,"--memory=2g","--cpus=1",
            "-p","127.0.0.1:19220:9200","--env-file",str(envfile),
            "-v",str(certs)+":/usr/share/elasticsearch/config/certs:ro",IMAGE],
            check=True,stdout=subprocess.DEVNULL)
        started = True
        context = ssl.create_default_context(cafile=str(certs/"ca.crt"))
        authorization = "Basic "+base64.b64encode(("elastic:"+password).encode()).decode()
        def api(method,path,body=None,content_type="application/json"):
            raw = body if isinstance(body,bytes) else json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(ORIGIN+path,data=raw,method=method,
                headers={"Authorization":authorization,"Content-Type":content_type})
            with urllib.request.urlopen(req,context=context,timeout=10) as response:
                return json.load(response)
        deadline=time.monotonic()+150
        while True:
            try:
                info=api("GET","/")
                break
            except (urllib.error.URLError,TimeoutError):
                if time.monotonic()>deadline: raise RuntimeError("Elasticsearch did not become ready")
                time.sleep(1)
        mapping={"mappings":{"properties":{"id":{"type":"long"},"label":{"type":"keyword"}}}}
        api("PUT","/kelvo_native_acceptance",mapping)
        api("PUT","/kelvo_native_forbidden",mapping)
        bulk=[]
        for i in range(1205):
            bulk.extend([json.dumps({"index":{"_index":"kelvo_native_acceptance"}}),
                         json.dumps({"id":9007199254740993+i,"label":None if i%10==0 else "item-"+str(i)})])
        bulk.extend([json.dumps({"index":{"_index":"kelvo_native_forbidden"}}),json.dumps({"id":1})])
        response=api("POST","/_bulk?refresh=wait_for",("\n".join(bulk)+"\n").encode(),"application/x-ndjson")
        if response.get("errors"): raise RuntimeError("Fixture indexing failed")
        key=api("POST","/_security/api_key",{"name":"kelvo-native-test","expiration":"1h",
            "role_descriptors":{"readonly":{"cluster":[],"indices":[{"names":["kelvo_native_acceptance"],"privileges":["read","view_index_metadata"]}]}}})
        private_config=scratch/"client.json"
        private_config.write_text(json.dumps({"URL":ORIGIN,"Token":key["encoded"],"CA":str(certs/"ca.crt")}))
        private_config.chmod(0o600)
        env=os.environ.copy()
        env.update({"KELVO_TEST_ELASTICSEARCH_CONFIG":str(private_config),"GOMAXPROCS":"2"})
        result=subprocess.run([args.go,"test","-p","1","-count=1","-json","./internal/sources/elasticsearch","-run","^TestLiveElasticsearch"],
            cwd=repo,env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        events=[]
        for line in result.stdout.splitlines():
            try: events.append(json.loads(line))
            except json.JSONDecodeError: pass
        checks=[{"test":e["Test"],"status":e["Action"]} for e in events if "Test" in e and e.get("Action") in ("pass","fail","skip")]
        stats=api("GET","/_nodes/stats/indices/search")
        open_contexts=sum(n["indices"]["search"]["open_contexts"] for n in stats["nodes"].values())
        digest=subprocess.check_output(docker+["inspect","--format","{{.Image}}",CONTAINER],text=True).strip()
        report={"provider":"elasticsearch","server_version":info["version"]["number"],"image":IMAGE,"image_id":digest,
                "rows":1205,"transport":"verified TLS with disposable fixture CA","authentication":"index-restricted API key",
                "scope":"live Go connector package, not cluster or throughput validation",
                "checks":checks,"open_search_contexts_after_tests":open_contexts,
                "go_mod_sha256":hashlib.sha256((repo/"go.mod").read_bytes()).hexdigest(),
                "passed":result.returncode==0 and len(checks)==3 and all(c["status"]=="pass" for c in checks) and open_contexts==0}
        output=repo/args.output
        output.parent.mkdir(parents=True,exist_ok=True)
        output.write_text(json.dumps(report,indent=2)+"\n")
        print(json.dumps(report,indent=2))
        if not report["passed"]:
            safe=result.stdout.replace(str(scratch),"<fixture>").replace(password,"<redacted>").replace(key["encoded"],"<redacted>")
            print(safe)
            raise SystemExit(1)
    finally:
        if started: subprocess.run(docker+["rm","-f",CONTAINER],check=False,stdout=subprocess.DEVNULL)
        shutil.rmtree(scratch)

if __name__=="__main__": main()

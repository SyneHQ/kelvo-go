#!/usr/bin/env python3
"""Run a synthetic HTTPS db.api contract fixture against a Kelvo image."""

"""HTTPS db.api contract acceptance against a final Kelvo container image."""
import argparse
import json
import os
import secrets
import subprocess
import tempfile
import time
from pathlib import Path
import pyarrow.ipc as ipc
FIXTURE_GO = r"""package main

import (
    "fmt"
    "io"
    "net/http"
    "os"
)

func reject(w http.ResponseWriter, clause string) {
    fmt.Println(clause)
    http.Error(w, "fixture rejected", http.StatusBadRequest)
}

func main() {
    key := os.Getenv("FIXTURE_API_KEY")
    expected := `{"id":"reference-id","query":"SELECT 9007199254740993"}`
    http.HandleFunc("/api/v1/metadata/query", func(w http.ResponseWriter, r *http.Request) {
        body, _ := io.ReadAll(r.Body)
        switch {
        case r.Method != http.MethodPost:
            reject(w, "method")
        case r.Header.Get("X-API-KEY") != key:
            reject(w, "api-key")
        case len(r.Header.Values("Authorization")) != 0:
            reject(w, "authorization-present")
        case r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Accept") != "application/json":
            reject(w, "content")
        case string(body) != expected:
            reject(w, "body")
        default:
            w.Header().Set("Content-Type", "application/json")
            _, _ = w.Write([]byte(`{"results":[{"n":9007199254740993,"nullable":null},{"n":null,"nullable":"ok"}],"rowCount":2}`))
        }
    })
    _ = http.ListenAndServeTLS(":8443", "/fixture/server.crt", "/fixture/server.key", nil)
}
"""

def dock():
    for c in (['docker'], ['sudo', '-n', 'docker']):
        if subprocess.run(c + ['info'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0:
            return c
    raise RuntimeError('Docker unavailable')

def main():
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser()
    parser.add_argument('--image', default='kelvo-go:accept')
    parser.add_argument('--output', type=Path, default=root / 'docs/evidence/adapter-acceptance.json')
    parser.add_argument('--go', default='go')
    args = parser.parse_args()
    docker = dock()
    ident = secrets.token_hex(6)
    pre = 'kelvo-dbapi-' + ident
    net = pre + '-net'
    made = []
    result = {'scope': 'HTTPS db.api contract fixture only; not cloud or live-database proof. CLI runs in a tenant container; this does not exercise node-startup Landlock.', 'image': args.image, 'tls_verified': False, 'assertions': {}}

    def run(*q, check=True, timeout=40):
        p = subprocess.run(docker + list(q), capture_output=True, timeout=timeout)
        if check and p.returncode:
            raise RuntimeError('docker operation failed: ' + q[0])
        return p

    def create(n, *q):
        run('create', '--name', n, *q)
        made.append(n)

    def flags():
        return ['--pull=never', '--network', net, '--user', '65532:65532', '--read-only', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true', '--pids-limit', '128', '--memory', '256m', '--memory-swap', '256m', '--cpus', '1', '--tmpfs', '/tmp:rw,nosuid,nodev,noexec,size=32m,uid=65532,gid=65532,mode=0700']
    try:
        result['image_id'] = json.loads(run('image', 'inspect', args.image).stdout)[0]['Id']
        with tempfile.TemporaryDirectory(prefix=pre + '-') as t:
            w = Path(t)
            os.chmod(w, 0o700)
            src = w / 'fixture.go'
            src.write_text(FIXTURE_GO)
            fixture = w / 'fixture'
            env = dict(os.environ, GOTOOLCHAIN='local', GOMAXPROCS='2')
            subprocess.run([args.go, 'build', '-trimpath', '-o', str(fixture), str(src)], check=True, capture_output=True, env=env)
            os.chmod(fixture, 0o755)
            ca_key, ca, srv_key, csr, crt = [w / n for n in ('ca.key', 'ca.crt', 'server.key', 'server.csr', 'server.crt')]
            ext = w / 'san.ext'
            ext.write_text('subjectAltName=DNS:fixture\nextendedKeyUsage=serverAuth\n')
            for q in (['openssl', 'genrsa', '-out', str(ca_key), '2048'], ['openssl', 'req', '-x509', '-new', '-key', str(ca_key), '-sha256', '-days', '1', '-subj', '/CN=kelvo-fixture-ca', '-out', str(ca)], ['openssl', 'genrsa', '-out', str(srv_key), '2048'], ['openssl', 'req', '-new', '-key', str(srv_key), '-subj', '/CN=fixture', '-out', str(csr)], ['openssl', 'x509', '-req', '-in', str(csr), '-CA', str(ca), '-CAkey', str(ca_key), '-CAcreateserial', '-days', '1', '-sha256', '-extfile', str(ext), '-out', str(crt)]):
                subprocess.run(q, check=True, capture_output=True)
            os.chmod(srv_key, 0o644)
            bundle = w / 'ca-certificates.crt'
            bundle.write_bytes(run('run', '--rm', '--pull=never', '--entrypoint', '/bin/cat', args.image, '/etc/ssl/certs/ca-certificates.crt').stdout)
            os.chmod(bundle, 0o644)
            with bundle.open('ab') as f:
                f.write(b'\n' + ca.read_bytes())
            cfg = w / 'kelvo.yml'
            cfg.write_text('sources:\n  - id: fixture\n    type: hive\n    adapter: dbapi\n    url_env: KELVO_SOURCE_ADAPTER_URL\n    token_env: KELVO_SOURCE_ADAPTER_TOKEN\n    options:\n      remote_connection_id: reference-id\n')
            os.chmod(cfg, 0o644)
            outdir = w / 'output'
            outdir.mkdir()
            os.chmod(outdir, 0o777)
            out = outdir / 'result.arrow'
            token = secrets.token_urlsafe(32)
            run('network', 'create', '--internal', net)
            fn = pre + '-fixture'
            create(fn, *flags(), '--network-alias', 'fixture', '--env', 'FIXTURE_API_KEY=' + token, '--mount', f'type=bind,src={fixture},dst=/fixture/server,readonly', '--mount', f'type=bind,src={crt},dst=/fixture/server.crt,readonly', '--mount', f'type=bind,src={srv_key},dst=/fixture/server.key,readonly', '--entrypoint', '/fixture/server', 'debian:bookworm-slim')
            run('start', fn)
            common = flags() + ['--mount', f'type=bind,src={cfg},dst=/work/kelvo.yml,readonly', '--mount', f'type=bind,src={bundle},dst=/etc/ssl/certs/ca-certificates.crt,readonly', '--mount', f'type=bind,src={outdir},dst=/work', '--env', 'KELVO_SOURCE_ADAPTER_URL=https://fixture:8443', '--env', 'KELVO_SOURCE_ADAPTER_TOKEN=' + token, '--entrypoint', '/usr/local/bin/kelvo', args.image, 'query', '--config', '/work/kelvo.yml', '--mode', 'native', '--connection']
            qn = pre + '-query'
            time.sleep(1)
            create(qn, *common, 'fixture', '--sql', 'SELECT 9007199254740993', '--out', '/work/result.arrow', '--timeout', '10s')
            first = run('start', '-a', qn, check=False, timeout=25)
            if first.returncode:
                raise AssertionError('registered native adapter query failed (fixture rejected the request)')
            result['tls_verified'] = True
            copied_output = w / 'copied-result.arrow'
            copied_output.write_bytes(run('run', '--rm', '--pull=never', '--mount', f'type=bind,src={outdir},dst=/work,readonly', '--entrypoint', '/bin/cat', 'debian:bookworm-slim', '/work/result.arrow').stdout)
            os.chmod(copied_output, 0o600)
            with ipc.open_stream(copied_output) as r:
                table = r.read_all()
            field = table.schema.field('document_json')
            meta = {k.decode(): v.decode() for k, v in (field.metadata or {}).items()}
            docs = table.column('document_json').to_pylist()
            expect = [b'{"n":9007199254740993,"nullable":null}', b'{"n":null,"nullable":"ok"}']
            assert str(field.type) == 'binary' and (not field.nullable) and (meta.get('content_type') == 'application/json') and (meta.get('adapter') == 'dbapi') and (docs == expect) and (b'9007199254740993' in docs[0])
            result['assertions']['arrow_binary_document_json'] = {'passed': True, 'rows': 2, 'raw_large_integer_preserved': True, 'null_preserved': True}
            result['assertions']['token_reached_worker'] = {'passed': True}
            bn = pre + '-bad'
            create(bn, *common, 'unregistered', '--sql', 'SELECT 9007199254740993', '--out', '/work/result.arrow', '--timeout', '10s')
            assert run('start', '-a', bn, check=False, timeout=25).returncode != 0
            result['assertions']['unregistered_connection_denied'] = {'passed': True}
            result['container_policy'] = {'internal_network': True, 'nonroot_user': '65532:65532', 'capabilities_dropped': True, 'no_new_privileges': True}
            hs = run('run', '--rm', '--pull=never', '--entrypoint', '/usr/bin/sha256sum', args.image, '/usr/local/bin/kelvo', '/usr/local/bin/kelvo-landlock').stdout.decode().splitlines()
            result['image_binaries'] = {z.split()[-1]: z.split()[0] for z in hs}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + '\n')
        os.chmod(args.output, 0o600)
        print(json.dumps(result, indent=2, sort_keys=True))
    finally:
        for n in reversed(made):
            run('rm', '-f', n, check=False)
        run('network', 'rm', net, check=False)
if __name__ == '__main__':
    main()

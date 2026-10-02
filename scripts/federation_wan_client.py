#!/usr/bin/env python3
"""Measure a real remote client through an SSH tunnel without saving result data.

The VM must already host the private capacity cluster. Manifest, fixture request,
token and CA are read over SSH into memory. Only public timing/hash evidence is
written locally. No credentials enter command arguments or the evidence output.
The trusted fixture request file has request, expected_wire_bytes,
expected_wire_sha256 and expected_rows fields, established by VM validation.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shlex
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ssh-target", required=True)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--request", required=True, help="private fixture request path on the VM")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    if not 1 <= args.trials <= 10 or args.ssh_target.startswith("-"):
        parser.error("invalid trial count or SSH target")
    ssh = ["ssh", "-i", str(Path(args.identity).expanduser()), "-o", "IdentitiesOnly=yes",
           "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
    fetch = """import json,pathlib,sys
m=json.loads(pathlib.Path(sys.argv[1]).read_text())
e=json.loads(pathlib.Path(m['environment_file']).read_text())
r=json.loads(pathlib.Path(sys.argv[2]).read_text())
print(json.dumps({'gateway':m['gateway_urls'][0], 'ca':pathlib.Path(m['ca_file']).read_text(),
 'token':e[m['token_envs']['a']], 'fixture':r, 'binary_sha256':m.get('binary_sha256')}))
"""
    command = shlex.join(["python3", "-c", fetch, args.manifest, args.request])
    private = json.loads(subprocess.check_output(ssh + [args.ssh_target, command], timeout=30))
    remote = urllib.parse.urlsplit(private["gateway"])
    if remote.scheme != "https" or remote.hostname != "127.0.0.1" or not remote.port:
        raise RuntimeError("fixture gateway must be verified TLS on remote loopback")
    fixture = private["fixture"]
    if not private.get("binary_sha256") or fixture.get("binary_sha256") != private["binary_sha256"]:
        raise RuntimeError("fixture reference and running cluster binary identities differ")
    context = ssl.create_default_context(cadata=private["ca"])
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                                         urllib.request.HTTPSHandler(context=context))
    with socket.socket() as available:
        available.bind(("127.0.0.1", 0))
        port = available.getsockname()[1]
    tunnel = subprocess.Popen(ssh + ["-N", "-T", "-o", "ExitOnForwardFailure=yes",
        "-o", "ServerAliveInterval=15", "-L", f"127.0.0.1:{port}:127.0.0.1:{remote.port}",
        args.ssh_target], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    base = f"https://127.0.0.1:{port}"

    def call(path, body=None):
        request = urllib.request.Request(base + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + private["token"], "Content-Type": "application/json"})
        return opener.open(request, timeout=180)

    trials, latency = [], []
    try:
        deadline = time.monotonic() + 20
        while True:
            if tunnel.poll() is not None:
                raise RuntimeError("SSH tunnel exited before readiness")
            try:
                with call("/ready") as response:
                    if response.status == 200:
                        break
            except urllib.error.URLError as error:
                if isinstance(error.reason, ssl.SSLCertVerificationError):
                    raise RuntimeError("remote fixture TLS verification failed: " + error.reason.verify_message) from None
            except OSError:
                pass
            if time.monotonic() >= deadline:
                raise RuntimeError("remote gateway did not become ready")
            time.sleep(0.1)
        for _ in range(3):
            started = time.perf_counter()
            with call("/health") as response:
                response.read()
            latency.append(time.perf_counter() - started)
        for trial in range(args.trials):
            identifier = None
            started = time.perf_counter()
            try:
                with call("/v1/queries", fixture["request"]) as response:
                    identifier = json.load(response)["id"]
                digest, size, tail, first_byte = hashlib.sha256(), 0, b"", None
                with call("/v1/queries/" + identifier + "/results") as response:
                    while True:
                        block = response.read(65536)
                        if not block:
                            break
                        if first_byte is None:
                            first_byte = time.perf_counter() - started
                        size += len(block)
                        digest.update(block)
                        tail = (tail + block)[-8:]
                export_seconds = time.perf_counter() - started
                if tail != b"\xff\xff\xff\xff\0\0\0\0":
                    raise RuntimeError("remote result omitted Arrow completion marker")
                if size != fixture["expected_wire_bytes"] or digest.hexdigest() != fixture["expected_wire_sha256"]:
                    raise RuntimeError("remote result differs from the exact validated VM reference")
                with call("/v1/queries/" + identifier) as response:
                    if json.load(response)["state"] != "succeeded":
                        raise RuntimeError("remote query did not finish successfully")
                verified_seconds = time.perf_counter() - started
                trials.append({"trial": trial + 1, "export_seconds": export_seconds,
                    "verified_seconds": verified_seconds, "first_64k_seconds": first_byte,
                    "wire_bytes": size, "wire_sha256": digest.hexdigest(),
                    "rows": fixture["expected_rows"],
                    "rows_per_export_second": fixture["expected_rows"] / export_seconds,
                    "wire_mb_per_export_second": size / export_seconds / 1_000_000})
                print(f"Remote export trial {trial+1}: {size} bytes in {export_seconds:.3f}s; reference matched", flush=True)
            except BaseException:
                if identifier:
                    try:
                        with call("/v1/queries/" + identifier + "/cancel", {}) as response:
                            response.read()
                    except OSError:
                        pass
                raise
    finally:
        tunnel.terminate()
        try:
            tunnel.wait(timeout=5)
        except subprocess.TimeoutExpired:
            tunnel.kill()
            tunnel.wait()
    evidence = {"passed": True, "checked_at": datetime.now(timezone.utc).isoformat(),
        "binary_sha256": private.get("binary_sha256"), "trials": trials,
        "request": fixture["request"],
        "reference_canonical_value_sha256": fixture["canonical_value_sha256"],
        "health_request_seconds": latency,
        "scope": "Actual macOS client to Linux VM over SSH forwarding, with inner verified TLS; no result data saved locally.",
        "limitations": ["SSH encryption, buffering and inner TLS are included; this is not direct HTTPS WAN capacity.",
            "The standard-library client opens an HTTPS connection per request; connection and TLS setup are included.",
            "Health request timing includes connection/TLS/server overhead and is not a pure network RTT.",
            "Export timing includes submission, execution and complete body read; final status verification is separate.",
            "Result bytes and SHA match a VM-decoded exact reference; no local Arrow library or database execution is used."]}
    Path(args.output).write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    main()

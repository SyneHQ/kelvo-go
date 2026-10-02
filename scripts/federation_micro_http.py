#!/usr/bin/env python3
"""Measure a local client exporting Arrow from a provisioned micro VM over SSH.

HTTP connects only to a local loopback SSH forward, whose destination is remote
loopback. SSH encrypts the remote transport; this script does not use HTTP TLS.
The private remote manifest and token stay in memory. Results are hashed while
reading and discarded; only public timing and reference evidence is saved.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import signal
import socket
import subprocess
import sys
import time


MAX_JSON = 1 << 20
EXPORT_TIMEOUT = 180
EOS = b"\xff\xff\xff\xff\0\0\0\0"
NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]{0,127}\Z")
HASH = re.compile(r"[a-f0-9]{64}\Z")
HANDLE = re.compile(r"[a-f0-9]{32}\Z")
FETCH_MANIFEST = r'''import os,pathlib,stat,sys
try:
    path=pathlib.Path(sys.argv[1]); info=path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > 1048576:
        sys.exit(1)
    data=path.read_bytes()
    if len(data)>1048576:
        sys.exit(1)
    sys.stdout.buffer.write(data)
except BaseException:
    sys.exit(1)
'''


class ClientError(Exception):
    """Fixed public error codes only; never expose arbitrary server messages."""


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def write_report(path, value):
    temporary = path.with_name(path.name + ".tmp-" + secrets.token_hex(4))
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(value, output, indent=2, allow_nan=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def bounded_integer(value, low, high):
    return type(value) is int and low <= value <= high


def validate_manifest(value):
    if not isinstance(value, dict) or set(value) != {"port", "token", "binary_sha256", "cases"}:
        raise ClientError("invalid_manifest_fields")
    if not bounded_integer(value["port"], 1024, 65535):
        raise ClientError("invalid_remote_loopback_port")
    token = value["token"]
    if not isinstance(token, str) or not 32 <= len(token) <= 4096 or any(ord(c) < 33 or ord(c) > 126 for c in token):
        raise ClientError("invalid_manifest_token")
    if not isinstance(value["binary_sha256"], str) or not HASH.fullmatch(value["binary_sha256"]):
        raise ClientError("invalid_binary_digest")
    cases = value["cases"]
    if not isinstance(cases, list) or not 1 <= len(cases) <= 10:
        raise ClientError("manifest_requires_1_to_10_cases")
    names = set()
    required = {"name", "request", "expected_wire_sha256", "expected_wire_bytes",
                "expected_rows", "canonical_value_sha256"}
    for case in cases:
        if not isinstance(case, dict) or set(case) != required:
            raise ClientError("invalid_case_fields")
        if not isinstance(case["name"], str) or not NAME.fullmatch(case["name"]) or case["name"] in names:
            raise ClientError("invalid_or_duplicate_case_name")
        names.add(case["name"])
        for key in ("expected_wire_sha256", "canonical_value_sha256"):
            if not isinstance(case[key], str) or not HASH.fullmatch(case[key]):
                raise ClientError("invalid_reference_digest")
        if not bounded_integer(case["expected_wire_bytes"], 8, 1 << 30) or not bounded_integer(case["expected_rows"], 0, 100_000_000):
            raise ClientError("reference_size_outside_client_limits")
        request = case["request"]
        allowed = {"sql", "mode", "sources", "connection_id", "parameters"}
        if not isinstance(request, dict) or not set(request) <= allowed or request.get("mode") not in ("native", "federated"):
            raise ClientError("invalid_query_request")
        if not isinstance(request.get("sql"), str) or not request["sql"] or "\0" in request["sql"] or len(json.dumps(request)) > 65536:
            raise ClientError("invalid_query_sql_or_request_size")
        if request["mode"] == "native":
            source = request.get("connection_id")
            if not isinstance(source, str) or not NAME.fullmatch(source):
                raise ClientError("native_request_requires_connection_id")
        else:
            sources = request.get("sources")
            if not isinstance(sources, list) or not 1 <= len(sources) <= 8 or any(not isinstance(source, str) or not NAME.fullmatch(source) for source in sources):
                raise ClientError("federated_request_requires_sources")
    return value


@contextmanager
def deadline(seconds):
    """A true wall deadline also interrupts HTTP headers or trickling reads."""
    def expired(*_):
        raise ClientError("operation_deadline_exceeded")
    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


class LoopbackClient:
    def __init__(self, port, token):
        self.port, self.token = port, token

    @contextmanager
    def response(self, path, body=None, authenticated=True, timeout=10):
        # No proxies, redirects, manifest URLs, or user-provided request paths.
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        headers = {"Connection": "close"}
        if authenticated:
            headers["Authorization"] = "Bearer " + self.token
        data = None if body is None else json.dumps(body, separators=(",", ":")).encode()
        if data is not None:
            headers["Content-Type"] = "application/json"
        try:
            connection.request("GET" if body is None else "POST", path, body=data, headers=headers)
            response = connection.getresponse()
            try:
                yield response
            finally:
                response.close()
        finally:
            connection.close()

    def json(self, path, body=None, expected_status=200, authenticated=True, timeout=10):
        with self.response(path, body, authenticated, timeout) as response:
            if response.status != expected_status:
                raise ClientError("unexpected_http_status_" + str(response.status))
            payload = response.read(MAX_JSON + 1)
            if len(payload) > MAX_JSON:
                raise ClientError("json_response_exceeds_limit")
            return json.loads(payload)


def wait_for_service(client, tunnel):
    with deadline(25):
        while True:
            if tunnel.poll() is not None:
                raise ClientError("ssh_forward_exited_before_readiness")
            try:
                if client.json("/health", timeout=2) == {"status": "ok"}:
                    break
            except ClientError as error:
                if str(error) == "operation_deadline_exceeded":
                    raise
            except (OSError, http.client.HTTPException, ValueError):
                pass
            time.sleep(0.2)
    # /health is public in the standalone API. Verify the bearer token on an
    # authenticated route without creating an extra execution or query handle.
    probe = "/v1/queries/" + secrets.token_hex(16)
    statuses = {}
    with deadline(10):
        for authenticated, expected in ((False, 401), (True, 404)):
            with client.response(probe, authenticated=authenticated) as response:
                statuses["authenticated" if authenticated else "unauthenticated"] = response.status
                if response.status != expected:
                    raise ClientError("bearer_authentication_probe_failed")
    return statuses


def run_trial(client, case, trial):
    result = {"case": case["name"], "mode": case["request"]["mode"], "trial": trial,
              "started_at": utc_now(), "state": "failed"}
    identifier, first_64k, size, tail = None, None, 0, b""
    digest = hashlib.sha256()
    started = time.perf_counter()
    try:
        with deadline(EXPORT_TIMEOUT):
            created = client.json("/v1/queries", case["request"], expected_status=201)
            identifier = created.get("id") if isinstance(created, dict) else None
            if not isinstance(identifier, str) or not HANDLE.fullmatch(identifier):
                identifier = None
                raise ClientError("invalid_query_handle")
            result["submission_seconds"] = time.perf_counter() - started
            with client.response("/v1/queries/" + identifier + "/results", timeout=EXPORT_TIMEOUT) as response:
                if response.status != 200:
                    raise ClientError("results_http_status_" + str(response.status))
                if response.getheader("Content-Type", "").split(";", 1)[0].strip() != "application/vnd.apache.arrow.stream":
                    raise ClientError("unexpected_result_content_type")
                while True:
                    block = response.read1(65536)
                    if not block:
                        break
                    size += len(block)
                    digest.update(block)
                    tail = (tail + block)[-8:]
                    if first_64k is None and size >= 65536:
                        first_64k = time.perf_counter() - started
                    if size > case["expected_wire_bytes"]:
                        raise ClientError("result_exceeds_reference_bytes")
            result["export_seconds"] = time.perf_counter() - started
        result["wire_matches_reference"] = (size == case["expected_wire_bytes"] and
            digest.hexdigest() == case["expected_wire_sha256"])
        status_started = time.perf_counter()
        with deadline(10):
            final = client.json("/v1/queries/" + identifier)
        result["final_state_check_seconds"] = time.perf_counter() - status_started
        result["verified_seconds"] = time.perf_counter() - started
        state = final.get("state") if isinstance(final, dict) else None
        result["api_final_state"] = state if state in ("queued", "running", "streaming", "succeeded", "failed", "cancelled", "expired") else "unknown"
        rows = final.get("stats", {}).get("rows") if isinstance(final, dict) and isinstance(final.get("stats"), dict) else None
        result["api_reported_rows"] = rows if bounded_integer(rows, 0, 100_000_000) else None
        if state != "succeeded":
            raise ClientError("query_did_not_succeed")
        if tail != EOS:
            raise ClientError("arrow_eos_missing")
        if not result["wire_matches_reference"]:
            raise ClientError("wire_reference_mismatch")
        if result["api_reported_rows"] != case["expected_rows"]:
            raise ClientError("api_rows_reference_mismatch")
        result["wire_mb_per_export_second"] = size / result["export_seconds"] / 1_000_000
        result["reference_rows_per_export_second"] = case["expected_rows"] / result["export_seconds"]
        result["state"] = "completed"
    except KeyboardInterrupt:
        result["error"] = "interrupted"
    except ClientError as error:
        result["error"] = str(error)
    except (OSError, http.client.HTTPException, ValueError):
        result["error"] = "http_transport_or_response_failed"
    finally:
        result.update({"wire_bytes_read": size, "wire_sha256": digest.hexdigest(),
            "arrow_eos_present": tail == EOS, "first_64k_seconds": first_64k,
            "attempt_seconds_before_cleanup": time.perf_counter() - started})
        if identifier and result["state"] != "completed":
            try:
                with deadline(10):
                    client.json("/v1/queries/" + identifier + "/cancel", {})
                result["cancel_request_succeeded"] = True
            except (OSError, http.client.HTTPException, ValueError, ClientError):
                result["cancel_request_succeeded"] = False
        result["finished_at"] = utc_now()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Remote manifest (owned by the SSH user, mode 0600, at most 1 MiB):
{\"port\":18080,\"token\":\"PRIVATE_BEARER_TOKEN\",\"binary_sha256\":\"64_hex_digits\",
 \"cases\":[{\"name\":\"native_million\",\"request\":{\"sql\":\"SELECT ...\",
   \"mode\":\"native\",\"connection_id\":\"taxi\"},\"expected_wire_sha256\":\"64_hex_digits\",
   \"expected_wire_bytes\":24000000,\"expected_rows\":1000000,
   \"canonical_value_sha256\":\"64_hex_digits\"}]}
Federated request: {\"sql\":\"SELECT ...\",\"mode\":\"federated\",\"sources\":[\"taxi\"]}.
List native/federated cases in alternating order; each trial runs that full list.
The provisioner must establish that the running server and independently decoded
CLI reference outputs use binary_sha256. The HTTP API does not expose its digest.
1-10 cases, 1-5 trials; each export has a hard 180-second wall deadline. Server
query limits are provisioned separately. Output must be a new absolute path.
""")
    parser.add_argument("--ssh-target", required=True)
    parser.add_argument("--identity", required=True)
    parser.add_argument("--manifest", required=True, help="absolute private manifest path on the VM")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    identity, output = Path(args.identity).expanduser(), Path(args.output)
    if sys.platform not in ("darwin", "linux") or not bounded_integer(args.trials, 1, 5):
        parser.error("requires macOS/Linux and 1-5 trials")
    if not re.fullmatch(r"(?:[A-Za-z0-9_.-]+@)?[A-Za-z0-9_.-]+", args.ssh_target) or args.ssh_target.startswith("-"):
        parser.error("invalid SSH target")
    if not identity.is_absolute() or not identity.is_file() or not args.manifest.startswith("/") or "\0" in args.manifest or len(args.manifest) > 4096:
        parser.error("identity and remote manifest must be valid absolute paths")
    if not output.is_absolute() or output.exists() or output.is_symlink() or not output.parent.is_dir():
        parser.error("output must be a new absolute file in an existing directory")
    report = {"schema_version": 1, "started_at": utc_now(), "state": "running", "results": [],
        "client_platform": sys.platform, "trials_per_case": args.trials,
        "client_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "export_timeout_seconds": EXPORT_TIMEOUT,
        "transport": "HTTP on loopback through an SSH tunnel; no HTTP TLS",
        "limitations": [
            "SSH transport encryption, buffering and connection setup affect timings; this is not direct public HTTP or HTTPS capacity.",
            "Submission and complete Arrow body read are timed together, including rolling SHA256; final state lookup is timed separately.",
            "Each HTTP request opens a new connection over the existing SSH tunnel; health timing is not a pure network RTT.",
            "Reference byte hashes, row counts and canonical digests were supplied by the provisioner after independent CLI output validation.",
            "Binary identity is supplied by the provisioned manifest; the standalone HTTP API does not expose a binary digest.",
            "The client checks exact Arrow bytes and EOS, plus the API-reported row count; it does not decode Arrow or recompute canonical values.",
            "Cases run in manifest order within each trial; no caches are dropped. Results are discarded after hashing."]}
    write_report(output, report)
    ssh = ["ssh", "-i", str(identity), "-o", "IdentitiesOnly=yes", "-o", "BatchMode=yes",
           "-o", "ConnectTimeout=15", "-o", "ControlMaster=no", "-o", "ControlPath=none",
           "-o", "ForkAfterAuthentication=no"]
    tunnel = None
    interrupted = False
    previous_term = signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        command = shlex.join(["python3", "-c", FETCH_MANIFEST, args.manifest])
        fetched = subprocess.run(ssh + [args.ssh_target, command], stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=30, check=False)
        if fetched.returncode or len(fetched.stdout) > MAX_JSON:
            raise ClientError("private_manifest_fetch_failed")
        manifest = validate_manifest(json.loads(fetched.stdout))
        report["binary_sha256"] = manifest["binary_sha256"]
        report["cases"] = [{**{key: case[key] for key in
            ("name", "expected_wire_sha256", "expected_wire_bytes", "expected_rows", "canonical_value_sha256")},
            "mode": case["request"]["mode"]} for case in manifest["cases"]]
        with socket.socket() as available:
            available.bind(("127.0.0.1", 0))
            port = available.getsockname()[1]
        tunnel = subprocess.Popen(ssh + ["-N", "-T", "-o", "ExitOnForwardFailure=yes",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=2", "-L",
            f"127.0.0.1:{port}:127.0.0.1:{manifest['port']}", args.ssh_target],
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        client = LoopbackClient(port, manifest["token"])
        report["bearer_authentication_probe_http_status"] = wait_for_service(client, tunnel)
        report["health_request_seconds"] = []
        for _ in range(3):
            with deadline(10):
                started = time.perf_counter()
                client.json("/health")
                report["health_request_seconds"].append(time.perf_counter() - started)
        write_report(output, report)
        for trial in range(1, args.trials + 1):
            for case in manifest["cases"]:
                if tunnel.poll() is not None:
                    raise ClientError("ssh_forward_exited")
                result = run_trial(client, case, trial)
                report["results"].append(result)
                write_report(output, report)
                print(json.dumps({"case": case["name"], "trial": trial, "state": result["state"]}), flush=True)
                if result.get("error") == "interrupted":
                    raise KeyboardInterrupt
                if result["state"] != "completed":
                    raise ClientError("stopped_after_failed_trial")
        report["state"] = "completed"
    except KeyboardInterrupt:
        interrupted = True
        report["error"] = "interrupted"
    except ClientError as error:
        report["error"] = str(error)
    except (OSError, ValueError, subprocess.TimeoutExpired, http.client.HTTPException):
        report["error"] = "setup_transport_or_response_failed"
    finally:
        if tunnel is not None:
            tunnel.terminate()
            try:
                tunnel.wait(timeout=5)
            except subprocess.TimeoutExpired:
                tunnel.kill()
                tunnel.wait(timeout=5)
            report["owned_ssh_process_exit_status"] = tunnel.returncode
        signal.signal(signal.SIGTERM, previous_term)
        report["finished_at"] = utc_now()
        if "error" in report:
            report["state"] = "interrupted" if interrupted else "failed"
        report["all_exports_matched_reference"] = report["state"] == "completed"
        write_report(output, report)
    return 0 if report["state"] == "completed" else 1


if __name__ == "__main__":
    sys.exit(main())

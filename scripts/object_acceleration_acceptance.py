#!/usr/bin/env python3
"""VM-only CLI acceptance against private TLS object-storage protocol fixtures.

This is not real-cloud acceptance. Requires built Kelvo, approved httpfs/azure
extensions, pyarrow and OpenSSL. Installs no packages. --install-test-ca is only
for the dedicated test VM: it temporarily installs one uniquely named public CA
certificate and removes that certificate in finally. No existing trust is edited.
Synthetic credentials live in memory; configs contain environment references.
Private source, certificates and redacted diagnostics remain in ignored artifacts.
"""

import argparse
import base64
import copy
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import re
import signal
import ssl
import statistics
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
import uuid

import pyarrow.parquet as pq

from acceleration_acceptance import (
    Acceptance, ROOT, BIN, TYPED_SCHEMA, TYPED_SQL, check_typed, dataset,
    private_directory, require, typed_csv, write_config, write_private,
)

PROVIDERS = ("s3", "r2", "gcs", "azure")
EVIDENCE = ROOT / "docs/evidence/object-acceleration.json"
EMPTY_SHA = hashlib.sha256(b"").hexdigest()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def mac(key, data):
    return hmac.new(key, data.encode(), hashlib.sha256).digest()


def signature(secret, scope, string_to_sign):
    key = ("AWS4" + secret).encode()
    for component in scope.split("/"):
        key = mac(key, component)
    return hmac.new(key, string_to_sign.encode(), hashlib.sha256).hexdigest()


def canonical_query(query):
    quote = lambda value: urllib.parse.quote(value, safe="-_.~")
    return "&".join(sorted(quote(k) + "=" + quote(v)
                           for k, v in urllib.parse.parse_qsl(query, keep_blank_values=True)))


class FixtureState:
    def __init__(self, provider):
        self.provider = provider
        self.objects = {}
        self.events = []
        self.lock = threading.Lock()
        self.sequence = 0
        self.fault = None
        self.reader_id = "KELVOREAD" + uuid.uuid4().hex[:12].upper()
        self.writer_id = "KELVOWRITE" + uuid.uuid4().hex[:12].upper()
        self.reader_secret = uuid.uuid4().hex + uuid.uuid4().hex
        self.writer_secret = uuid.uuid4().hex + uuid.uuid4().hex
        self.reader_sas = self.sas("r")
        self.writer_sas = self.sas("rcw")
        self.auth_failures = []

    @staticmethod
    def sas(permissions):
        return urllib.parse.urlencode({
            "sv": "2023-11-03", "sp": permissions, "sr": "c", "spr": "https",
            "se": "2030-01-01T00:00:00Z", "sig": base64.b64encode(os.urandom(32)).decode(),
        })

    def authenticate(self, handler, parsed, body):
        if self.provider == "azure":
            supplied = urllib.parse.parse_qs(parsed.query)
            for name, sas in (("reader", self.reader_sas), ("writer", self.writer_sas)):
                expected = urllib.parse.parse_qs(sas)
                if all(supplied.get(k) == v for k, v in expected.items()):
                    return name
            return ""
        auth = handler.headers.get("Authorization", "")
        match = re.fullmatch(
            r"AWS4-HMAC-SHA256\s+Credential=([^/]+)/([^,]+),\s*SignedHeaders=([^,]+),\s*Signature=([0-9a-f]{64})", auth)
        if match is None:
            self.auth_failures.append("unsupported_signature_format")
            return ""
        access_id, scope, signed_headers, supplied = match.groups()
        known = {self.reader_id: ("reader", self.reader_secret), self.writer_id: ("writer", self.writer_secret)}
        if access_id not in known:
            self.auth_failures.append("unknown_access_key")
            return ""
        identity, secret = known[access_id]
        payload_hash = handler.headers.get("x-amz-content-sha256", EMPTY_SHA)
        if handler.command == "PUT" and payload_hash != digest(body):
            self.auth_failures.append("body_digest_mismatch")
            return ""
        canonical_headers = "".join(
            name + ":" + ",".join(" ".join(value.split()) for value in handler.headers.get_all(name, [])) + "\n"
            for name in signed_headers.split(";"))
        canonical = "\n".join((handler.command, parsed.path, canonical_query(parsed.query),
                                canonical_headers, signed_headers, payload_hash))
        string_to_sign = "\n".join(("AWS4-HMAC-SHA256", handler.headers.get("x-amz-date", ""), scope, digest(canonical.encode())))
        if not hmac.compare_digest(signature(secret, scope, string_to_sign), supplied):
            self.auth_failures.append("signature_mismatch")
            return ""
        return identity

    def event_count(self):
        with self.lock:
            return len(self.events)

    def events_since(self, start):
        with self.lock:
            return copy.deepcopy(self.events[start:])


class FixtureServer(ThreadingHTTPServer):
    def __init__(self, *args):
        super().__init__(*args)
        self.fixture_errors = []

    def handle_error(self, request, client_address):
        error = sys.exc_info()[1]
        if not isinstance(error, (BrokenPipeError, ConnectionResetError, ssl.SSLError)):
            self.fixture_errors.append(type(error).__name__)


class FixtureHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        # BaseHTTPRequestHandler otherwise logs the SAS-bearing request URL.
        pass

    def do_HEAD(self):
        self.handle_object()

    def do_GET(self):
        self.handle_object()

    def do_PUT(self):
        self.handle_object()

    def reply(self, code, body=b"", headers=None):
        self.send_response(code)
        for name, value in (headers or {}).items():
            if value is not None:
                self.send_header(name, value)
        if not headers or "Content-Length" not in headers:
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD" and body:
            self.wfile.write(body)

    def handle_object(self):
        state = self.server.state
        parsed = urllib.parse.urlsplit(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        if length < 0 or length > 8 << 20:
            self.reply(413)
            return
        body = self.rfile.read(length) if length else b""
        with state.lock:
            identity = state.authenticate(self, parsed, body)
            key = parsed.path
            kind = "parquet" if key.endswith(".parquet") else "manifest"
            event = {"method": self.command, "identity": identity or "invalid", "kind": kind,
                     "range": self.headers.get("Range", self.headers.get("x-ms-range", "")), "bytes": 0, "status": 0}
            state.events.append(event)
            def respond(code, data=b"", headers=None):
                event["status"] = code
                event["bytes"] = len(data) if self.command == "GET" else 0
                self.reply(code, data, headers)
            if not identity or not key.startswith("/snapshots/private/acceptance/"):
                respond(403)
                return
            if self.command == "PUT" and identity != "writer":
                respond(403)
                return
            current = state.objects.get(key)
            if self.command == "PUT":
                if state.provider == "gcs":
                    condition = self.headers.get("x-goog-if-generation-match")
                    allowed = condition == "0" if current is None else condition == str(current["generation"])
                    metadata_key = "x-goog-meta-kelvo-sha256"
                else:
                    allowed = (self.headers.get("If-None-Match") == "*" and current is None) or (
                        current is not None and self.headers.get("If-Match") == current["etag"])
                    metadata_key = "x-ms-meta-kelvo-sha256" if state.provider == "azure" else "x-amz-meta-kelvo-sha256"
                if not allowed:
                    respond(412)
                    return
                if self.headers.get(metadata_key) != digest(body):
                    respond(400)
                    return
                if state.provider == "azure" and self.headers.get("x-ms-blob-type") != "BlockBlob":
                    respond(400)
                    return
                state.sequence += 1
                current = {"body": body, "digest": digest(body), "generation": state.sequence,
                           "etag": '"fixture-' + str(state.sequence) + '"'}
                state.objects[key] = current
                respond(201 if state.provider == "azure" else 200, headers=self.identity_headers(current))
                return
            if current is None or (kind == "parquet" and state.fault == "missing_object"):
                respond(404)
                return
            expected = self.headers.get("If-Match")
            generation = self.headers.get("x-goog-if-generation-match")
            if (expected and expected != current["etag"]) or (generation and generation != str(current["generation"])):
                respond(412)
                return
            data = current["body"]
            headers = self.identity_headers(current)
            headers.update({"Accept-Ranges": "bytes", "Content-Type": "application/octet-stream",
                            "Content-Length": str(len(data)), "Last-Modified": self.date_time_string()})
            if kind == "parquet":
                metadata_key = {"azure": "x-ms-meta-kelvo-sha256", "gcs": "x-goog-meta-kelvo-sha256"}.get(state.provider, "x-amz-meta-kelvo-sha256")
                if state.fault == "missing_digest":
                    headers.pop(metadata_key)
                elif state.fault == "corrupt_digest":
                    headers[metadata_key] = "0" * 64
                elif state.fault == "missing_version":
                    headers.pop("x-goog-generation" if state.provider == "gcs" else "ETag")
                elif state.fault == "unsupported_encoding":
                    headers["Content-Encoding"] = "gzip"
                elif state.fault == "missing_size":
                    headers["Content-Length"] = None
                    headers["Connection"] = "close"
                    self.close_connection = True
            requested_range = event["range"]
            if kind == "parquet" and self.command == "GET" and requested_range:
                if state.fault == "range_redirect":
                    respond(307, headers={"Location": state.redirect_target})
                    return
                if state.fault == "ignored_range":
                    respond(200, data, headers)
                    return
            if requested_range:
                match = re.fullmatch(r"bytes=(\d*)-(\d*)", requested_range)
                if not match or not any(match.groups()):
                    respond(416)
                    return
                start, end = match.groups()
                if start:
                    start, end = int(start), min(int(end) if end else len(data) - 1, len(data) - 1)
                else:
                    start, end = max(0, len(data) - int(end)), len(data) - 1
                if start > end or start >= len(data):
                    respond(416)
                    return
                headers["Content-Range"] = f"bytes {start}-{end}/{len(data)}"
                data = data[start:end + 1]
                headers["Content-Length"] = str(len(data))
                if kind == "parquet" and state.fault == "wrong_range":
                    headers["Content-Range"] = f"bytes {start + 1}-{end + 1}/{len(current['body']) + 1}"
                respond(206, data, headers)
            else:
                respond(200, data, headers)

    def identity_headers(self, current):
        state = self.server.state
        headers = {"ETag": current["etag"]}
        if state.provider == "gcs":
            headers["x-goog-generation"] = str(current["generation"])
            headers["x-goog-meta-kelvo-sha256"] = current["digest"]
        elif state.provider == "azure":
            headers.update({"x-ms-meta-kelvo-sha256": current["digest"], "x-ms-blob-type": "BlockBlob",
                            "x-ms-version": "2023-11-03", "x-ms-request-id": "fixture-request",
                            "x-ms-creation-time": self.date_time_string(), "x-ms-server-encrypted": "true"})
        else:
            headers["x-amz-meta-kelvo-sha256"] = current["digest"]
        return headers


class RedirectSinkHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.server.foreign_requests += 1
        self.send_response(403)
        self.send_header("Content-Length", "0")
        self.end_headers()

    do_HEAD = do_GET


class ObjectAcceptance(Acceptance):
    def __init__(self, args):
        os.umask(0o077)
        self.directory = private_directory(private_directory(ROOT / "artifacts/object-acceleration-private") / ("run-" + uuid.uuid4().hex))
        self.env = {key: value for key, value in os.environ.items() if not key.startswith("KELVO_SOURCE_")}
        self.env["GOMAXPROCS"] = "2"
        self.processes, self.catalog_backups = {}, {}
        self.command_number = 0
        self.checks, self.servers, self.states = [], [], []
        self.stage = "prerequisites"
        self.binary = args.binary.resolve()
        self.extension_directory = args.extension_directory.resolve()
        self.ca_destination = None
        self.secret_values = []
        self.writer_names = set()
        self.provider = None
        self.install_test_ca = args.install_test_ca

    def record(self, name, **data):
        self.checks.append({"test": name, "passed": True, "provider": self.provider, **data})
        print(self.provider + ": " + name + ": passed", flush=True)

    def redact(self, data):
        for value in self.secret_values:
            data = data.replace(value.encode(), b"<redacted>")
        return data

    def cli(self, args, success=True, timeout=45):
        self.command_number += 1
        if args and args[0] == "query":
            args = [*args, "--max-rows", "1000", "--max-bytes", str(8 << 20), "--timeout", "15s"]
        environment = dict(self.env)
        # Status, verification and selected snapshot queries must work without
        # publisher credentials, even in the parent CLI process.
        if args[:2] != ["accelerate", "refresh"]:
            for name in self.writer_names:
                environment.pop(name, None)
        scratch = private_directory(self.directory / ("process-" + str(self.command_number)))
        environment["TMPDIR"] = str(scratch)
        proc = subprocess.Popen([str(self.binary), *map(str, args)], env=environment,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except BaseException:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
            stdout, stderr = proc.communicate()
            write_private(self.directory / f"cli-{self.command_number}.log", self.redact(stdout + b"\n" + stderr))
            raise
        write_private(self.directory / f"cli-{self.command_number}.log", self.redact(stdout + b"\n" + stderr))
        require(not any(value.encode() in stdout + stderr for value in self.secret_values), "CLI exposed a synthetic credential")
        require((proc.returncode == 0) == success, "CLI returned an unexpected exit status")
        require(not list(scratch.rglob("*.parquet")), "CLI left a downloaded or staged Parquet file")
        return stdout, stderr

    def query(self, config, name="typed_snapshot", sql=None, success=True):
        columns = ", ".join(TYPED_SCHEMA)
        return super().query(config, name, sql or ("SELECT " + columns + " FROM " + name + " ORDER BY row_id"), success)

    def command(self, command):
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=45)
        require(result.returncode == 0, "fixture certificate operation failed")

    def setup_certificates(self):
        self.ca = self.directory / "ca.crt"
        ca_key, server_key = self.directory / "ca.key", self.directory / "server.key"
        request, certificate = self.directory / "server.csr", self.directory / "server.crt"
        self.command(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(ca_key),
                      "-out", str(self.ca), "-days", "2", "-subj", "/CN=KelvoObjectFixture-" + uuid.uuid4().hex,
                      "-addext", "basicConstraints=critical,CA:TRUE"])
        self.command(["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", str(server_key),
                      "-out", str(request), "-subj", "/CN=localhost"])
        extension = self.directory / "server.ext"
        write_private(extension, "subjectAltName=IP:127.0.0.1,DNS:localhost\nbasicConstraints=critical,CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
        self.command(["openssl", "x509", "-req", "-in", str(request), "-CA", str(self.ca), "-CAkey", str(ca_key),
                      "-set_serial", "2", "-out", str(certificate), "-days", "2", "-extfile", str(extension)])
        require(self.install_test_ca, "standalone CLI acceptance requires --install-test-ca on the dedicated VM")
        require(sys.platform == "linux", "temporary fixture trust is supported only on the Linux test VM")
        destination = Path("/usr/local/share/ca-certificates") / ("kelvo-object-fixture-" + uuid.uuid4().hex + ".crt")
        require(not destination.exists(), "unique fixture trust destination already exists")
        self.ca_destination = destination
        self.command(["sudo", "-n", "install", "-m", "0644", str(self.ca), str(destination)])
        self.command(["sudo", "-n", "update-ca-certificates"])
        self.server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.server_tls.minimum_version = ssl.TLSVersion.TLSv1_2
        self.server_tls.load_cert_chain(certificate, server_key)
        self.client_tls = ssl.create_default_context(cafile=str(self.ca))
        sink = FixtureServer(("127.0.0.1", 0), RedirectSinkHandler)
        sink.daemon_threads = True
        sink.foreign_requests = 0
        sink.socket = self.server_tls.wrap_socket(sink.socket, server_side=True)
        thread = threading.Thread(target=sink.serve_forever, daemon=True)
        thread.start()
        self.servers.append((sink, thread))
        self.redirect_sink = sink

    def fixture(self, provider):
        state = FixtureState(provider)
        state.redirect_target = "https://127.0.0.1:" + str(self.redirect_sink.server_port) + "/foreign-object"
        server = FixtureServer(("127.0.0.1", 0), FixtureHandler)
        server.daemon_threads = True
        server.state = state
        server.socket = self.server_tls.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.servers.append((server, thread))
        self.states.append(state)
        endpoint = "https://127.0.0.1:" + str(server.server_port)
        return state, endpoint

    def storage_config(self, state, endpoint):
        credentials = {}
        self.writer_names.clear()
        for role in ("reader", "writer"):
            prefix = "KELVO_SOURCE_OBJECT_" + role.upper()
            if state.provider == "azure":
                fields = {"sas_token_env": (prefix + "_SAS", getattr(state, role + "_sas"))}
            else:
                fields = {"access_key_id_env": (prefix + "_ID", getattr(state, role + "_id")),
                          "secret_access_key_env": (prefix + "_SECRET", getattr(state, role + "_secret"))}
            credentials[role] = {field: name for field, (name, _) in fields.items()}
            for name, value in fields.values():
                self.env[name] = value
                self.secret_values.append(value)
                if role == "writer":
                    self.writer_names.add(name)
        # Redact individual SAS signatures as well as full token strings.
        for sas in (state.reader_sas, state.writer_sas):
            value = urllib.parse.parse_qs(sas)["sig"][0]
            self.secret_values.extend((value, urllib.parse.quote(value, safe="")))
        config = {"provider": state.provider, "endpoint": endpoint, "bucket": "snapshots", "prefix": "private",
                  "read_credentials": credentials["reader"], "write_credentials": credentials["writer"]}
        if state.provider == "s3":
            config["region"] = "us-east-1"
        elif state.provider == "azure":
            config["account"] = "kelvofixture"
        return config

    def reader_put_is_denied(self, state, endpoint):
        body = b"reader must not publish"
        path = "/snapshots/private/acceptance/typed_snapshot/unauthorized.parquet"
        headers = {"Content-Type": "application/octet-stream", "If-None-Match": "*"}
        if state.provider == "azure":
            url = endpoint + path + "?" + state.reader_sas
            headers.update({"x-ms-version": "2023-11-03", "x-ms-blob-type": "BlockBlob",
                            "x-ms-meta-kelvo-sha256": digest(body)})
        else:
            url = endpoint + path
            now = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            scope = now[:8] + "/" + ("us-east-1" if state.provider == "s3" else "auto") + "/s3/aws4_request"
            signed_headers = "host;x-amz-content-sha256;x-amz-date"
            canonical_headers = "host:" + urllib.parse.urlsplit(endpoint).netloc + "\nx-amz-content-sha256:" + digest(body) + "\nx-amz-date:" + now + "\n"
            canonical = "\n".join(("PUT", path, "", canonical_headers, signed_headers, digest(body)))
            string_to_sign = "\n".join(("AWS4-HMAC-SHA256", now, scope, digest(canonical.encode())))
            headers.update({"x-amz-content-sha256": digest(body), "x-amz-date": now,
                            "Authorization": "AWS4-HMAC-SHA256 Credential=" + state.reader_id + "/" + scope + ", SignedHeaders=" + signed_headers + ", Signature=" + signature(state.reader_secret, scope, string_to_sign)})
        request = urllib.request.Request(url, data=body, headers=headers, method="PUT")
        try:
            with urllib.request.urlopen(request, context=self.client_tls, timeout=10) as response:
                code = response.status
        except urllib.error.HTTPError as error:
            code = error.code
        require(code == 403, "reader identity can publish an object")
        require(state.events[-1]["identity"] == "reader", "reader denial was caused by invalid credentials")
        require(path not in state.objects, "denied reader write changed object state")

    @contextmanager
    def fault(self, state, name):
        with state.lock:
            state.fault = name
        try:
            yield
        finally:
            with state.lock:
                state.fault = None

    def run_provider(self, provider):
        self.provider, self.stage = provider, provider + "_setup"
        state, endpoint = self.fixture(provider)
        directory = private_directory(self.directory / provider)
        source = directory / "source.csv"
        # Large, incompressible, unselected columns make partial reads measurable.
        # Source remains private and is removed before the fresh reader starts.
        import csv
        rows = list(csv.reader(io.StringIO(typed_csv())))
        rows[0].append("padding_text")
        for row in rows[1:]:
            row.append(base64.b64encode(os.urandom(768 << 10)).decode())
        csv_file = io.StringIO(newline="")
        csv.writer(csv_file).writerows(rows)
        source_data = csv_file.getvalue()
        write_private(source, source_data)
        refresh_sql = TYPED_SQL.replace(" FROM raw", ", padding_text AS padding FROM raw")
        config = {"extension_directory": str(self.extension_directory),
                  "sources": [{"id": "raw", "type": "csv", "path": str(source)}],
                  "acceleration": {"directory": str(directory / "publisher-stage"), "tenant_id": "acceptance",
                                   "object_storage": self.storage_config(state, endpoint),
                                   "datasets": [dataset("typed_snapshot", refresh_sql)]}}
        writer_catalog = directory / "publisher.yml"
        write_config(writer_catalog, config)
        self.stage = provider + "_cold_refresh"
        self.query(writer_catalog, success=False)
        first = self.status(writer_catalog, "typed_snapshot", command="refresh")
        require(first["ready"] and first["rows"] == 3, "refresh did not commit the expected snapshot")
        key = "/snapshots/private/acceptance/typed_snapshot/" + first["generation"] + ".parquet"
        payload = state.objects[key]["body"]
        require(digest(payload) == first["sha256"] and len(payload) == first["bytes"], "uploaded Parquet digest or length differs from manifest")
        uploaded = pq.read_table(io.BytesIO(payload)).select(list(TYPED_SCHEMA)).sort_by([("row_id", "ascending")])
        check_typed(uploaded)
        require(len(payload) > 2 << 20, "fixture Parquet is too small to demonstrate partial reads")
        self.record("refresh_publishes_exact_typed_parquet", rows=3, parquet_bytes=len(payload))

        # The local control uses the same refresh SQL and source bytes, then the
        # same selected-snapshot query after the original source is removed.
        local_config = copy.deepcopy(config)
        local_config["acceleration"].pop("object_storage")
        local_config["acceleration"]["directory"] = str(directory / "local-control-snapshots")
        local_catalog = directory / "local-control.yml"
        write_config(local_catalog, local_config)
        local_snapshot = self.status(local_catalog, "typed_snapshot", command="refresh")
        require(local_snapshot["ready"] and local_snapshot["rows"] == first["rows"] and
                local_snapshot["sha256"] == first["sha256"], "local control snapshot differs from remote snapshot")

        reader_config = copy.deepcopy(config)
        reader_stage = directory / "fresh-reader-stage"
        require(not reader_stage.exists(), "fresh reader scratch already exists")
        reader_config["acceleration"]["directory"] = str(reader_stage)
        reader_catalog = directory / "fresh-reader.yml"
        write_config(reader_catalog, reader_config)
        source.unlink()
        self.stage = provider + "_fresh_reader"
        start = state.event_count()
        table, stats = self.query(reader_catalog)
        check_typed(table)
        require(stats["accelerations"][0]["generation"] == first["generation"], "fresh reader selected another generation")
        events = state.events_since(start)
        reads = [event for event in events if event["kind"] == "parquet" and event["method"] == "GET"]
        require(reads and all(event["range"] and event["status"] == 206 for event in reads), "query made a full object GET instead of range reads")
        require(all(event["identity"] == "reader" and event["method"] in ("GET", "HEAD") for event in events), "query used publisher identity or wrote to object storage")
        bytes_read = sum(event["bytes"] for event in reads)
        require(bytes_read < len(payload), "projection downloaded the full Parquet payload")
        require(not reader_stage.exists() or not list(reader_stage.rglob("*.parquet")), "fresh reader downloaded a local snapshot")
        self.record("fresh_reader_uses_bounded_remote_ranges_without_source_or_shared_snapshot", range_requests=len(reads),
                    requested_bytes=bytes_read, parquet_bytes=len(payload))

        self.stage = provider + "_status_verify"
        require(self.status(reader_catalog, "typed_snapshot")["generation"] == first["generation"], "status changed the generation")
        require(self.status(reader_catalog, "typed_snapshot", command="verify")["generation"] == first["generation"], "verify changed the generation")
        self.reader_put_is_denied(state, endpoint)
        self.record("reader_only_status_verify_and_cloud_write_denial")

        self.stage = provider + "_local_control_timings"
        trials = []
        for trial in range(3):
            order = ["remote", "local"] if trial % 2 == 0 else ["local", "remote"]
            measured = {"trial": trial + 1, "order": order}
            for mode in order:
                started = time.perf_counter()
                check_typed(self.query(reader_catalog if mode == "remote" else local_catalog)[0])
                measured[mode + "_ms"] = round((time.perf_counter() - started) * 1000, 3)
            trials.append(measured)
        remote_median = statistics.median(trial["remote_ms"] for trial in trials)
        local_median = statistics.median(trial["local_ms"] for trial in trials)
        self.record("alternating_remote_and_local_snapshot_wall_timings", trials=trials, rows=3,
                    remote_median_ms=remote_median, local_median_ms=local_median,
                    median_difference_ms=round(remote_median - local_median, 3),
                    median_ratio=round(remote_median / local_median, 3),
                    limits={"max_rows": 1000, "max_bytes": 8 << 20, "timeout_seconds": 15,
                            "threads": 1, "memory_mb": 128, "max_temp_mb": 64,
                            "parent_gomaxprocs": 2, "worker_gomaxprocs": 1},
                    measured_scope="CLI process startup, query, Arrow export and exact value validation; loopback TLS fixtures, no WAN or real cloud")

        self.stage = provider + "_failed_refresh"
        self.status(writer_catalog, "typed_snapshot", command="refresh", success=False)
        require(self.status(reader_catalog, "typed_snapshot")["generation"] == first["generation"], "failed refresh replaced committed generation")
        check_typed(self.query(reader_catalog)[0])
        self.record("source_outage_refresh_preserves_committed_generation")

        self.stage = provider + "_metadata_failures"
        for name in ("missing_object", "missing_digest", "corrupt_digest", "missing_version", "missing_size", "unsupported_encoding"):
            with self.fault(state, name):
                self.query(reader_catalog, success=False)
        self.record("missing_corrupt_unsupported_object_metadata_fails_closed", failure_cases=6)
        self.stage = provider + "_range_failures"
        for name in ("range_redirect", "ignored_range", "wrong_range"):
            with self.fault(state, name):
                self.query(reader_catalog, success=False)
        require(self.redirect_sink.foreign_requests == 0, "range read followed a redirect to another origin")
        self.record("remote_redirect_ignored_range_and_wrong_interval_fail_closed", failure_cases=3, foreign_requests=0)
        manifest_key = "/snapshots/private/acceptance/typed_snapshot/current.yaml"
        with state.lock:
            manifest = state.objects[manifest_key]
            invalid = copy.deepcopy(manifest)
            invalid["body"] = re.sub(rb"^version: 2", b"version: 999", manifest["body"])
            require(invalid["body"] != manifest["body"], "manifest format fixture did not change")
            invalid["digest"] = digest(invalid["body"])
            state.objects[manifest_key] = invalid
        try:
            self.query(reader_catalog, success=False)
        finally:
            with state.lock:
                state.objects[manifest_key] = manifest
        self.record("unsupported_manifest_version_fails_closed")
        with state.lock:
            original = state.objects.pop(manifest_key)
        try:
            self.query(reader_catalog, success=False)
        finally:
            with state.lock:
                state.objects[manifest_key] = original
        check_typed(self.query(reader_catalog)[0])
        self.record("missing_manifest_fails_closed_and_restore_recovers")

    def cleanup(self):
        try:
            for server, thread in reversed(self.servers):
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)
                require(not server.fixture_errors, "fixture server encountered an unexpected handler error")
            for state in self.states:
                write_private(self.directory / (state.provider + "-requests.json"), json.dumps({"events": state.events, "auth_failures": state.auth_failures}, indent=2))
        finally:
            if self.ca_destination is not None:
                self.command(["sudo", "-n", "rm", "-f", "--", str(self.ca_destination)])
                self.command(["sudo", "-n", "update-ca-certificates"])
                require(not self.ca_destination.exists(), "temporary fixture trust was not removed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=BIN)
    parser.add_argument("--extension-directory", type=Path, required=True)
    parser.add_argument("--install-test-ca", action="store_true")
    parser.add_argument("--provider", choices=PROVIDERS, action="append", help="Limit a diagnostic run; default is all four")
    args = parser.parse_args()
    acceptance = ObjectAcceptance(args)
    outcome = {"schema_version": 1, "passed": False, "mode": "tls_protocol_fixtures",
               "checked_at": datetime.now(timezone.utc).isoformat(), "providers": args.provider or list(PROVIDERS),
               "checks": acceptance.checks, "scope": "Local TLS protocol acceptance only; no real cloud, throughput or production-readiness claim"}
    def interrupted(signum, frame):
        raise InterruptedError("acceptance interrupted")
    signal.signal(signal.SIGTERM, interrupted)
    result = 1
    try:
        require(acceptance.binary.is_file() and os.access(acceptance.binary, os.X_OK), "built Kelvo binary is unavailable")
        require(acceptance.extension_directory.is_dir(), "approved object extensions are unavailable")
        binary_hash = hashlib.sha256()
        with acceptance.binary.open("rb") as binary:
            for block in iter(lambda: binary.read(1 << 20), b""):
                binary_hash.update(block)
        outcome["binary_sha256"] = binary_hash.hexdigest()
        acceptance.setup_certificates()
        for provider in outcome["providers"]:
            acceptance.run_provider(provider)
        outcome["passed"], result = True, 0
    except BaseException as error:
        outcome["failure"] = {"stage": acceptance.stage, "type": type(error).__name__}
        write_private(acceptance.directory / "failure.log", acceptance.redact(traceback.format_exc().encode()))
        print("Object acceleration acceptance failed at " + acceptance.stage + "; diagnostics retained privately", file=sys.stderr)
    finally:
        # Finish exact trust cleanup even if a second interactive interrupt
        # arrives while joining the fixture's own HTTP threads.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        try:
            acceptance.cleanup()
        except BaseException as error:
            outcome["passed"] = False
            outcome["cleanup_failure"] = {"type": type(error).__name__}
            write_private(acceptance.directory / "cleanup-failure.log", acceptance.redact(traceback.format_exc().encode()))
            result = 1
        EVIDENCE.parent.mkdir(parents=True, exist_ok=True)
        EVIDENCE.write_text(json.dumps(outcome, indent=2) + "\n")
    return result


if __name__ == "__main__":
    sys.exit(main())

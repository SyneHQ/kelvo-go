"""Small, inspectable helpers for Kelvo's educational notebooks.

Queries run through the Kelvo executable or authenticated HTTP API. PyArrow
reads results; it is never a substitute query engine. This is not a production SDK.
Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
"""
from __future__ import annotations

import contextlib
import hashlib
import http.client
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import secrets
import socket
import subprocess
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

RELEASE = "v0.1.0-preview.1"
# Immutable archive identity; a mutable release manifest cannot replace this pin.
RELEASE_SHA256 = "a1d1ef598560c5b23b35ddb4b55ef04c8cf8ec391050a190ae08e4e19dd66d05"
RELEASE_ROOT = f"https://github.com/SyneHQ/kelvo-go/releases/download/{RELEASE}"
PENGUINS_URL = "https://raw.githubusercontent.com/allisonhorst/palmerpenguins/8957207b78d6ccd1b4654a9dd9c9041b657478ab/inst/extdata/penguins.csv"
DATASETS = {
    "penguins.csv": (PENGUINS_URL, "f204db2c753b0937caac3cb35258562c14f073e4bbc76be24b4c51ce22767a93", 1 << 20),
    "yellow_tripdata_2024-01.parquet": ("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet", "c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510", 64 << 20),
    "taxi_zone_lookup.csv": ("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv", "1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed", 1 << 20),
}


def _download(url: str, destination: Path, max_bytes: int, expected: str | None = None):
    """Bounded HTTPS download with atomic publication and optional SHA-256 pin."""
    if urllib.parse.urlsplit(url).scheme != "https":
        raise ValueError("Downloads require HTTPS")
    destination = Path(destination)
    digest = hashlib.sha256()
    size = 0
    tmp = destination.with_name(destination.name + "." + secrets.token_hex(8) + ".part")
    try:
        request = urllib.request.Request(url, headers={"User-Agent": "Kelvo-notebooks/1"})
        with urllib.request.urlopen(request, timeout=60) as response, tmp.open("xb") as output:
            if urllib.parse.urlsplit(response.url).scheme != "https":
                raise ValueError("Download redirect requires HTTPS")
            os.chmod(tmp, 0o600)
            while block := response.read(256 << 10):
                size += len(block)
                if size > max_bytes:
                    raise ValueError("Download exceeded its declared byte budget")
                digest.update(block)
                output.write(block)
        if expected and not secrets.compare_digest(digest.hexdigest(), expected):
            raise ValueError("Download checksum mismatch; file was not installed")
        tmp.replace(destination)
        return digest.hexdigest()
    finally:
        tmp.unlink(missing_ok=True)


def install_binary(root: Path) -> Path:
    """Use an explicit local binary or the checksummed Linux x86-64 preview."""
    override = os.environ.get("KELVO_BINARY")
    if override:
        binary = Path(override).expanduser().resolve(strict=True)
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise ValueError("KELVO_BINARY must be an executable file")
        return binary
    if platform.system() != "Linux" or platform.machine() not in ("x86_64", "amd64"):
        raise RuntimeError("Use a Linux x86-64 Colab CPU runtime, or build Kelvo and set KELVO_BINARY")
    name = "kelvo-linux-amd64.tar.gz"
    checksums = root / "SHA256SUMS"
    _download(RELEASE_ROOT + "/SHA256SUMS", checksums, 16 << 10)
    matches = [line.split()[0] for line in checksums.read_text().splitlines()
               if len(line.split()) == 2 and line.split()[1] == name]
    if len(matches) != 1 or not re.fullmatch(r"[0-9a-f]{64}", matches[0]):
        raise ValueError("Release has no unique valid checksum for this platform")
    if not secrets.compare_digest(matches[0], RELEASE_SHA256):
        raise ValueError("Release manifest does not match the pinned archive checksum")
    archive = root / name
    _download(RELEASE_ROOT + "/" + name, archive, 128 << 20, RELEASE_SHA256)
    # Copy only the expected regular file. Never extract archive paths or links.
    with tarfile.open(archive, "r:gz") as bundle:
        members = [entry for entry in bundle.getmembers() if entry.name == "kelvo"]
        if len(members) != 1 or not members[0].isfile() or not 0 < members[0].size <= 384 << 20:
            raise ValueError("Unexpected executable in release archive")
        member = bundle.extractfile(members[0])
        if member is None:
            raise ValueError("Release executable is absent")
        binary = root / "kelvo"
        with member, binary.open("xb") as output:
            while block := member.read(256 << 10):
                output.write(block)
    binary.chmod(0o700)
    subprocess.run([str(binary), "version"], check=True, capture_output=True, timeout=15)
    return binary


class QueryResult:
    def __init__(self, path: Path, stats: dict, elapsed_seconds: float):
        self.path, self.stats, self.elapsed_seconds = path, stats, elapsed_seconds

    def batches(self):
        """Read borrowed-by-Python batches; this does not bound DuckDB execution."""
        import pyarrow.ipc as ipc
        with self.path.open("rb") as source, ipc.open_stream(source) as reader:
            yield from reader

    def table(self):
        import pyarrow.ipc as ipc
        with self.path.open("rb") as source, ipc.open_stream(source) as reader:
            return reader.read_all()


class Lab:
    """One disposable local catalog and result directory per notebook."""
    def __init__(self):
        self._temporary = tempfile.TemporaryDirectory(prefix="kelvo-notebook-")
        self.root = Path(self._temporary.name)
        self._servers = []
        try:
            self.binary = install_binary(self.root)
        except BaseException:
            self.close()
            raise

    @classmethod
    def create(cls):
        return cls()

    def close(self):
        for process in self._servers:
            _stop(process)
        self._servers.clear()
        self._temporary.cleanup()

    def _data(self, filename):
        path = self.root / filename
        url, digest, limit = DATASETS[filename]
        if not path.exists():
            # An explicit local cache is useful for all-notebook acceptance. It
            # remains verified and is never a source of executable code.
            cache = os.environ.get("KELVO_NOTEBOOK_DATA")
            cached = Path(cache) / filename if cache else None
            if cached and cached.is_file():
                if cached.stat().st_size > limit:
                    raise ValueError("Cached dataset exceeds its byte budget")
                data = cached.read_bytes()
                if hashlib.sha256(data).hexdigest() != digest:
                    raise ValueError("Cached dataset checksum mismatch")
                path.write_bytes(data)
            else:
                _download(url, path, limit, digest)
        return path

    def penguins(self):
        return self._data("penguins.csv")

    def taxi(self):
        return self._data("yellow_tripdata_2024-01.parquet")

    def zones(self):
        return self._data("taxi_zone_lookup.csv")

    def catalog(self, *, sources, **kwargs):
        import yaml
        config = self.root / ("catalog-" + secrets.token_hex(8) + ".yml")
        config.write_text(yaml.safe_dump({"sources": sources, **kwargs}, sort_keys=False))
        config.chmod(0o600)
        return config

    def command(self, args, check=True):
        return subprocess.run([str(self.binary), *map(str, args)], check=check,
                              capture_output=True, text=True, timeout=180, cwd=self.root)

    def query(self, config, sql, *, sources, parameters=None, compression="none",
              max_rows=100000, max_bytes=67108864, timeout="60s"):
        destination = self.root / ("result-" + secrets.token_hex(8) + ".arrow")
        args = ["query", "--config", str(config), "--sources", ",".join(sources),
                "--sql", sql, "--parameters", json.dumps(parameters or []),
                "--result-compression", compression, "--max-rows", str(max_rows),
                "--max-bytes", str(max_bytes), "--timeout", timeout,
                "--memory-mb", "512", "--threads", "2", "--temp-mb", "1024",
                "--out", str(destination)]
        start = time.perf_counter()
        completed = self.command(args)
        elapsed = time.perf_counter() - start
        stats = json.loads(completed.stderr.strip().splitlines()[-1])
        return QueryResult(destination, stats, elapsed)

    @contextlib.contextmanager
    def gateway(self, config):
        """Private loopback service for the notebook, never a public listener."""
        with socket.socket() as candidate:
            candidate.bind(("127.0.0.1", 0))
            port = candidate.getsockname()[1]
        token = secrets.token_urlsafe(32)
        url = f"http://127.0.0.1:{port}"
        environment = os.environ.copy()
        environment["KELVO_NOTEBOOK_TOKEN"] = token
        # The token is passed in the environment, never printed or in argv.
        process = subprocess.Popen([str(self.binary), "serve", "--config", str(config),
            "--listen", f"127.0.0.1:{port}", "--token-env", "KELVO_NOTEBOOK_TOKEN",
            "--memory-mb", "512", "--threads", "2", "--timeout", "60s",
            "--max-rows", "100000", "--max-bytes", "67108864", "--concurrency", "2"],
            env=environment, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=self.root)
        self._servers.append(process)
        try:
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("Local Kelvo gateway did not start")
                try:
                    with urllib.request.urlopen(url + "/health", timeout=1) as response:
                        if response.status == 200:
                            break
                except (OSError, urllib.error.URLError):
                    time.sleep(0.05)
            else:
                raise TimeoutError("Local Kelvo health deadline expired")
            yield {"url": url, "token": token}
        finally:
            _stop(process)
            self._servers.remove(process)


def _stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


class _ReadDeadline:
    """Interrupt socket I/O at one absolute deadline, including slow headers.

    A duplicate TCP descriptor survives HTTP response ownership transfers and
    TLS wrapping. Shutting it down interrupts all readers of that connection.
    Synchronous stdlib DNS/address connection does not expose its socket until
    connect returns, so this cannot promise a hard DNS-resolution deadline.
    """
    def __init__(self, deadline):
        self.deadline = deadline
        self._lock = threading.Lock()
        self._sockets = []
        self._closed = False
        self._timer = threading.Timer(max(0, deadline - time.monotonic()), self._expire)
        self._timer.name = "kelvo-notebook-read-deadline"
        self._timer.daemon = True
        self._timer.start()

    def check(self):
        if time.monotonic() >= self.deadline:
            raise TimeoutError("Notebook query deadline expired")

    def watch(self, connection):
        # Do not reuse a numeric fd after its owner closes it. The duplicate has
        # independent lifetime, but shutdown still affects the same TCP stream.
        duplicate = socket.socket(fileno=os.dup(connection.fileno()))
        with self._lock:
            expired = self._closed or time.monotonic() >= self.deadline
            if not expired:
                self._sockets.append(duplicate)
        if expired:
            try:
                duplicate.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            duplicate.close()
            connection.close()
            raise TimeoutError("Notebook query deadline expired")

    def _expire(self):
        with self._lock:
            sockets = tuple(self._sockets)
        for connection in sockets:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._timer.cancel()
        self._timer.join()
        with self._lock:
            sockets, self._sockets = self._sockets, []
        for connection in sockets:
            connection.close()


class _DeadlineConnection:
    """Track socket assignment before HTTPSConnection starts a TLS handshake."""
    def __init__(self, *args, read_deadline, **kwargs):
        self._read_deadline = read_deadline
        self._deadline_socket = None
        super().__init__(*args, **kwargs)

    @property
    def sock(self):
        return self._deadline_socket

    @sock.setter
    def sock(self, value):
        self._deadline_socket = value
        if value is not None:
            self._read_deadline.watch(value)


class _DeadlineHTTPConnection(_DeadlineConnection, http.client.HTTPConnection):
    pass


class _DeadlineHTTPSConnection(_DeadlineConnection, http.client.HTTPSConnection):
    pass


class _DeadlineHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, read_deadline):
        super().__init__()
        self._read_deadline = read_deadline

    def http_open(self, request):
        def connection(host, **kwargs):
            return _DeadlineHTTPConnection(host, read_deadline=self._read_deadline, **kwargs)
        return self.do_open(connection, request)


class _DeadlineHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, read_deadline):
        super().__init__()
        self._read_deadline = read_deadline

    def https_open(self, request):
        def connection(host, **kwargs):
            return _DeadlineHTTPSConnection(host, read_deadline=self._read_deadline, **kwargs)
        return self.do_open(connection, request, context=self._context)


class _DeadlineResponse:
    def __init__(self, response, read_deadline):
        self._response, self._read_deadline = response, read_deadline
        self.headers = response.headers

    def read(self, size=-1):
        self._read_deadline.check()
        try:
            result = self._response.read(size)
        except Exception:
            self._read_deadline.check()
            raise
        self._read_deadline.check()
        return result

    def close(self):
        try:
            self._response.close()
        finally:
            self._read_deadline.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _deadline_open(request, deadline):
    guard = _ReadDeadline(deadline)
    response = None
    try:
        guard.check()
        opener = urllib.request.build_opener(_NoRedirect(), _DeadlineHTTPHandler(guard),
                                             _DeadlineHTTPSHandler(guard))
        response = opener.open(request, timeout=min(deadline - time.monotonic(), 60))
        guard.check()
        return _DeadlineResponse(response, guard)
    except BaseException as error:
        if response is not None:
            response.close()
        elif isinstance(error, urllib.error.HTTPError):
            error.close()
        guard.close()
        guard.check()
        raise


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("Authenticated Kelvo requests do not follow redirects")


def _endpoint(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Gateway URL must not include credentials, query or fragment")
    try:
        loopback = ipaddress.ip_address(parsed.hostname or "").is_loopback
    except ValueError:
        loopback = parsed.hostname == "localhost"
    if not parsed.hostname or (parsed.scheme != "https" and not (parsed.scheme == "http" and loopback)):
        raise ValueError("Gateway requires HTTPS; HTTP is allowed only on loopback")
    return url.rstrip("/")


def remote_query(url, token, request, *, timeout=120, max_bytes=67108864):
    """Consume Arrow, require terminal success, and reject partial/oversize data.

    max_bytes bounds both encoded download and accumulated logical Arrow buffers;
    it is not a hard native allocation/RSS bound during IPC decompression. Connect
    only to a trusted gateway with operator-enforced result limits. An absolute
    watchdog bounds socket I/O, including headers and slow response bodies.
    Synchronous stdlib DNS/address connection cannot be interrupted before a
    socket is available; the timeout is not a hard DNS-resolution bound.
    """
    import pyarrow as pa
    import pyarrow.ipc as ipc
    base = _endpoint(url)
    if not token or "\n" in token or "\r" in token:
        raise ValueError("A valid bearer token is required")
    if not 0 < timeout <= 3600 or not 1024 <= max_bytes <= 256 << 20:
        raise ValueError("Invalid notebook client limits")
    deadline = time.monotonic() + timeout
    query_id = None

    def call(path, body=None):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Notebook query deadline expired")
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(base + path, data=data, headers={
            "Authorization": "Bearer " + token, "Content-Type": "application/json"})
        return _deadline_open(req, deadline)

    def read_json(response):
        with response:
            raw = response.read((1 << 20) + 1)
        if len(raw) > 1 << 20:
            raise ValueError("Gateway metadata exceeded byte budget")
        return json.loads(raw)

    try:
        submitted = read_json(call("/v1/queries", request))
        query_id = submitted.get("id", "")
        if not isinstance(query_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", query_id):
            raise ValueError("Invalid query handle")
        with tempfile.TemporaryFile() as encoded:
            downloaded = 0
            with call(f"/v1/queries/{query_id}/results") as response:
                if response.headers.get_content_type() != "application/vnd.apache.arrow.stream":
                    raise ValueError("Gateway did not return an Arrow stream")
                while block := response.read(256 << 10):
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Notebook query deadline expired")
                    downloaded += len(block)
                    if downloaded > max_bytes:
                        raise ValueError("Encoded result exceeds notebook byte budget")
                    encoded.write(block)
            # Arrow EOF alone does not establish successful execution.
            while True:
                status = read_json(call(f"/v1/queries/{query_id}"))
                state = status.get("state")
                if state == "succeeded":
                    break
                if state in ("failed", "cancelled", "expired"):
                    raise RuntimeError("Kelvo query did not succeed; partial results discarded")
                if state not in ("queued", "running", "streaming", "assigned"):
                    raise ValueError("Unknown gateway query state")
                time.sleep(min(0.05, max(0, deadline - time.monotonic())))
            if downloaded < 8:
                raise ValueError("Incomplete Arrow stream")
            encoded.seek(-8, 2)
            if encoded.read(8) != b"\xff\xff\xff\xff\x00\x00\x00\x00":
                raise ValueError("Missing complete Arrow end marker")
            encoded.seek(0)
            batches, decoded = [], 0
            with ipc.open_stream(encoded) as reader:
                schema = reader.schema
                for batch in reader:
                    decoded += batch.nbytes
                    if decoded > max_bytes:
                        raise ValueError("Decoded result exceeds notebook byte budget")
                    batches.append(batch)
            if encoded.tell() != downloaded:
                raise ValueError("Unexpected bytes after Arrow stream")
            table = pa.Table.from_batches(batches, schema=schema)
            if status.get("stats", {}).get("rows") != table.num_rows:
                raise ValueError("Terminal row count does not match the complete result")
            return table
    except BaseException:
        # Best effort only, with its own short timeout after the main deadline.
        if query_id and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", query_id):
            try:
                req = urllib.request.Request(base + f"/v1/queries/{query_id}/cancel", data=b"{}",
                    headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
                with _deadline_open(req, time.monotonic() + 3):
                    pass
            except Exception:
                pass
        raise

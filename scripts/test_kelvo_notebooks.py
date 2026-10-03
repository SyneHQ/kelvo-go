"""Client safety checks: HTTP success alone must never admit partial analysis."""
import contextlib
import hashlib
import http.server
import importlib.util
import io
import json
from pathlib import Path
import sys
import tarfile
import time
import tempfile
import threading
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("kelvo_notebooks", ROOT / "notebooks/kelvo_notebooks.py")
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)


@contextlib.contextmanager
def fixture(*, state="succeeded", rows=2, truncate=False, extra=False, redirect=False, large=False):
    import pyarrow as pa
    import pyarrow.ipc as ipc
    sink = io.BytesIO()
    table = pa.table({"id": [1, 2], "value": [None, "x" * (4096 if large else 1)]})
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    data = sink.getvalue()
    if truncate:
        data = data[:-8]
    if extra:
        data += sink.getvalue()
    observed = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            observed.append((self.path, self.headers.get("Authorization")))
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            if redirect:
                self.send_response(307)
                self.send_header("Location", "https://example.com/credential-trap")
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"id": "test-id", "state": "queued"}).encode())

        def do_GET(self):
            observed.append((self.path, self.headers.get("Authorization")))
            self.send_response(200)
            if self.path.endswith("/results"):
                self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            else:
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"state": state, "stats": {"rows": rows}}).encode())

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", observed, table
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@contextlib.contextmanager
def slow_fixture(phase):
    """Bytes keep arriving within socket inactivity timeout, beyond the deadline."""
    stop = threading.Event()
    observed = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def drip(self, payload):
            try:
                for byte in payload:
                    if stop.is_set():
                        break
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    if stop.wait(0.02):
                        break
            except (BrokenPipeError, ConnectionResetError):
                pass

        def metadata(self):
            payload = json.dumps({"id": "slow-id", "padding": "x" * 512}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if phase == "metadata" and not self.path.endswith("/cancel"):
                self.drip(payload)
            else:
                self.wfile.write(payload)

        def do_POST(self):
            observed.append(self.path)
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            if phase == "headers" and not self.path.endswith("/cancel"):
                self.close_connection = True
                self.drip(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                          b"Content-Length: 16\r\nConnection: close\r\n\r\n" b'{"id":"slow-id"}')
            else:
                self.metadata()

        def do_GET(self):
            observed.append(self.path)
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
            self.send_header("Content-Length", "4096")
            self.end_headers()
            self.drip(b"x" * 4096)

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = False
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", observed
    finally:
        stop.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class NotebookClientTests(unittest.TestCase):
    def test_preserves_values_types_and_nulls_after_terminal_success(self):
        with fixture() as (url, observed, expected):
            actual = lab.remote_query(url, "test-token", {"sql": "fixture"})
            self.assertTrue(actual.equals(expected, check_metadata=True))
            self.assertEqual(observed[-1][0], "/v1/queries/test-id")

    def test_rejects_terminal_failure_after_valid_arrow(self):
        with fixture(state="failed") as (url, observed, _):
            with self.assertRaisesRegex(RuntimeError, "did not succeed"):
                lab.remote_query(url, "test-token", {"sql": "fixture"})
            self.assertTrue(observed[-1][0].endswith("/cancel"))

    def test_rejects_truncation_extra_stream_and_wrong_row_count(self):
        for kwargs in ({"truncate": True}, {"extra": True}, {"rows": 3}):
            with self.subTest(kwargs=kwargs), fixture(**kwargs) as (url, _, _):
                with self.assertRaises(ValueError):
                    lab.remote_query(url, "test-token", {"sql": "fixture"})

    def test_bounded_response_cancels(self):
        with fixture(large=True) as (url, observed, _):
            with self.assertRaisesRegex(ValueError, "byte budget"):
                lab.remote_query(url, "test-token", {"sql": "fixture"}, max_bytes=1024)
            self.assertTrue(observed[-1][0].endswith("/cancel"))

    def test_deadline_is_terminal(self):
        with fixture(state="streaming") as (url, observed, _):
            with self.assertRaises(TimeoutError):
                lab.remote_query(url, "test-token", {"sql": "fixture"}, timeout=0.1)
            self.assertTrue(observed[-1][0].endswith("/cancel"))

    def test_no_authenticated_redirects(self):
        with fixture(redirect=True) as (url, observed, _):
            with self.assertRaisesRegex(ValueError, "redirect"):
                lab.remote_query(url, "secret-token", {"sql": "fixture"})
            self.assertEqual(len(observed), 1)

    def test_absolute_deadline_interrupts_slow_headers_metadata_and_results(self):
        for phase in ("headers", "metadata", "results"):
            with self.subTest(phase=phase), slow_fixture(phase) as (url, observed):
                started = time.monotonic()
                with self.assertRaises(TimeoutError):
                    lab.remote_query(url, "test-token", {"sql": "fixture"}, timeout=0.2)
                elapsed = time.monotonic() - started
                self.assertLess(elapsed, 1.0, "slow trickle escaped the absolute read deadline")
                self.assertGreaterEqual(elapsed, 0.1)
                if phase == "results":
                    self.assertTrue(observed[-1].endswith("/cancel"))
                self.assertFalse(any(thread.name == "kelvo-notebook-read-deadline"
                                     for thread in threading.enumerate()))

    def test_deadline_sockets_and_timers_are_reaped(self):
        descriptors = Path("/proc/self/fd")
        with fixture() as (url, _, _):
            before = len(list(descriptors.iterdir())) if descriptors.is_dir() else None
            for _ in range(3):
                lab.remote_query(url, "test-token", {"sql": "fixture"})
            self.assertFalse(any(thread.name == "kelvo-notebook-read-deadline"
                                 for thread in threading.enumerate()))
            if before is not None:
                self.assertLessEqual(len(list(descriptors.iterdir())), before)

    def test_release_manifest_must_match_immutable_code_pin(self):
        def replaced_manifest(url, destination, limit, expected=None):
            destination.write_text("0" * 64 + "  kelvo-linux-amd64.tar.gz\n")
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.dict(lab.os.environ, {}, clear=True), \
             mock.patch.object(lab.platform, "system", return_value="Linux"), \
             mock.patch.object(lab.platform, "machine", return_value="x86_64"), \
             mock.patch.object(lab, "_download", side_effect=replaced_manifest) as download:
            with self.assertRaisesRegex(ValueError, "pinned archive"):
                lab.install_binary(Path(directory))
            self.assertEqual(download.call_count, 1)
            self.assertFalse((Path(directory) / "kelvo").exists())

    def test_release_extraction_rejects_executable_symlink(self):
        def download(url, destination, limit, expected=None):
            if destination.name == "SHA256SUMS":
                destination.write_text(lab.RELEASE_SHA256 + "  kelvo-linux-amd64.tar.gz\n")
            else:
                self.assertEqual(expected, lab.RELEASE_SHA256)
                with tarfile.open(destination, "w:gz") as archive:
                    member = tarfile.TarInfo("kelvo")
                    member.type = tarfile.SYMTYPE
                    member.linkname = "/untrusted/executable"
                    archive.addfile(member)
        with tempfile.TemporaryDirectory() as directory, \
             mock.patch.dict(lab.os.environ, {}, clear=True), \
             mock.patch.object(lab.platform, "system", return_value="Linux"), \
             mock.patch.object(lab.platform, "machine", return_value="x86_64"), \
             mock.patch.object(lab, "_download", side_effect=download):
            with self.assertRaisesRegex(ValueError, "Unexpected executable"):
                lab.install_binary(Path(directory))
            self.assertFalse((Path(directory) / "kelvo").exists())

    def test_secure_endpoint(self):
        for url in ("http://example.com", "https://user:pass@example.com", "https://example.com?token=secret", "https://example.com#secret"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                lab._endpoint(url)
        self.assertEqual(lab._endpoint("https://example.com/prefix/"), "https://example.com/prefix")

    def test_download_hash_failure_does_not_publish(self):
        class Response(io.BytesIO):
            url = "https://example.com/public-data"
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "data"
            destination.write_bytes(b"keep")
            with mock.patch.object(lab.urllib.request, "urlopen", return_value=Response(b"tampered")):
                with self.assertRaisesRegex(ValueError, "checksum"):
                    lab._download("https://example.com/public-data", destination, 100, hashlib.sha256(b"good").hexdigest())
            self.assertEqual(destination.read_bytes(), b"keep")
            self.assertEqual([x.name for x in Path(root).iterdir()], ["data"])


if __name__ == "__main__":
    unittest.main()

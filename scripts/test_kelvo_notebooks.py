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
def fixture(*, state="succeeded", rows=2, truncate=False, extra=False, redirect=False, large=False,
            receipt=None, status_code=200, chunked=False, incomplete_http=False,
            invalid_arrow=False, compressed=False, chunk_fault=None,
            metadata_framing=None, metadata_target="status"):
    import pyarrow as pa
    import pyarrow.ipc as ipc
    sink = io.BytesIO()
    table = pa.table({"id": [1, 2], "value": [None, "x" * (4096 if large else 1)]})
    options = ipc.IpcWriteOptions(compression="lz4") if compressed else None
    with ipc.new_stream(sink, table.schema, options=options) as writer:
        writer.write_table(table)
    data = sink.getvalue()
    if truncate:
        data = data[:-8]
    if extra:
        data += sink.getvalue()
    if invalid_arrow:
        data = b"\x00" * 8 + b"\xff\xff\xff\xff\x00\x00\x00\x00"
    observed = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def write_json(self, payload, code=200):
            data = json.dumps(payload).encode()
            selected = ((metadata_target == "submit" and self.path == "/v1/queries")
                        or (metadata_target == "status" and self.path == "/v1/queries/test-id"))
            framing = metadata_framing if selected else None
            chunk_json = framing in ("missing-zero", "missing-final-crlf", "ambiguous")
            if chunk_json:
                self.protocol_version = "HTTP/1.1"
                self.close_connection = True
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            if framing == "short-length":
                self.send_header("Content-Length", str(len(data) + 8))
            if chunk_json:
                self.send_header("Transfer-Encoding", "chunked")
                if framing == "ambiguous":
                    self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            if chunk_json:
                data = f"{len(data):x}\r\n".encode() + data + b"\r\n"
                if framing == "missing-final-crlf":
                    data += b"0\r\n"
                elif framing != "missing-zero":
                    data += b"0\r\n\r\n"
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_POST(self):
            observed.append((self.path, self.headers.get("Authorization")))
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            if redirect:
                self.send_response(307)
                self.send_header("Location", "https://example.com/credential-trap")
                self.end_headers()
                return
            self.write_json({"id": "test-id", "state": "queued"})

        def do_GET(self):
            observed.append((self.path, self.headers.get("Authorization")))
            if self.path.endswith("/results"):
                if chunked:
                    self.protocol_version = "HTTP/1.1"
                    self.close_connection = True
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.apache.arrow.stream")
                if receipt is not None:
                    for value in receipt if isinstance(receipt, list) else [receipt]:
                        self.send_header("Kelvo-Result-Completion", value)
                if chunked:
                    self.send_header("Transfer-Encoding", "chunked")
                else:
                    self.send_header("Content-Length", str(len(data) + (8 if incomplete_http else 0)))
                self.end_headers()
                if chunked:
                    size_ending = b"\n" if chunk_fault == "lf-size" else b"\r\n"
                    extension = b";checked=1" if chunk_fault == "valid-metadata" else b""
                    separator = b"XX" if chunk_fault == "bad-separator" else b"\r\n"
                    self.wfile.write(f"{len(data):x}".encode() + extension + size_ending + data + separator)
                    if not incomplete_http:
                        ending = b"0\r\n\r\n"
                        if chunk_fault == "missing-final-crlf":
                            ending = b"0\r\n"
                        elif chunk_fault == "trailer-without-blank":
                            ending = b"0\r\nX-Trace: fixture\r\n"
                        elif chunk_fault == "valid-metadata":
                            ending = b"0\r\nX-Trace: fixture\r\n\r\n"
                        try:
                            self.wfile.write(ending)
                        except (BrokenPipeError, ConnectionResetError):
                            pass
                else:
                    self.wfile.write(data)
            else:
                self.write_json({"state": state, "stats": {"rows": rows}}, status_code)

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

    def test_verified_durable_receipt_accepts_reclaimed_handle(self):
        for options in ({}, {"chunked": True}, {"chunked": True, "chunk_fault": "valid-metadata"}):
            with self.subTest(options=options), fixture(receipt="durable-eos-v1", status_code=404,
                                                       **options) as (url, observed, expected):
                actual = lab.remote_query(url, "test-token", {"sql": "fixture"})
                self.assertTrue(actual.equals(expected, check_metadata=True))
                self.assertEqual(observed[-1][0], "/v1/queries/test-id")
                self.assertFalse(any(thread.name == "kelvo-notebook-read-deadline"
                                     for thread in threading.enumerate()))

    def test_missing_unknown_or_duplicate_receipt_does_not_allow_404(self):
        for receipt in (None, "future-v2", "DURABLE-EOS-V1"):
            with self.subTest(receipt=receipt), fixture(receipt=receipt, status_code=404) as (url, observed, _):
                with self.assertRaises(lab.urllib.error.HTTPError) as failure:
                    lab.remote_query(url, "test-token", {"sql": "fixture"})
                self.assertEqual(failure.exception.code, 404)
                self.assertTrue(observed[-1][0].endswith("/cancel"))

        with fixture(receipt=["durable-eos-v1", "durable-eos-v1"]) as (url, _, _):
            with self.assertRaisesRegex(ValueError, "Ambiguous HTTP completion"):
                lab.remote_query(url, "test-token", {"sql": "fixture"})

    def test_available_terminal_status_overrules_receipt(self):
        for state in ("failed", "cancelled", "expired"):
            with self.subTest(state=state), fixture(receipt="durable-eos-v1", state=state) as (url, observed, _):
                with self.assertRaisesRegex(RuntimeError, "did not succeed"):
                    lab.remote_query(url, "test-token", {"sql": "fixture"})
                self.assertTrue(observed[-1][0].endswith("/cancel"))
        with fixture(receipt="durable-eos-v1", rows=3) as (url, _, _):
            with self.assertRaisesRegex(ValueError, "row count"):
                lab.remote_query(url, "test-token", {"sql": "fixture"})

    def test_receipt_never_upgrades_incomplete_or_corrupt_arrow(self):
        for options in ({"truncate": True}, {"extra": True}, {"invalid_arrow": True}):
            with self.subTest(options=options), fixture(receipt="durable-eos-v1", status_code=404,
                                                       **options) as (url, observed, _):
                with self.assertRaises(ValueError):
                    lab.remote_query(url, "test-token", {"sql": "fixture"})
                self.assertNotIn("/v1/queries/test-id", [path for path, _ in observed])
                self.assertTrue(observed[-1][0].endswith("/cancel"))

    def test_receipt_requires_complete_http_framing_after_valid_arrow_eos(self):
        for chunked in (False, True):
            with self.subTest(chunked=chunked), fixture(receipt="durable-eos-v1", status_code=404,
                                                       chunked=chunked, incomplete_http=True) as (url, observed, _):
                with self.assertRaises((ValueError, lab.http.client.IncompleteRead)):
                    lab.remote_query(url, "test-token", {"sql": "fixture"})
                self.assertNotIn("/v1/queries/test-id", [path for path, _ in observed])
                self.assertTrue(observed[-1][0].endswith("/cancel"))

    def test_chunk_sizes_separators_and_final_trailers_are_strict(self):
        for fault in ("lf-size", "bad-separator", "missing-final-crlf", "trailer-without-blank"):
            with self.subTest(fault=fault), fixture(receipt="durable-eos-v1", status_code=404,
                                                   chunked=True, chunk_fault=fault) as (url, observed, _):
                with self.assertRaises((ValueError, lab.http.client.IncompleteRead)):
                    lab.remote_query(url, "test-token", {"sql": "fixture"})
                self.assertNotIn("/v1/queries/test-id", [path for path, _ in observed])

    def test_submission_and_status_json_require_complete_http_framing(self):
        for target in ("submit", "status"):
            for framing in ("short-length", "missing-zero", "missing-final-crlf", "ambiguous"):
                with self.subTest(target=target, framing=framing), fixture(
                    receipt="durable-eos-v1", metadata_target=target,
                    metadata_framing=framing,
                ) as (url, observed, _):
                    with self.assertRaises((ValueError, lab.http.client.IncompleteRead)):
                        lab.remote_query(url, "test-token", {"sql": "fixture"})
                    if target == "submit":
                        self.assertNotIn("/v1/queries/test-id/results", [path for path, _ in observed])

    def test_receipt_preserves_encoded_and_decoded_result_budgets(self):
        for compressed, expected in ((False, "Encoded"), (True, "Decoded")):
            with self.subTest(compressed=compressed), fixture(receipt="durable-eos-v1", status_code=404,
                                                             large=True, compressed=compressed) as (url, observed, _):
                with self.assertRaisesRegex(ValueError, expected + " result exceeds"):
                    lab.remote_query(url, "test-token", {"sql": "fixture"}, max_bytes=2048)
                self.assertNotIn("/v1/queries/test-id", [path for path, _ in observed])
                self.assertTrue(observed[-1][0].endswith("/cancel"))

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

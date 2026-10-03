#!/usr/bin/env python3
"""Negative controls for operational evidence, configuration and resource samples."""
import argparse
import copy
from contextlib import contextmanager
import http.client
import io
from pathlib import Path
import tempfile
import socketserver
import threading
import urllib.request
import unittest
from unittest import mock

import operational_acceptance as fixture


def valid_report():
    report = {"requested_duration_seconds": 30, "interrupted": False,
              "checks": [{"test": name, "passed": True} for name in fixture.REQUIRED]}
    for check in report["checks"]:
        if check["test"] == "mixed_query_refresh":
            check.update(observed_seconds=31, client_query_counts=[2, 3, 4, 5], completed_queries=14,
                         query_refresh_overlap_samples=2, successful_refresh_deltas={"a1": 2, "b1": 3},
                         completion_protocol="durable-eos-v1", reclaimed_status_after_certified_delivery=0)
        elif check["test"] == "cleanup":
            check.update(forced_application_kills=0, observed_live_descendants=0, worker_scratch_directories=0,
                         broker_processes_untouched=True, original_configurations_untouched=True)
    return report


class EvidenceTests(unittest.TestCase):
    def test_all_required_checks_and_cleanup_must_pass(self):
        report = valid_report()
        self.assertTrue(fixture.reconcile(report))
        for index in range(len(report["checks"])):
            failed = copy.deepcopy(report)
            failed["checks"][index]["passed"] = False
            self.assertFalse(fixture.reconcile(failed))
            missing = copy.deepcopy(report)
            missing["checks"].pop(index)
            self.assertFalse(fixture.reconcile(missing))
        duplicate = copy.deepcopy(report)
        duplicate["checks"].append(duplicate["checks"][0])
        self.assertFalse(fixture.reconcile(duplicate))

    def test_interruption_flag_cannot_pass_even_after_all_checks(self):
        for value in (True, None, 0, "false"):
            report = valid_report()
            report["interrupted"] = value
            self.assertFalse(fixture.reconcile(report))
        report = valid_report()
        del report["interrupted"]
        self.assertFalse(fixture.reconcile(report))

    def test_sampler_follows_nonleader_threads_and_deduplicates_descendants(self):
        with tempfile.TemporaryDirectory() as path:
            proc_root = Path(path)
            for pid, kib in ((10, 1), (20, 2), (30, 3)):
                base = proc_root / str(pid)
                (base / "task" / str(pid)).mkdir(parents=True)
                (base / "task" / str(pid) / "children").write_text("30" if pid == 20 else "")
                fields = ["S", "1"] + ["0"]*17 + [str(pid)]
                (base / "stat").write_text(str(pid)+" (fixture) "+" ".join(fields))
                (base / "status").write_text("VmRSS: "+str(kib)+" kB\n")
                (base / "cgroup").write_text("")
            for tid in (11, 12):
                thread = proc_root / "10" / "task" / str(tid)
                thread.mkdir()
                (thread / "children").write_text("20 20")
            # A thread which exited after enumeration is safely ignored.
            (proc_root / "10" / "task" / "13").mkdir()
            process = mock.Mock(pid=10)
            process.poll.return_value = None
            samples = fixture.Samples({"root": process}, proc_root=proc_root)
            samples.sample()
            self.assertEqual(set(samples.owned), {10, 20, 30})
            self.assertEqual(samples.peak_rss, 6*1024)
            self.assertTrue(samples.rss_available)
            self.assertEqual(samples.errors, set())

    def test_elapsed_time_alone_cannot_pass_mixed_acceptance(self):
        for key, value in (("observed_seconds", 29), ("client_query_counts", [0, 3, 4, 7]),
                           ("completed_queries", 99), ("query_refresh_overlap_samples", 0),
                           ("completion_protocol", "unknown"), ("reclaimed_status_after_certified_delivery", 99),
                           ("successful_refresh_deltas", {"a1": 0, "b1": 1})):
            report = valid_report()
            check = next(item for item in report["checks"] if item["test"] == "mixed_query_refresh")
            check[key] = value
            self.assertFalse(fixture.reconcile(report), key)
        for key in ("forced_application_kills", "observed_live_descendants", "worker_scratch_directories"):
            report = valid_report()
            next(item for item in report["checks"] if item["test"] == "cleanup")[key] = 1
            self.assertFalse(fixture.reconcile(report), key)

    def test_duration_is_bounded_and_finite(self):
        for value in ("30", "7200", "14400"):
            self.assertEqual(fixture.duration_value(value), float(value))
        for value in ("0", "29", "14401", "nan", "inf", "-inf"):
            with self.assertRaises(argparse.ArgumentTypeError):
                fixture.duration_value(value)

    def test_catalog_yaml_supports_scalar_source_lists_and_dataset_mappings(self):
        catalog = {"sources": [{"id": "ops_raw", "type": "parquet", "path": "/tmp/a.parquet"}],
                   "acceleration": {"tenant_id": "a", "datasets": [{"id": "ops_snapshot", "query": {
                       "mode": "federated", "sources": ["ops_raw"], "sql": "SELECT * FROM ops_raw"}}]}}
        text = fixture.yaml_document(catalog)
        self.assertIn('sources:\n          - "ops_raw"\n', text)
        self.assertIn('datasets:\n    -\n      id: "ops_snapshot"\n', text)
        self.assertIn('path: "/tmp/a.parquet"\n', text)
        with self.assertRaises(fixture.AcceptanceError):
            fixture.yaml_document({"bad:key": 1})

    def test_yaml_edit_preserves_policy_and_adjacent_fields(self):
        original = 'policy:\n  limits:\n    memory_mb: 256\nresources:\n  memory_mb: 100\n  max_concurrent: 1\nworker_id: "a1"\n'
        changed = fixture.replace_field(original, "resources", {"memory_mb": 1024, "max_concurrent": 2})
        self.assertIn('policy:\n  limits:\n    memory_mb: 256\n', changed)
        self.assertIn('worker_id: "a1"\n', changed)
        self.assertNotIn('memory_mb: 100\n', changed)
        for bad in ("worker_id: a1\n", original+"resources: {}\n"):
            with self.assertRaises(fixture.AcceptanceError):
                fixture.replace_field(bad, "resources", {})

    def test_mixed_failure_keeps_original_private_traceback_and_safe_category(self):
        acceptance = fixture.Acceptance.__new__(fixture.Acceptance)
        acceptance.args = argparse.Namespace(duration=30)
        acceptance.reclaimed_statuses = 0
        acceptance.refresh_count = lambda name: 1
        acceptance.resources = lambda name: {"Active": 0, "BackgroundActive": 0}
        with tempfile.TemporaryDirectory() as path:
            acceptance.directory = Path(path)
            with mock.patch.object(acceptance, "query", side_effect=fixture.AcceptanceError("QUERY_STATUS_FAILED")):
                with self.assertRaisesRegex(fixture.AcceptanceError, "^MIXED_QUERY_STATUS_FAILED$"):
                    acceptance.mixed()
            traces = list(acceptance.directory.glob("mixed-client-*-failure.log"))
            self.assertTrue(traces)
            self.assertIn("QUERY_STATUS_FAILED", traces[0].read_text())
            self.assertIn("Traceback", traces[0].read_text())

    def test_sampler_failure_does_not_skip_application_cleanup(self):
        acceptance = fixture.Acceptance.__new__(fixture.Acceptance)
        process = mock.Mock()
        process.poll.return_value = None
        process.wait.return_value = 0
        acceptance.processes = {"node": process}
        acceptance.samples = mock.Mock()
        acceptance.samples.finish.side_effect = RuntimeError("fixture")
        acceptance.samples.owned = {}
        acceptance.wait = lambda callback, timeout=3: callback()
        with tempfile.TemporaryDirectory() as path:
            acceptance.directory = Path(path)
            with self.assertRaises(fixture.AcceptanceError):
                acceptance.cleanup()
        process.send_signal.assert_called_once_with(fixture.signal.SIGTERM)
        process.wait.assert_called_once_with(timeout=10)

    def test_cleanup_never_signals_reused_pid(self):
        with mock.patch.object(fixture, "proc_identity", return_value=(42, "new")), mock.patch.object(fixture.os, "pidfd_open") as opened:
            fixture.signal_owned((42, "old"), 9)
            opened.assert_not_called()
        with mock.patch.object(fixture, "proc_identity", side_effect=[(42, "old"), (42, "new")]), mock.patch.object(fixture.os, "pidfd_open", return_value=5), mock.patch.object(fixture.signal, "pidfd_send_signal") as sent, mock.patch.object(fixture.os, "close") as closed:
            fixture.signal_owned((42, "old"), 9)
            sent.assert_not_called()
            closed.assert_called_once_with(5)

    def test_rss_and_cgroup_measurements_have_distinct_units(self):
        self.assertEqual(fixture.rss_bytes("Name: kelvo\nVmRSS:\t123 kB\n"), 125952)
        self.assertIsNone(fixture.rss_bytes("State: Z\n"))
        self.assertEqual(fixture.numeric_counters("low 0\nmax 3\noom 0\n"), {"low": 0, "max": 3, "oom": 0})
        with self.assertRaises(fixture.AcceptanceError):
            fixture.numeric_counters("oom -1\n")
        sample = fixture.Samples({}).evidence()
        self.assertFalse(sample["cgroup_v2_memory_available"])
        self.assertIsNone(sample["sampled_process_tree_rss_peak_bytes"])
        self.assertIn("CGROUP_V2_MEMORY_NOT_OBSERVED", sample["missed_counter_categories"])

    def test_pid_identity_rejects_reused_start_time_and_zombie(self):
        with tempfile.TemporaryDirectory() as path:
            root = Path(path)
            (root/"42").mkdir()
            def stat(state, start):
                fields = [state, "1"] + ["0"]*17 + [str(start)]
                (root/"42"/"stat").write_text("42 (name with ) parens) "+" ".join(fields))
            stat("S", 100)
            original = fixture.proc_identity(42, root)
            self.assertEqual(original, (42, "100"))
            stat("S", 101)
            self.assertNotEqual(fixture.proc_identity(42, root), original)
            stat("Z", 101)
            self.assertIsNone(fixture.proc_identity(42, root))



class WaiterScheduleControls(unittest.TestCase):
    def run_schedule(self, report_parked):
        acceptance = fixture.Acceptance.__new__(fixture.Acceptance)
        ids = ["job"+str(index) for index in range(8)]
        states = {query_id: "queued" for query_id in ids}
        states[ids[-1]] = "assigned"  # Deliberately not the first submitted job.
        releases = {query_id: threading.Event() for query_id in ids[:2]}
        pending = set()
        attempts = {query_id: 0 for query_id in ids[:2]}
        lock = threading.Lock()
        active_rejection = (429, b'{"error":{"code":"RESOURCE_EXHAUSTED","message":"Request capacity unavailable"}}')
        waiter_rejection = (429, b'{"error":{"code":"RESOURCE_EXHAUSTED","message":"Queued result wait capacity unavailable"}}')
        def call(path, *args, **kwargs):
            if path == "/health":
                return 200, b"{}"
            query_id = path.split("/")[3]
            if path.endswith("/cancel"):
                states[query_id] = "cancelled"
                releases[query_id].set()
                return 200, b"{}"
            self.assertTrue(path.endswith("/results"))
            self.assertNotEqual(query_id, ids[-1], "assigned blocker must never be consumed")
            if query_id in releases:
                with lock:
                    attempts[query_id] += 1
                    if query_id == ids[0] and attempts[query_id] == 1:
                        return active_rejection  # Controlled original race.
                    pending.add(query_id)
                releases[query_id].wait(2)
                return 409, b"{}"
            self.assertEqual(query_id, ids[2])
            with lock:
                both_parked = len(pending) == 2
            return waiter_rejection if report_parked and both_parked else active_rejection
        acceptance.call = call
        acceptance.retry = call
        acceptance.state = lambda query_id: states[query_id]
        if not report_parked:
            def reject_false(predicate, timeout=3):
                value = predicate()
                if value:
                    return value
                for released in releases.values():
                    released.set()
                raise fixture.AcceptanceError("CONDITION_DEADLINE")
            acceptance.wait = reject_false
        with tempfile.TemporaryDirectory() as path:
            acceptance.directory = Path(path)
            if report_parked:
                result = acceptance.queued_waiter_saturation(ids)
                self.assertEqual(result["waiter_rejection_source"], "parked_waiter_pool")
                self.assertEqual(result["queued_handles_confirmed"], 3)
                self.assertEqual(attempts[ids[0]], 2)
                self.assertEqual(states[ids[-1]], "assigned")
            else:
                with self.assertRaisesRegex(fixture.AcceptanceError, "CONDITION_DEADLINE"):
                    acceptance.queued_waiter_saturation(ids)
            self.assertTrue((acceptance.directory/"saturation-waiters.json").exists())

    def test_retries_transient_active_admission_and_uses_observed_queue_order(self):
        self.run_schedule(True)

    def test_generic_active_admission_rejection_cannot_prove_parked_waiter_capacity(self):
        self.run_schedule(False)


class FixtureHTTPConnection(http.client.HTTPConnection):
    response_class = fixture.StrictHTTPResponse


class FixtureHTTPHandler(urllib.request.HTTPHandler):
    def http_open(self, request):
        return self.do_open(FixtureHTTPConnection, request)


@contextmanager
def http_fixture(wire):
    """A real loopback socket sends exact framing, then closes its write side."""
    class Handler(socketserver.BaseRequestHandler):
        def handle(self):
            self.request.settimeout(3)
            request = b""
            while not request.endswith(b"\r\n\r\n"):
                part = self.request.recv(4096)
                if not part or len(request) > 65536:
                    return
                request += part
            self.request.sendall(wire)

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
        thread.start()
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), FixtureHTTPHandler())

        class LoopbackOpener:
            def open(self, request, timeout):
                # Exercise Acceptance.call including HTTPError, but use this
                # real local HTTP server instead of the production TLS port.
                local = urllib.request.Request("http://127.0.0.1:"+str(server.server_address[1])+"/result",
                                               data=request.data, headers=dict(request.header_items()),
                                               method=request.get_method())
                return opener.open(local, timeout=timeout)

        acceptance = fixture.Acceptance.__new__(fixture.Acceptance)
        acceptance.client_opener = LoopbackOpener()
        acceptance.env = {"KELVO_TOKEN_A": "fixture-only"}
        try:
            yield acceptance
        finally:
            server.shutdown()
            thread.join(timeout=3)
            if thread.is_alive():
                raise RuntimeError("HTTP fixture did not stop")


class HTTPFramingControls(unittest.TestCase):
    @staticmethod
    def response(body, headers, status=200):
        return (f"HTTP/1.1 {status} Fixture\r\nConnection: close\r\n".encode()
                + b"".join(name.encode()+b": "+value.encode()+b"\r\n" for name, value in headers)
                + b"\r\n" + body)

    @staticmethod
    def arrow():
        import pyarrow as pa
        table = pa.Table.from_pylist([{"value": 42}])
        output = io.BytesIO()
        with pa.ipc.new_stream(output, table.schema) as writer:
            writer.write_table(table)
        return output.getvalue()

    def test_complete_fixed_and_chunked_real_http_certifies_arrow(self):
        body = self.arrow()
        capability = [("Kelvo-Result-Completion", "durable-eos-v1")]
        framed = (f"{len(body):x}\r\n".encode()+body+b"\r\n0\r\n\r\n")
        for wire in (self.response(body, capability+[("Content-Length", str(len(body)))]),
                     self.response(framed, capability+[("Transfer-Encoding", "chunked")])):
            with http_fixture(wire) as acceptance:
                status, raw, headers = acceptance.call("/result", with_headers=True)
            self.assertEqual(status, 200)
            self.assertEqual(fixture.verify_completed_arrow(raw, headers, [{"value": 42}]), 1)

    def test_arrow_eos_inside_short_content_length_never_certifies(self):
        body = self.arrow()
        headers = [("Kelvo-Result-Completion", "durable-eos-v1"), ("Content-Length", str(len(body)+1))]
        # Exercise both success and HTTPError's separate response path.
        for status in (200, 503):
            with self.subTest(status=status), http_fixture(self.response(body, headers, status)) as acceptance:
                with self.assertRaisesRegex(fixture.AcceptanceError, "INCOMPLETE_HTTP_CONTENT_LENGTH"):
                    acceptance.call("/result", with_headers=True)

    def test_duplicate_or_ambiguous_protocol_headers_are_rejected(self):
        body = self.arrow()
        capability = ("Kelvo-Result-Completion", "durable-eos-v1")
        length = ("Content-Length", str(len(body)))
        cases = ([capability, length, ("kelvo-result-completion", "durable-eos-v1")],
                 [capability, length, ("Kelvo-Result-Completion", "unknown")],
                 [capability, length, length],
                 [capability, length, ("Transfer-Encoding", "chunked")],
                 [capability, ("Transfer-Encoding", "chunked"), ("transfer-encoding", "chunked")],
                 [capability, ("Transfer-Encoding", "gzip, chunked")],
                 [capability, ("Content-Length", str(len(body))+", "+str(len(body)))],
                 [capability])
        for headers in cases:
            with self.subTest(headers=headers), http_fixture(self.response(body, headers)) as acceptance:
                with self.assertRaises(fixture.AcceptanceError):
                    acceptance.call("/result", with_headers=True)

    def test_arrow_eos_inside_incomplete_chunked_framing_never_certifies(self):
        body = self.arrow()
        headers = [("Kelvo-Result-Completion", "durable-eos-v1"), ("Transfer-Encoding", "chunked")]
        prefix = f"{len(body):x}\r\n".encode()+body
        cases = (prefix+b"\r\n", prefix+b"\r\n0\r\n", prefix+b"xx0\r\n\r\n",
                 f"{len(body):x}\n".encode()+body+b"\r\n0\r\n\r\n", prefix+b"\r\n0\r\n\n",
                 prefix+b"\r\n0\r\nX-Partial: value")
        for framed in cases:
            with self.subTest(framing=framed[-24:]), http_fixture(self.response(framed, headers)) as acceptance:
                with self.assertRaises((fixture.AcceptanceError, http.client.IncompleteRead)):
                    acceptance.call("/result", with_headers=True)

    def test_body_bound_is_preserved_for_fixed_and_chunked_responses(self):
        body = b"x"*(fixture.MAX_BODY+1)
        chunk = f"{len(body):x}\r\n".encode()+body+b"\r\n0\r\n\r\n"
        for raw, headers in ((body, [("Content-Length", str(len(body)))]),
                             (chunk, [("Transfer-Encoding", "chunked")])):
            with http_fixture(self.response(raw, headers)) as acceptance:
                with self.assertRaisesRegex(fixture.AcceptanceError, "HTTP_RESPONSE_BOUND"):
                    acceptance.call("/result", with_headers=True)


class ArrowControls(unittest.TestCase):
    def test_reclaimed_status_requires_certified_arrow_delivery_first(self):
        import pyarrow as pa
        import threading
        expected = [{"value": 42}]
        table = pa.Table.from_pylist(expected)
        output = io.BytesIO()
        with pa.ipc.new_stream(output, table.schema) as writer:
            writer.write_table(table)
        good = output.getvalue()
        for headers, body, accepted in (({"kelvo-result-completion": "durable-eos-v1"}, good, True),
                                        ({}, good, False),
                                        ({"kelvo-result-completion": "durable-eos-v1"}, good[:-8], False)):
            acceptance = fixture.Acceptance.__new__(fixture.Acceptance)
            acceptance.expected = {"a": expected}
            acceptance.observation_lock = threading.Lock()
            acceptance.reclaimed_statuses = 0
            acceptance.submit = lambda tenant: "query-handle"
            acceptance.retry = mock.Mock(side_effect=[(200, body, headers), (404, b"")])
            if accepted:
                self.assertEqual(acceptance.query("a"), 1)
                self.assertEqual(acceptance.reclaimed_statuses, 1)
            else:
                with self.assertRaises(Exception):
                    acceptance.query("a")
                self.assertEqual(acceptance.retry.call_count, 1)
                self.assertEqual(acceptance.reclaimed_statuses, 0)

    def test_exact_types_values_and_complete_single_stream(self):
        import pyarrow as pa
        expected = [{"tenant": "a", "bucket": 1, "n": 4, "present": 3, "total": 9}]
        table = pa.Table.from_pylist(expected)
        def encode(value):
            output = io.BytesIO()
            with pa.ipc.new_stream(output, value.schema) as writer:
                writer.write_table(value)
            return output.getvalue()
        good = encode(table)
        self.assertEqual(fixture.verify_arrow(good, expected), 1)
        self.assertEqual(fixture.verify_completed_arrow(good, {"kelvo-result-completion": "durable-eos-v1"}, expected), 1)
        for headers in ({}, {"kelvo-result-completion": "unknown"}):
            with self.assertRaises(fixture.AcceptanceError):
                fixture.verify_completed_arrow(good, headers, expected)
        with self.assertRaises(Exception):
            fixture.verify_completed_arrow(good[:-8], {"kelvo-result-completion": "durable-eos-v1"}, expected)
        wrong = table.set_column(1, "bucket", pa.array([1], type=pa.int32()))
        for raw in (good[:-8], good+good, encode(wrong), b"bad"+fixture.EOS):
            with self.assertRaises(Exception):
                fixture.verify_arrow(raw, expected)
        changed = [dict(expected[0], present=4)]
        with self.assertRaises(fixture.AcceptanceError):
            fixture.verify_arrow(good, changed)


if __name__ == "__main__":
    unittest.main()

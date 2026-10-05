#!/usr/bin/env python3
"""Fail-closed export grants and offline fixture identity controls."""
import hashlib
import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import export_broker_acceptance as fixture
import nats_export_permissions as permissions


def matches(pattern, subject):
    want, got = pattern.split("."), subject.split(".")
    for index, token in enumerate(want):
        if token == ">":
            return index < len(got)
        if index >= len(got) or token not in ("*", got[index]):
            return False
    return len(want) == len(got)


def allowed(role, subject, exports=True):
    return any(matches(pattern, subject) for pattern in permissions.tenant_permissions(role, exports)["publish"]["allow"])


class ExportPermissionTests(unittest.TestCase):
    def test_runtime_cannot_manage_even_declared_resources(self):
        for role in ("gateway", "worker"):
            for stream in permissions.BASE_STREAMS + permissions.EXPORT_STREAMS:
                for operation in ("CREATE", "UPDATE", "DELETE", "PURGE"):
                    self.assertFalse(allowed(role, "$JS.API.STREAM." + operation + "." + stream))
            for subject in ("$JS.API.CONSUMER.CREATE.KELVO_EXPORT_QUEUE.exports.export.ready",
                            "$JS.API.CONSUMER.DELETE.KELVO_EXPORT_QUEUE.exports", "$KV.KELVO_META.meta.config"):
                self.assertFalse(allowed(role, subject))

    def test_exact_initializer_namespace_and_consumer(self):
        for stream in permissions.BASE_STREAMS + permissions.EXPORT_STREAMS:
            self.assertTrue(allowed("initializer", "$JS.API.STREAM.CREATE." + stream))
        for subject in ("$JS.API.STREAM.CREATE.UNDECLARED", "$JS.API.STREAM.DELETE.UNDECLARED",
                        "$JS.API.CONSUMER.CREATE.KELVO_EXPORT_QUEUE.other.export.ready",
                        "$JS.API.CONSUMER.CREATE.KELVO_EXPORT_QUEUE.exports.foreign.ready",
                        "$JS.API.CONSUMER.DELETE.KELVO_EXPORT_QUEUE.other", "export.ready"):
            self.assertFalse(allowed("initializer", subject))

    def test_exports_are_explicit_and_role_split_is_preserved(self):
        for role in ("initializer", "gateway", "worker"):
            for subject in ("$JS.API.STREAM.INFO.KV_KELVO_EXPORT_JOBS", "$KV.KELVO_EXPORT_JOBS.export.0", "export.ready"):
                self.assertFalse(allowed(role, subject, exports=False))
        self.assertTrue(allowed("gateway", "export.ready"))
        self.assertFalse(allowed("worker", "export.ready"))
        self.assertTrue(allowed("worker", "$JS.API.CONSUMER.MSG.NEXT.KELVO_EXPORT_QUEUE.exports"))
        self.assertFalse(allowed("gateway", "$JS.API.CONSUMER.MSG.NEXT.KELVO_EXPORT_QUEUE.exports"))
        self.assertFalse(allowed("worker", "$JS.ACK.OTHER.exports.1.2.3"))
        self.assertFalse(allowed("worker", "$JS.ACK.KELVO_EXPORT_QUEUE.other.1.2.3"))

    def test_no_direct_read_or_admin_wildcard(self):
        for role in ("initializer", "gateway", "worker"):
            self.assertFalse(allowed(role, "$JS.API.DIRECT.GET.KV_KELVO_EXPORT_JOBS.export.0"))
            grants = permissions.tenant_permissions(role, True)
            self.assertEqual(grants["subscribe"]["allow"], ["_INBOX.>"])
            self.assertNotIn("$JS.API.>", grants["publish"]["allow"])

    def test_optional_template_is_current_and_contains_no_credentials(self):
        path = Path(__file__).resolve().parents[1] / "deploy/examples/nats-export-permissions.yml"
        self.assertEqual(path.read_text(), permissions.template())
        self.assertNotIn("password", permissions.template())
        for role in ("initializer", "gateway", "worker"):
            full = permissions.tenant_permissions(role, True)["publish"]["allow"]
            merged = permissions.tenant_permissions(role, False)["publish"]["allow"] + permissions.export_delta(role)["publish"]["allow"]
            self.assertEqual(set(merged), set(full))

    def test_invalid_role_and_opt_in_fail_closed(self):
        for role, exports in (("admin", True), ("worker", "true"), ("initializer", 1)):
            with self.assertRaises(ValueError):
                permissions.tenant_permissions(role, exports)


class FixtureIdentityTests(unittest.TestCase):
    def test_dispatch_mode_is_separate_and_keeps_explicit_test_selection(self):
        self.assertEqual(set(fixture.ACCEPTANCE_TESTS), {"acl", "lifecycle", "dispatch"})
        self.assertEqual(fixture.ACCEPTANCE_TESTS["dispatch"], "TestNATSExportDispatchRenewalConflictAndDelayedRedelivery")
        self.assertEqual(fixture.FIXTURE_PREFIXES["dispatch"], "KELVO_TEST_EXPORT_DISPATCH_NATS")
        self.assertEqual(set(fixture.FIXTURE_PREFIXES), {"acl", "lifecycle", "dispatch", "query", "protected-export"})
        self.assertEqual(fixture.FIXTURE_PREFIXES["query"], "KELVO_TEST_QUERY_NATS")
        self.assertEqual(fixture.FIXTURE_PREFIXES["protected-export"], "KELVO_TEST_PROTECTED_EXPORT_NATS")
        self.assertEqual(len(set(fixture.FIXTURE_PREFIXES.values())), 5)
        self.assertIn("synthetic Arrow executor", fixture.SCOPES["dispatch"])
        broker = fixture.ExportBrokerFixture("/unused", "/unused", "0" * 64, "2.15.0", "dispatch")
        self.assertEqual(broker.mode, "dispatch")

    def test_early_failure_preserves_identity_and_private_receipt(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(fixture, "source_manifest", side_effect=ValueError("invalid source")):
            output = Path(directory) / "receipt.json"
            args = SimpleNamespace(output=str(output), fixture=str(Path(directory) / "broker"), mode="acl",
                                   nats_server="/missing", server_sha256="0" * 64, server_version="2.15.0",
                                   tags="duckdb_arrow", source_manifest="/invalid")
            old_mask = os.umask(0o022)
            try:
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(fixture.run(args), 1)
            finally:
                os.umask(old_mask)
            report = json.loads(output.read_text())
            self.assertFalse(report["passed"])
            self.assertTrue(report["broker_stopped"])
            self.assertEqual(report["server_binary_sha256"], args.server_sha256)
            self.assertEqual(report["server_version"], args.server_version)
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            self.assertFalse(Path(args.fixture).exists())

    def test_prelaunch_failure_removes_only_owned_private_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            broker = fixture.ExportBrokerFixture(Path(directory) / "broker", "/missing", "0" * 64, "2.15.0")
            def fail_start():
                broker.directory.mkdir(mode=0o700)
                broker.created = True
                for name in ("server.key", "broker.conf", "environment.json"):
                    (broker.directory / name).write_text("private fixture data")
                (broker.directory / "diagnostic.txt").write_text("preserve diagnostics")
                raise RuntimeError("prelaunch failure")
            with mock.patch.object(broker, "start", side_effect=fail_start), self.assertRaises(RuntimeError):
                with broker:
                    self.fail("incomplete fixture started")
            self.assertEqual([p.name for p in broker.directory.iterdir()], ["diagnostic.txt"])

    def test_unsupported_version_fails_before_creating_state(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(fixture.platform, "system", return_value="Linux"):
            root = Path(directory) / "private"
            with self.assertRaises(RuntimeError):
                with fixture.ExportBrokerFixture(root, "/missing", "0" * 64, "2.12.0"):
                    self.fail("unsupported fixture started")
            self.assertFalse(root.exists())

    def test_hash_mismatch_and_symlink_fail_without_launch(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(fixture.platform, "system", return_value="Linux"), mock.patch.object(fixture.subprocess, "run") as run:
            binary = Path(directory) / "broker"
            binary.write_bytes(b"synthetic executable identity")
            link = Path(directory) / "link"
            link.symlink_to(binary)
            for candidate, digest in ((binary, "0" * 64), (link, hashlib.sha256(binary.read_bytes()).hexdigest())):
                root = Path(directory) / "private"
                with self.assertRaises(RuntimeError):
                    with fixture.ExportBrokerFixture(root, candidate, digest, "2.15.0"):
                        self.fail("unverified broker started")
                self.assertFalse(root.exists())
            run.assert_not_called()

    def test_unknown_mode_is_rejected(self):
        with self.assertRaises(ValueError):
            fixture.ExportBrokerFixture("/unused", "/unused", "0" * 64, "2.15.0", "union")


if __name__ == "__main__":
    unittest.main()

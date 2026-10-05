#!/usr/bin/env python3
"""Offline controls for protected exports' separate broker identity and grants."""
import json
import unittest
from unittest import mock

import export_broker_acceptance as broker
import protected_export_acceptance as fixture
import protected_query_acceptance as shared
from test_protected_query_acceptance import BrokerControlFixture, GROUP, IDENTITY, Process


class ProtectedExportControls(BrokerControlFixture, unittest.TestCase):
    def test_export_accounts_have_exact_roles_and_bounded_storage(self):
        owned = fixture.ProtectedExportBroker(self.root / "exports", self.binary, self.digest, GROUP)
        stack, launch, _ = self.start_context()
        with stack, self.close_context():
            with owned:
                config = json.loads((owned.directory / "broker.conf").read_text())
                self.assertEqual(set(owned.environment), fixture.ENVIRONMENT)
                self.assertTrue(set(owned.environment).isdisjoint(shared.ENVIRONMENT))
                self.assertEqual(set(config["accounts"]), {"a", "b"})
                self.assertEqual(config["host"], "127.0.0.1")
                self.assertNotIn("authorization", config)
                self.assertEqual(config["max_payload"], 1 << 20)
                self.assertEqual(config["max_connections"], 32)
                self.assertEqual(config["max_subscriptions"], 256)
                self.assertEqual(config["max_pending"], 4 << 20)
                self.assertFalse(config["debug"])
                self.assertFalse(config["trace"])
                self.assertEqual(config["jetstream"], {"store_dir": str(owned.directory / "state"),
                                                       "max_mem_store": 32 << 20, "max_file_store": 128 << 20})
                passwords = set()
                for tenant, account in config["accounts"].items():
                    self.assertEqual(account["jetstream"], {"max_memory": 16 << 20, "max_file": 64 << 20,
                                                            "max_streams": 5, "max_consumers": 2})
                    self.assertEqual(len(account["users"]), 3)
                    for role, user in zip(("initializer", "gateway", "worker"), account["users"]):
                        prefix = "KELVO_TEST_PROTECTED_EXPORT_NATS_" + tenant.upper() + "_" + role.upper()
                        self.assertEqual(user["user"], tenant + "-" + role)
                        self.assertEqual(owned.environment[prefix + "_USER"], user["user"])
                        self.assertEqual(owned.environment[prefix + "_PASSWORD"], user["password"])
                        passwords.add(user["password"])
                        self.assertRegex(user["password"], r"^[0-9a-f]{64}$")
                        self.assertEqual(user["permissions"], broker.tenant_permissions(role, True))
                        subjects = user["permissions"]["publish"]["allow"]
                        self.assertNotIn("$JS.API.>", subjects)
                        self.assertNotIn(">", subjects)
                        self.assertEqual(user["permissions"]["subscribe"]["allow"], ["_INBOX.>"])
                        if role != "initializer":
                            self.assertFalse(any(".STREAM.CREATE." in value or ".STREAM.DELETE." in value for value in subjects))
                self.assertEqual(len(passwords), 6)
                self.assertEqual(launch.call_args.args[0], [str(self.binary), "-c", str(owned.directory / "broker.conf")])
            self.assertTrue(fixture.valid_broker(owned.receipt, GROUP))
            self.assertFalse((owned.directory / "broker.conf").exists())
            self.assertFalse((owned.directory / "environment.json").exists())
            self.assertFalse((owned.directory / "server.key").exists())

    def test_export_broker_refuses_query_or_extra_credentials(self):
        for environment in (shared.ENVIRONMENT, fixture.ENVIRONMENT | {"UNRELATED_CREDENTIAL"}, fixture.ENVIRONMENT - {"KELVO_TEST_PROTECTED_EXPORT_NATS_URL"}):
            with self.subTest(count=len(environment)):
                owned = fixture.ProtectedExportBroker(self.root / "wrong-env", self.binary, self.digest, GROUP)
                owned.process = Process()
                owned.identity = dict(IDENTITY)
                owned.environment = {name: "fixture" for name in environment}
                with mock.patch.object(broker.ExportBrokerFixture, "start", return_value=owned), \
                        mock.patch.object(shared, "process_identity", return_value=dict(IDENTITY)), self.assertRaises(RuntimeError):
                    owned.start()

    def test_export_gate_is_explicit_and_keeps_its_source_contract(self):
        self.assertEqual(fixture.ROOT, "TestContainedProtectedObjectExports")
        self.assertEqual(len(fixture.LEAVES), len(set(fixture.LEAVES)))
        self.assertEqual(len(fixture.LEAVES), 12)
        self.assertTrue(all(name.startswith(fixture.ROOT + "/") for name in fixture.LEAVES))
        self.assertNotIn("protected-export", broker.ACCEPTANCE_TESTS)
        self.assertIn("internal/cluster/protected_object_export_linux_test.go", fixture.SOURCE_REQUIRED)
        self.assertIn("scripts/protected_query_acceptance.py", fixture.SOURCE_REQUIRED)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Owned, offline broker support for contained protected-source exports."""
from protected_query_acceptance import QueryBroker, VERSION, seed_binary, valid_broker

ROOT = "TestContainedProtectedObjectExports"
LEAVES = [ROOT + "/" + name for name in (
    "publish-real-single-and-two-part", "exact-principal-fills-and-repeat-downloads",
    "foreign-handles-and-hidden-columns", "forged-stale-and-unsupported-before-io",
    "revocation-before-ready", "revocation-none-chunked", "revocation-none-length",
    "revocation-lz4-chunked", "revocation-lz4-length", "lost-completion-is-not-replayed",
    "cancelled-range-retains-custody", "stalled-registry-retains-custody")]
SOURCE_REQUIRED = {"scripts/protected_export_acceptance.py", "scripts/protected_query_acceptance.py",
                   "scripts/export_broker_acceptance.py", "scripts/nats_export_permissions.py",
                   "internal/cluster/protected_object_export_linux_test.go",
                   "internal/cluster/protected_object_export_fixture_linux_test.go",
                   "internal/cluster/protected_object_query_fixture_linux_test.go",
                   "internal/testutil/protectedobject/fixture.go", "internal/testutil/protectedobject/http.go"}
ENVIRONMENT = {"KELVO_TEST_PROTECTED_EXPORT_NATS_URL", "KELVO_TEST_PROTECTED_EXPORT_NATS_CA_FILE"} | {
    "KELVO_TEST_PROTECTED_EXPORT_NATS_" + tenant + "_" + role + "_" + field
    for tenant in ("A", "B") for role in ("INITIALIZER", "GATEWAY", "WORKER") for field in ("USER", "PASSWORD")}


class ProtectedExportBroker(QueryBroker):
    """Share exact process custody while requiring distinct export-enabled roles."""
    def __init__(self, directory, binary, binary_sha256, group):
        super().__init__(directory, binary, binary_sha256, group,
                         mode="protected-export", environment=ENVIRONMENT)

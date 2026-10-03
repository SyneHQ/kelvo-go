#!/usr/bin/env python3
"""Signed TLS object-to-local migration and sandboxed recovery acceptance.

Uses the existing disposable four-provider TLS fixture with process-scoped CA
trust. It never changes host trust or touches a real cloud account. Source
files, credential references and private logs stay under ignored artifacts.
"""
import base64
import copy
import csv
import io
import os
from pathlib import Path
import re
import sys
import ssl
import threading
import uuid

from object_acceleration_acceptance import ObjectAcceptance, FixtureServer, RedirectSinkHandler, main
from acceleration_acceptance import (
    ROOT, TYPED_SQL, check_typed, dataset, private_directory, require,
    typed_csv, write_config, write_private, parse_status,
)


def migration_status(raw):
    require(raw.startswith(b"verified: true\n"), "migration did not report verified completion")
    snapshot = parse_status(raw.replace(b"verified: true\n", b"ready: true\n", 1))
    for name in ("source_fingerprint", "source_generation_sha256"):
        match = re.search(rb"^" + name.encode() + rb": ([0-9a-f]{64})$", raw, re.MULTILINE)
        require(match is not None, "migration provenance is absent")
        snapshot[name] = match.group(1).decode()
    return snapshot


class ObjectMigrationAcceptance(ObjectAcceptance):
    def __init__(self, args):
        super().__init__(args)
        self.launcher = Path(os.environ.get("KELVO_TEST_SANDBOX_BINARY", self.binary.parent / "kelvo-landlock")).resolve()
        require(self.launcher.is_file() and os.access(self.launcher, os.X_OK), "built sandbox launcher unavailable")

    def setup_certificates(self):
        # Every object operation runs in a fresh trusted parent CLI. The workers
        # only read CSV or recovered local Parquet and require no fixture trust.
        self.ca = self.directory / "ca.crt"
        ca_key, server_key = self.directory / "ca.key", self.directory / "server.key"
        request, certificate = self.directory / "server.csr", self.directory / "server.crt"
        self.command(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(ca_key),
                      "-out", str(self.ca), "-days", "2", "-subj", "/CN=KelvoMigrationFixture-" + uuid.uuid4().hex,
                      "-addext", "basicConstraints=critical,CA:TRUE"])
        self.command(["openssl", "req", "-newkey", "rsa:2048", "-nodes", "-keyout", str(server_key),
                      "-out", str(request), "-subj", "/CN=localhost"])
        extension = self.directory / "server.ext"
        write_private(extension, "subjectAltName=IP:127.0.0.1,DNS:localhost\nbasicConstraints=critical,CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n")
        self.command(["openssl", "x509", "-req", "-in", str(request), "-CA", str(self.ca), "-CAkey", str(ca_key),
                      "-set_serial", "2", "-out", str(certificate), "-days", "2", "-extfile", str(extension)])
        self.env["SSL_CERT_FILE"] = str(self.ca)
        self.env["SSL_CERT_DIR"] = str(private_directory(self.directory / "empty-ca-directory"))
        self.server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.server_tls.minimum_version = ssl.TLSVersion.TLSv1_2
        self.server_tls.load_cert_chain(certificate, server_key)
        self.client_tls = ssl.create_default_context(cafile=str(self.ca))
        sink = FixtureServer(("127.0.0.1", 0), RedirectSinkHandler)
        sink.daemon_threads, sink.foreign_requests = True, 0
        sink.socket = self.server_tls.wrap_socket(sink.socket, server_side=True)
        thread = threading.Thread(target=sink.serve_forever, daemon=True)
        thread.start()
        self.servers.append((sink, thread))
        self.redirect_sink = sink

    def cli(self, args, success=True, timeout=45):
        if args and (args[0] == "query" or args[:2] == ["accelerate", "refresh"]):
            args = [*args, "--sandbox", self.launcher]
        return super().cli(args, success, timeout)

    def migrate(self, source, target, success=True):
        raw, _ = self.cli(["accelerate", "migrate-backup", "--config", source,
                           "--destination-config", target, "--dataset", "typed_snapshot"], success)
        return migration_status(raw) if success else None

    def run_provider(self, provider):
        self.provider, self.stage = provider, provider + "_migration_setup"
        state, endpoint = self.fixture(provider)
        provider_dir = private_directory(self.directory / provider)
        for layout, empty in (("single", False), ("multipart", False), ("single_empty", True), ("multipart_empty", True)):
            self.stage = provider + "_" + layout
            directory = private_directory(provider_dir / layout)
            source = directory / "source.csv"
            rows = list(csv.reader(io.StringIO(typed_csv())))
            rows[0].append("padding_text")
            for row in rows[1:]:
                row.append(base64.b64encode(os.urandom(256 << 10)).decode())
            encoded = io.StringIO(newline="")
            csv.writer(encoded).writerows(rows)
            write_private(source, encoded.getvalue())
            sql = TYPED_SQL.replace(" FROM raw", ", padding_text AS padding FROM raw")
            if empty:
                sql += " WHERE false"
            refresh = dataset("typed_snapshot", sql)
            if layout.startswith("multipart"):
                refresh["multipart"] = {"max_part_bytes": 1 << 20, "max_parts": 8}
            config = {
                "extension_directory": str(self.extension_directory),
                "sources": [{"id": "raw", "type": "csv", "path": str(source)},
                            {"id": "unused_database", "type": "postgresql", "dsn_env": "KELVO_SOURCE_NEVER_SET"}],
                "acceleration": {"directory": str(directory / "remote-stage"), "tenant_id": "acceptance",
                                 "object_storage": self.storage_config(state, endpoint), "datasets": [refresh]},
            }
            remote_catalog = directory / "remote.yml"
            write_config(remote_catalog, config)
            original = self.status(remote_catalog, "typed_snapshot", command="refresh")
            # A real migration must not reopen its source data or database.
            source.unlink()
            target = copy.deepcopy(config)
            target["acceleration"].pop("object_storage")
            target["acceleration"]["directory"] = str(directory / "recovered")
            target_catalog = directory / "local.yml"
            write_config(target_catalog, target)
            before = state.event_count()
            migrated = self.migrate(remote_catalog, target_catalog)
            events = state.events_since(before)
            require(events and all(e["identity"] == "reader" and e["method"] in ("GET", "HEAD") for e in events),
                    "migration used writes or non-reader credentials")
            require(not state.auth_failures, "object request signature or reader grant failed")
            require(migrated["generation"] == original["generation"] and migrated["refreshed_at"] == original["refreshed_at"],
                    "migration changed original identity or data age")
            require(migrated["source_fingerprint"] == original["fingerprint"] and migrated["fingerprint"] != original["fingerprint"],
                    "migration failed explicit target-policy derivation")
            require(migrated["source_generation_sha256"] == original["sha256"] and migrated["rows"] == original["rows"]
                    and migrated["bytes"] == original["bytes"], "migration lost provenance or payload accounting")
            parts = list((directory / "recovered" / "acceptance" / "typed_snapshot").glob("*.parquet"))
            require(parts and (not layout.startswith("multipart") or empty or len(parts) > 1), "multipart fixture did not exercise multiple parts")
            # Make every remote object unavailable and remove object credentials.
            with state.lock:
                saved_objects, state.objects = state.objects, {}
            saved_env = self.env
            self.env = {key: value for key, value in saved_env.items() if not key.startswith("KELVO_SOURCE_")}
            local_before = state.event_count()
            try:
                verified = self.status(target_catalog, "typed_snapshot", command="verify")
                table, stats = self.query(target_catalog)
                check_typed(table, empty=empty)
                require(verified["generation"] == original["generation"] and stats["accelerations"][0]["generation"] == original["generation"],
                        "recovered query used another generation")
                require(state.event_count() == local_before, "local recovery contacted remote storage")
            finally:
                self.env = saved_env
                with state.lock:
                    state.objects = saved_objects
            self.record("migrate_" + layout + "_and_query_without_source_or_object_access",
                        rows=migrated["rows"], encoded_bytes=migrated["bytes"], parts=len(parts), sandboxed=True)
            self.migrate(remote_catalog, target_catalog, success=False)
            self.record("migrate_" + layout + "_rejects_existing_destination")
            if layout == "single":
                altered = copy.deepcopy(target)
                altered["acceleration"]["directory"] = str(directory / "invalid-policy")
                altered["acceleration"]["datasets"][0]["authorization_version"] = "different-grants"
                bad_catalog = directory / "bad-policy.yml"
                write_config(bad_catalog, altered)
                before = state.event_count()
                self.migrate(remote_catalog, bad_catalog, success=False)
                require(state.event_count() == before and not (directory / "invalid-policy").exists(),
                        "invalid policy reached storage")
                self.record("migration_policy_mismatch_fails_before_object_access")
                altered = copy.deepcopy(target)
                altered["acceleration"]["directory"] = str(directory / "corrupt-result")
                corrupt_catalog = directory / "corrupt-target.yml"
                write_config(corrupt_catalog, altered)
                with self.fault(state, "corrupt_digest"):
                    self.migrate(remote_catalog, corrupt_catalog, success=False)
                require(not (directory / "corrupt-result").exists(), "corrupt object published a target")
                self.record("migration_corrupt_object_metadata_never_publishes")


if __name__ == "__main__":
    sys.exit(main(ObjectMigrationAcceptance, ROOT / "docs/evidence/object-migration-acceptance.json"))

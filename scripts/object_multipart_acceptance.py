#!/usr/bin/env python3
"""Remote multipart CLI acceptance over real TLS protocol fixtures, not live clouds."""
import base64
import copy
import csv
import io
import re
import sys

import pyarrow.parquet as pq

from object_acceleration_acceptance import ObjectAcceptance, main, digest
from acceleration_acceptance import ROOT, TYPED_SCHEMA, TYPED_SQL, check_typed, dataset, private_directory, require, typed_csv, write_config, write_private, parse_status


class MultipartObjectAcceptance(ObjectAcceptance):
    def run_provider(self, provider):
        self.provider, self.stage = provider, provider + "_multipart_setup"
        state, endpoint = self.fixture(provider)
        directory = private_directory(self.directory / provider)
        source = directory / "source.csv"
        rows = list(csv.reader(io.StringIO(typed_csv())))
        rows[0].append("padding_text")
        # One ~350KiB row fits a part's512KiB rotation target; two do not.
        import os
        for row in rows[1:]:
            row.append(base64.b64encode(os.urandom(256 << 10)).decode())
        encoded = io.StringIO(newline="")
        csv.writer(encoded).writerows(rows)
        write_private(source, encoded.getvalue())
        refresh = dataset("typed_snapshot", TYPED_SQL.replace(" FROM raw", ", padding_text AS padding FROM raw"))
        config = {"extension_directory": str(self.extension_directory), "sources": [{"id": "raw", "type": "csv", "path": str(source)}], "acceleration": {"directory": str(directory / "publisher-stage"), "tenant_id": "acceptance", "object_storage": self.storage_config(state, endpoint), "datasets": [refresh]}}
        writer_catalog = directory / "publisher.yml"
        write_config(writer_catalog, config)
        initial = self.status(writer_catalog, "typed_snapshot", command="refresh")
        refresh["multipart"] = {"max_part_bytes": 1 << 20, "max_parts": 8}
        write_config(writer_catalog, config)
        self.stage = provider + "_multipart_publish"
        current = self.status(writer_catalog, "typed_snapshot", command="refresh")
        prefix = "/snapshots/private/acceptance/typed_snapshot/"
        descriptor_key = prefix + current["generation"] + ".parts.yaml"
        with state.lock:
            descriptor = state.objects[descriptor_key]["body"]
            part_keys = sorted(key for key in state.objects if re.fullmatch(re.escape(prefix + current["generation"]) + r"-part-[0-9]{4}\.parquet", key))
            payloads = [state.objects[key]["body"] for key in part_keys]
        require(1 < len(payloads) <= 8 and sum(map(len, payloads)) == current["bytes"], "multipart accounting changed")
        require(all(len(body) <= 1 << 20 for body in payloads), "part encoded limit exceeded")
        require(digest(descriptor) == current["sha256"], "root does not authenticate descriptor")
        require(sum(pq.ParquetFile(io.BytesIO(body)).metadata.num_rows for body in payloads) == 3, "part row count changed")
        self.record("multipart_upload_commits_descriptor_and_bounded_parts", parts=len(payloads), encoded_bytes=current["bytes"])

        reader = copy.deepcopy(config)
        reader["acceleration"]["directory"] = str(directory / "fresh-reader")
        reader_catalog = directory / "reader.yml"
        write_config(reader_catalog, reader)
        source.unlink()
        self.stage = provider + "_multipart_ranges"
        before = state.event_count()
        table, stats = self.query(reader_catalog)
        check_typed(table)
        require(stats["accelerations"][0]["generation"] == current["generation"], "query lost pinned generation")
        events = state.events_since(before)
        reads = [e for e in events if e["kind"] == "parquet" and e["method"] == "GET"]
        require(reads and all(e["range"] and e["status"] == 206 for e in reads), "query fetched full data objects")
        require(all(e["identity"] == "reader" and e["method"] in ("GET", "HEAD") for e in events), "query leaked writer identity")
        require(sum(e["bytes"] for e in reads) < current["bytes"], "projection read all padding")
        self.status(reader_catalog, "typed_snapshot", command="verify")
        self.record("fresh_reader_queries_all_parts_with_reader_only_bounded_ranges")

        self.stage = provider + "_multipart_restore"
        restored, _ = self.cli(["accelerate", "restore", "--config", writer_catalog, "--dataset", "typed_snapshot", "--generation", initial["generation"], "--expected-generation", current["generation"]])
        require(parse_status(restored)["refreshed_at"] == initial["refreshed_at"], "restore changed original freshness")
        check_typed(self.query(reader_catalog)[0])
        self.cli(["accelerate", "restore", "--config", writer_catalog, "--dataset", "typed_snapshot", "--generation", current["generation"], "--expected-generation", initial["generation"]])
        check_typed(self.query(reader_catalog)[0])
        self.record("single_and_multipart_remote_restore_preserves_values_and_freshness")

        self.stage = provider + "_multipart_descriptor_corruption"
        with state.lock:
            original = state.objects[descriptor_key]
            altered = copy.deepcopy(original)
            altered["body"] += b"unapproved: true\n"
            altered["digest"] = digest(altered["body"])
            state.objects[descriptor_key] = altered
        try:
            self.query(reader_catalog, success=False)
        finally:
            with state.lock:
                state.objects[descriptor_key] = original
        with state.lock:
            missing = state.objects.pop(part_keys[-1])
        try:
            self.query(reader_catalog, success=False)
        finally:
            with state.lock:
                state.objects[part_keys[-1]] = missing
        check_typed(self.query(reader_catalog)[0])
        self.record("changed_descriptor_and_missing_last_part_fail_closed")

        self.stage = provider + "_multipart_transport_failures"
        for failure in ("range_redirect", "ignored_range", "wrong_range"):
            with self.fault(state, failure):
                self.query(reader_catalog, success=False)
        require(self.redirect_sink.foreign_requests == 0, "range redirect leaked a capability")
        self.reader_put_is_denied(state, endpoint)
        self.record("multipart_transport_failures_and_reader_write_denial")


if __name__ == "__main__":
    sys.exit(main(MultipartObjectAcceptance, ROOT / "docs/evidence/object-multipart-acceptance.json"))

#!/usr/bin/env python3
"""Read dependency selection from the full binary, not unused module graphs."""
import unittest

from build_nats_compatibility import linked_modules


class LinkedModuleTests(unittest.TestCase):
    def test_actual_versions_checksums_and_native_replacement_are_preserved(self):
        info = "/private/kelvo: go1.26.8\n\tpath\tgithub.com/SYNEHQ/kelvo-go/cmd/kelvo\n"
        info += "\tdep\tgithub.com/nats-io/nats.go\tv1.53.1\th1:checked\n"
        info += "\tdep\tgithub.com/duckdb/duckdb-go/v2\tv2.5.6\n\t=>\t/private/native\t(devel)\t\n"
        info += "\tbuild\t-tags=duckdb_arrow,duckbridge\n"
        self.assertEqual(linked_modules(info), [
            {"path": "github.com/nats-io/nats.go", "version": "v1.53.1", "sum": "h1:checked", "replacement": None},
            {"path": "github.com/duckdb/duckdb-go/v2", "version": "v2.5.6", "sum": None,
             "replacement": {"path": "/private/native", "version": "(devel)"}},
        ])


if __name__ == "__main__":
    unittest.main()

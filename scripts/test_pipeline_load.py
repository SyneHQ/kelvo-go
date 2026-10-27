import datetime as dt
import decimal
import unittest

import pipeline_load as p


class PipelineLoadTest(unittest.TestCase):
    def test_digest_ignores_order_and_integer_representation(self):
        a = p.digest([(1, "x", dt.date(2025, 1, 2)), (2, None, dt.date(2025, 1, 3))])
        b = p.digest([(2, None, dt.date(2025, 1, 3)), (decimal.Decimal("1"), "x", dt.date(2025, 1, 2))])
        self.assertEqual(a, b)
        self.assertNotEqual(a, p.digest([(1, "x", dt.date(2025, 1, 2))]))

    def test_digest_refuses_inexact_values(self):
        with self.assertRaises(ValueError):
            p.digest([(decimal.Decimal("1.5"),)])
        with self.assertRaises(ValueError):
            p.digest([(1.0,)])

    def test_binding_and_sources(self):
        sql = "SELECT * FROM {orders} o JOIN {shipments} s ON 1=1 UNION ALL SELECT * FROM {events_cold}"
        self.assertEqual(p.sources_of(sql, p.FEDERATED), ["cold", "my", "pg"])
        self.assertEqual(p.sources_of(sql, p.SNAPSHOT), ["cold", "orders_snap", "shipments_snap"])
        self.assertIn("pg.orders", p.bind(sql, p.FEDERATED))

    def test_every_workflow_binds_in_all_modes(self):
        doc = p.spec()
        self.assertGreaterEqual(len(doc["workflows"]), 8)
        for workflow in doc["workflows"].values():
            for mapping in (p.FEDERATED, p.SNAPSHOT):
                self.assertNotIn("{", p.bind(workflow["sql"], mapping))

    def test_fifty_unique_tiles(self):
        board = p.tiles()
        self.assertEqual(len(board), 50)
        for _, sql in board:
            self.assertNotIn("{", p.bind(sql, p.SNAPSHOT))


if __name__ == "__main__":
    unittest.main()

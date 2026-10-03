from email.message import Message
import unittest

import node_capacity_client as client


def headers(*pairs):
    result = Message()
    result['Kelvo-Result-Completion'] = 'durable-eos-v1'
    for name, value in pairs:
        result[name] = value
    return result


class CapacityClientControls(unittest.TestCase):
    def test_counter_requires_one_bounded_unsigned_sample(self):
        metric = b'kelvo_jobs_completed_total{kind="query",outcome="success"} '
        self.assertEqual(client.success_counter(metric + b'4\n'), 4)
        for value in (b'', metric + b'-1\n', metric + b'1.0\n', metric + b'18446744073709551616\n',
                      metric + b'4\n' + metric + b'4\n'):
            with self.assertRaises(client.ops.AcceptanceError):
                client.success_counter(value)

    def test_workload_delta_accounts_for_one_startup_probe(self):
        client.verify_success_delta(1, 4, 3)
        client.verify_success_delta(1, 14, 13)
        for before, after, successes in ((0, 3, 3), (2, 5, 3), (1, 3, 3), (1, 5, 3), (1, 0, 3), (True, 4, 3)):
            with self.assertRaises(client.ops.AcceptanceError):
                client.verify_success_delta(before, after, successes)

    def test_chunked_and_bounded_fixed_length_are_valid(self):
        self.assertIsNone(client.response_framing(headers(('Transfer-Encoding', 'chunked'))))
        self.assertEqual(client.response_framing(headers(('Content-Length', '840'))), 840)

    def test_ambiguous_or_absent_framing_fails(self):
        cases = [headers(), headers(('Content-Length', '840'), ('Transfer-Encoding', 'chunked')),
                 headers(('Content-Length', '840'), ('Content-Length', '840')),
                 headers(('Transfer-Encoding', 'chunked'), ('Transfer-Encoding', 'chunked')),
                 headers(('Transfer-Encoding', 'gzip')), headers(('Content-Length', ' 840')),
                 headers(('Content-Length', '7')), headers(('Content-Length', '33554433')),
                 headers(('Content-Length', '840'), ('Kelvo-Result-Completion', 'durable-eos-v1'))]
        for value in cases:
            with self.assertRaises(client.ops.AcceptanceError):
                client.response_framing(value)


if __name__ == '__main__':
    unittest.main()

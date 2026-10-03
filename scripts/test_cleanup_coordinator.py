import errno
import json
import subprocess
import unittest

from cleanup_coordinator import cleanup_every_host, REPORT_LIMIT

HOSTS = ('oracle', 'azure')
GOOD = {'owned_workers_removed': True, 'owned_tunnels_removed': True,
        'owned_tunnel_cgroups_removed': True, 'private_key_removed': True, 'failures': []}
AZURE_GOOD = {'source_user_removed': True, 'forwarding_key_removed': True,
              'source_data_preserved': True, 'brokers_stopped': True, 'gateway_stopped': True, 'failures': []}
REPORTS = {'oracle': GOOD, 'azure': AZURE_GOOD}


def completed(code=0, raw=None, host='oracle'):
    return subprocess.CompletedProcess(['fixture'], code, json.dumps(REPORTS[host]) if raw is None else raw, '')


class CleanupCoordinatorControls(unittest.TestCase):
    def run_case(self, cleanup=None, timer=None, persist=None):
        events = []
        def invoke_cleanup(host):
            events.append(('cleanup', host))
            return cleanup(host) if cleanup else completed(host=host)
        def invoke_timer(host):
            events.append(('timer', host))
            return timer(host) if timer else completed()
        def invoke_persist(host, outcome):
            events.append(('persist', host))
            if persist:
                persist(host, outcome)
        return cleanup_every_host(HOSTS, invoke_cleanup, invoke_timer, invoke_persist), events

    def test_both_successful_then_persist(self):
        result, events = self.run_case()
        self.assertEqual(events, [('cleanup', 'oracle'), ('timer', 'oracle'),
                                  ('cleanup', 'azure'), ('timer', 'azure'),
                                  ('persist', 'oracle'), ('persist', 'azure')])
        for host, item in result.items():
            self.assertTrue(item['cleanup_verified'] and item['timer_stopped'] and item['persisted'])
            self.assertEqual(item['cleanup_report'], REPORTS[host])

    def test_local_enospc_first_or_every_persist_never_skips_remote_cleanup(self):
        for failing_hosts in ({'oracle'}, set(HOSTS)):
            with self.subTest(failing_hosts=failing_hosts):
                def persist(host, outcome):
                    if host in failing_hosts:
                        raise OSError(errno.ENOSPC, 'PRIVATE_PATH_NOT_FOR_RECEIPT')
                result, events = self.run_case(persist=persist)
                self.assertEqual(events[-2:], [('persist', 'oracle'), ('persist', 'azure')])
                self.assertEqual(sum(phase == 'cleanup' for phase, _ in events), 2)
                self.assertEqual(sum(phase == 'timer' for phase, _ in events), 2)
                for host in HOSTS:
                    self.assertTrue(result[host]['cleanup_verified'] and result[host]['timer_stopped'])
                    self.assertEqual(result[host]['persisted'], host not in failing_hosts)
                self.assertNotIn('PRIVATE_PATH', json.dumps(result))

    def test_remote_cleanup_exception_first_retains_timer_and_reaches_second(self):
        for exception in (OSError('PRIVATE_TRANSPORT'), KeyboardInterrupt(), SystemExit(3)):
            with self.subTest(exception=type(exception).__name__):
                def cleanup(host):
                    if host == 'oracle':
                        raise exception
                    return completed(host=host)
                result, events = self.run_case(cleanup=cleanup)
                self.assertFalse(result['oracle']['cleanup_verified'])
                self.assertFalse(result['oracle']['timer_stop_attempted'])
                self.assertTrue(result['azure']['cleanup_verified'] and result['azure']['timer_stopped'])
                self.assertIn(('cleanup', 'azure'), events)
                self.assertNotIn(('timer', 'oracle'), events)
                self.assertNotIn('PRIVATE_TRANSPORT', json.dumps(result))

    def test_nonzero_malformed_missing_false_and_unbounded_reports_keep_timer(self):
        invalid = [completed(1), completed(0, 'not-json'), completed(0, '{}'),
                   completed(0, json.dumps({'failures': []})),
                   completed(0, json.dumps(dict(GOOD, private_key_removed=False))),
                   completed(0, json.dumps(dict(GOOD, private_key_removed='true'))),
                   completed(0, json.dumps(dict(GOOD, private_key_removed=1))),
                   completed(0, json.dumps(dict(GOOD, failures=['PRIVATE_FAILURE']))),
                   completed(0, json.dumps(dict(GOOD, failures=None))),
                   completed(0, json.dumps({'failures': [], 'anything': True})),
                   completed(0, json.dumps({key: value for key, value in GOOD.items() if key != 'private_key_removed'})),
                   completed(0, json.dumps(dict(GOOD, anything=True))),
                   completed(0, json.dumps(AZURE_GOOD)),
                   completed(0, json.dumps(GOOD).replace('"private_key_removed": true', '"private_key_removed": false, "private_key_removed": true')),
                   completed(0, ' ' * (REPORT_LIMIT + 1)), completed(0, b'\xff'),
                   completed(0, json.dumps([True, []]))]
        for response in invalid:
            with self.subTest(raw_type=type(response.stdout).__name__, exit=response.returncode):
                result, events = self.run_case(cleanup=lambda host: response if host == 'oracle' else completed(host=host))
                self.assertFalse(result['oracle']['cleanup_verified'])
                self.assertFalse(result['oracle']['timer_stop_attempted'])
                self.assertNotIn(('timer', 'oracle'), events)
                self.assertTrue(result['azure']['timer_stopped'])
                self.assertNotIn('PRIVATE_FAILURE', json.dumps(result))

    def test_timer_exception_or_nonzero_is_uncertain_but_other_host_continues(self):
        for failure in (OSError('PRIVATE_TIMER'), KeyboardInterrupt(), completed(1)):
            with self.subTest(failure=type(failure).__name__):
                def timer(host):
                    if host == 'oracle':
                        if isinstance(failure, BaseException):
                            raise failure
                        return failure
                    return completed()
                result, events = self.run_case(timer=timer)
                self.assertTrue(result['oracle']['cleanup_verified'] and result['oracle']['timer_stop_attempted'])
                self.assertFalse(result['oracle']['timer_stopped'])
                self.assertTrue(result['azure']['timer_stopped'])
                self.assertIn(('cleanup', 'azure'), events)
                self.assertNotIn('PRIVATE_TIMER', json.dumps(result))

    def test_baseexception_during_persistence_does_not_skip_next_receipt(self):
        def persist(host, outcome):
            if host == 'oracle':
                raise KeyboardInterrupt()
        result, events = self.run_case(persist=persist)
        self.assertFalse(result['oracle']['persisted'])
        self.assertTrue(result['azure']['persisted'])
        self.assertEqual(events[-1], ('persist', 'azure'))


if __name__ == '__main__':
    unittest.main()

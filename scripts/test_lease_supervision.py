import copy
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock

import lease_supervision as supervision


def unfenced_evidence():
    return {'schema': 1, 'deadline_seconds': 25., 'expiry_basis': 'observed_exact_parent_exit_plus_configured_lease_upper_bound',
            'original_query_deadlines_unchanged': True, 'query_resubmissions': 0, 'errors': [],
            'nodes': [{'node': name, 'fenced_exit_observed': False, 'restart_attempts': 0, 'lease_seconds': 5.,
                       'final_running_verified': True, 'attempts': []} for name in ('a1', 'b1')]}


def recovered_evidence():
    value = unfenced_evidence()
    value['nodes'][0].update(fenced_exit_observed=True, restart_attempts=1, exit_observed_seconds=1.,
        lease_expiry_upper_bound_seconds=6., old_children_gone_seconds=6., restart_started_seconds=6.,
        replacement_ready_seconds=7., same_configuration_verified=True, fresh_owner_claimed=True,
        attempts=[{'number': 1, 'started_seconds': 6.}])
    return value


class SupervisionControls(unittest.TestCase):
    def test_exact_expiry_cleanup_and_fresh_owner_evidence(self):
        self.assertTrue(supervision.valid_evidence(unfenced_evidence()))
        self.assertTrue(supervision.valid_evidence(recovered_evidence()))
        for field, value in [('exit_observed_seconds', -1), ('lease_expiry_upper_bound_seconds', 5),
                             ('old_children_gone_seconds', 5), ('restart_started_seconds', 5),
                             ('replacement_ready_seconds', 26), ('replacement_ready_seconds', float('nan')),
                             ('same_configuration_verified', False), ('fresh_owner_claimed', False),
                             ('restart_attempts', True), ('restart_attempts', 6), ('lease_seconds', True),
                             ('final_running_verified', False), ('replacement_ready_seconds', None),
                             ('attempts', []), ('attempts', [{'number': 1, 'started_seconds': 6., 'exit_code': 1}])]:
            report = recovered_evidence()
            report['nodes'][0][field] = value
            self.assertFalse(supervision.valid_evidence(report), field)
        for field, value in [('schema', True), ('errors', ['UNEXPECTED_EXIT']), ('query_resubmissions', 1),
                             ('query_resubmissions', False), ('original_query_deadlines_unchanged', False),
                             ('nodes', [None, None]), ('nodes', [{'node': 'a1'}, {'node': 'a1'}])]:
            report = recovered_evidence()
            report[field] = value
            self.assertFalse(supervision.valid_evidence(report), field)

    def test_only_fixed_terminal_exit_markers_are_returned(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary)/'node.log'
            for raw, expected in [(b'private credentials\nWorker coordination lease lost\n', supervision.LEASE_EXIT),
                                  (b'Worker coordination lease lost\nother failure\n', None),
                                  (b'worker identity is already active or its store is unavailable\n', next(x for x in supervision.RETRY_EXIT if x.startswith(b'worker identity'))),
                                  (b'secret-token', None)]:
                path.write_bytes(raw)
                self.assertEqual(supervision.exit_marker(path), expected)
                self.assertIsNone(supervision.exit_marker(path, len(raw)))

    def subject(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        config, catalog = root/'a1.yml', root/'a1-catalog.yml'
        config.write_text('policy:\n  lease_duration: 5s\n')
        catalog.write_text('sources: []\n')
        (root/'a1.log').write_bytes(supervision.LEASE_EXIT+b'\n')
        old = mock.Mock(pid=17)
        old.poll.return_value = 1
        acceptance = mock.Mock(directory=root, binary=Path('/private/bin/kelvo'))
        acceptance.call.return_value = (200, b'')
        subject = supervision.LeaseSupervisor.__new__(supervision.LeaseSupervisor)
        subject.acceptance, subject.started, subject.deadline = acceptance, 0., 25.
        subject.errors, subject.thread, subject.stopping = [], None, threading.Event()
        state = {'process': old, 'identity': (17, 'old'), 'config': config, 'catalog': catalog,
            'hashes': (supervision.ops.sha256(config), supervision.ops.sha256(catalog)), 'lease_seconds': 5.,
            'runtime': root/'a1-runtime', 'cgroup': None, 'exit_seen': None, 'expiry_bound': None,
            'attempts': 0, 'replacement': None, 'ready': None, 'children_gone': None, 'config_verified': False,
            'final_running_verified': False, 'log_offset': 0, 'attempt_observations': []}
        subject.nodes = {'a1': state}
        return subject, state

    def test_conservative_expiry_and_child_cleanup_precede_same_config_restart(self):
        subject, state = self.subject()
        new = mock.Mock(pid=18)
        new.poll.return_value = None
        with mock.patch.object(supervision.ops, 'proc_identity', side_effect=lambda pid: None if pid==17 else (18,'new')), \
             mock.patch.object(supervision.ops.Acceptance, 'start_process', return_value=new) as start, \
             mock.patch.object(supervision.time, 'monotonic', return_value=15.1), \
             mock.patch.object(supervision, 'live_children', return_value=1) as children:
            subject.poll('a1', state, 10.)
            self.assertEqual(state['expiry_bound'], 15.)
            start.assert_not_called()
            subject.poll('a1', state, 14.9)
            start.assert_not_called()
            subject.poll('a1', state, 15.1)
            start.assert_not_called()
            children.return_value = 0
            subject.poll('a1', state, 15.1)
            start.assert_called_once_with(subject.acceptance, 'a1', [subject.acceptance.binary, 'node', '--config', state['config'], '--drain-timeout', '8s'])
            self.assertTrue(state['config_verified'])
            self.assertEqual(state['ready'], 15.1)
            self.assertTrue(all(call.args[0]=='/ready' for call in subject.acceptance.call.call_args_list))
            subject.stop()
            self.assertTrue(state['final_running_verified'])

    def test_unexpected_original_exit_or_live_identity_cannot_restart(self):
        for code, identity, diagnostic in [(0, None, 'SUPERVISOR_UNEXPECTED_NODE_EXIT'),
                                            (1, (17, 'old'), 'SUPERVISOR_OLD_NODE_STILL_LIVE')]:
            subject, state = self.subject()
            state['process'].poll.return_value = code
            with mock.patch.object(supervision.ops, 'proc_identity', return_value=identity), \
                 mock.patch.object(supervision.ops.Acceptance, 'start_process') as start, \
                 self.assertRaisesRegex(supervision.ops.AcceptanceError, diagnostic):
                subject.poll('a1', state, 10.)
            start.assert_not_called()

    def test_config_drift_cannot_restart(self):
        subject, state = self.subject()
        state['config'].write_text('changed')
        with mock.patch.object(supervision.ops, 'proc_identity', return_value=None), \
             mock.patch.object(supervision.ops.Acceptance, 'start_process') as start, \
             self.assertRaisesRegex(supervision.ops.AcceptanceError, 'SUPERVISOR_CONFIGURATION_CHANGED'):
            subject.poll('a1', state, 10.)
        start.assert_not_called()

    def test_replacement_retries_only_fresh_allowlisted_errors(self):
        for marker, expected in [(b'NATS connection failed\n', True), (b'unknown failure\n', False), (b'', False)]:
            subject, state = self.subject()
            state.update(exit_seen=1., expiry_bound=6.)
            replacement = mock.Mock(pid=18)
            replacement.poll.return_value = 1
            def launch(*args):
                with (subject.acceptance.directory/'a1.log').open('ab') as log:
                    log.write(marker)
                return replacement
            with mock.patch.object(supervision.ops.Acceptance, 'start_process', side_effect=launch), \
                 mock.patch.object(supervision.time, 'monotonic', return_value=6.), \
                 mock.patch.object(supervision, 'live_children', return_value=0):
                if expected:
                    subject.poll('a1', state, 6.)
                    self.assertIsNone(state['replacement'])
                else:
                    with self.assertRaisesRegex(supervision.ops.AcceptanceError, 'SUPERVISOR_REPLACEMENT_EXIT'):
                        subject.poll('a1', state, 6.)
            self.assertEqual(state['attempts'], 1)
            subject.acceptance.call.assert_not_called()

    def test_deadline_prevents_start_and_late_readiness(self):
        subject, state = self.subject()
        state.update(exit_seen=1., expiry_bound=6.)
        with mock.patch.object(supervision.ops.Acceptance, 'start_process') as start, \
             mock.patch.object(supervision.time, 'monotonic', return_value=25.), \
             mock.patch.object(supervision, 'live_children', return_value=0), \
             self.assertRaisesRegex(supervision.ops.AcceptanceError, 'SUPERVISOR_RECOVERY_DEADLINE'):
            subject.poll('a1', state, 25.)
        start.assert_not_called()
        replacement = mock.Mock(pid=18)
        replacement.poll.return_value = None
        state.update(replacement=replacement, attempts=1)
        with mock.patch.object(supervision.time, 'monotonic', side_effect=[24.9, 25.1]), \
             self.assertRaisesRegex(supervision.ops.AcceptanceError, 'SUPERVISOR_RECOVERY_DEADLINE'):
            subject.poll('a1', state, 24.9)
        self.assertIsNone(state['ready'])

    def test_unobserved_original_or_replacement_exit_at_stop_fails(self):
        for replacement in (False, True):
            subject, state = self.subject()
            if replacement:
                process = mock.Mock(pid=18)
                process.poll.return_value = 1
                state.update(exit_seen=1., replacement=process, replacement_identity=(18, 'new'), ready=7.)
            with mock.patch.object(supervision.ops, 'proc_identity', return_value=None):
                subject.stop()
            self.assertEqual(subject.errors, ['SUPERVISOR_FINAL_NODE_NOT_RUNNING'])
            self.assertFalse(state['final_running_verified'])

    def test_recovered_node_exit_during_monitoring_is_not_hidden(self):
        subject, state = self.subject()
        replacement = mock.Mock(pid=18)
        replacement.poll.return_value = 1
        state.update(exit_seen=1., ready=7., replacement=replacement, replacement_identity=(18, 'new'))
        with self.assertRaisesRegex(supervision.ops.AcceptanceError, 'SUPERVISOR_RECOVERED_NODE_EXIT'):
            subject.poll('a1', state, 8.)

    def test_malformed_or_short_lease_is_rejected(self):
        self.assertEqual(supervision.lease_seconds('policy: {lease_duration: 5s}'), 5.)
        for value in ['4s', '61s', '5', '5ms', 'NaNs']:
            with self.assertRaises(Exception):
                supervision.lease_seconds('policy: {lease_duration: '+value+'}')


if __name__ == '__main__':
    unittest.main()

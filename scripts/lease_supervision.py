#!/usr/bin/env python3
"""Bounded fixture supervision for permanently fenced, fully exited nodes."""
import math
import http.client
from pathlib import Path
import re
import threading
import time
import urllib.error

import operational_acceptance as ops

LEASE_EXIT = b'Worker coordination lease lost'
RETRY_EXIT = {b'worker identity is already active or its store is unavailable',
              b'NATS connection failed', b'JetStream unavailable',
              b'cluster metadata unavailable', b'job store unavailable',
              b'KV store configuration unavailable'}
DEADLINE = 25.


def lease_seconds(data):
    import yaml
    value = yaml.safe_load(data)['policy']['lease_duration']
    ops.require(isinstance(value, str) and re.fullmatch(r'[0-9]+(?:\.[0-9]+)?s', value), 'SUPERVISOR_LEASE_FORMAT')
    seconds = float(value[:-1])
    ops.require(math.isfinite(seconds) and 5 <= seconds <= 60, 'SUPERVISOR_LEASE_BOUND')
    return seconds


def exit_marker(path, offset=0):
    # Return only a recognized fixed terminal diagnostic, never private logs.
    with path.open('rb') as data:
        data.seek(0, 2)
        data.seek(max(offset, data.tell()-4096))
        lines = data.read(4096).splitlines()
    last = lines[-1] if lines else b''
    return last if last == LEASE_EXIT or last in RETRY_EXIT else None


def live_children(runtime, group=None):
    """Observe private-runtime descendants, including reparented native children."""
    count = 0
    for path in Path('/proc').iterdir():
        if not path.name.isdecimal():
            continue
        try:
            if (path/'cwd').resolve(strict=True).is_relative_to(runtime):
                identity = ops.proc_identity(int(path.name))
                if identity is not None:
                    count += 1
        except (OSError, RuntimeError):
            continue  # Unrelated UIDs and children which exited during sampling.
    if group is not None:
        # Contained campaigns also verify every delegated descendant cgroup;
        # this catches children whose procfs working directory is unavailable.
        for membership in group.rglob('cgroup.procs'):
            count += len(membership.read_text().split())
    return count


class LeaseSupervisor:
    def __init__(self, acceptance):
        self.acceptance = acceptance
        self.nodes = {}
        self.errors = []
        self.stopping = threading.Event()
        self.thread = None
        for name in ('a1', 'b1'):
            process = acceptance.processes[name]
            config = acceptance.directory/(name+'.yml')
            catalog = acceptance.directory/(name+'-catalog.yml')
            identity = ops.proc_identity(process.pid)
            ops.require(process.poll() is None and identity is not None, 'SUPERVISOR_NODE_NOT_RUNNING')
            self.nodes[name] = {'process': process, 'identity': identity,
                'config': config, 'catalog': catalog, 'hashes': (ops.sha256(config), ops.sha256(catalog)),
                'lease_seconds': lease_seconds(config.read_text()), 'runtime': acceptance.directory/(name+'-runtime'),
                'cgroup': getattr(acceptance, 'containment_roots', {}).get(name),
                'exit_seen': None, 'expiry_bound': None, 'attempts': 0, 'replacement': None, 'ready': None,
                'children_gone': None, 'config_verified': False, 'final_running_verified': False,
                'log_offset': (acceptance.directory/(name+'.log')).stat().st_size,
                'attempt_observations': []}

    def start(self, faulted_at):
        self.started = faulted_at
        self.deadline = self.started + DEADLINE
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def poll(self, name, state, now):
        process = state['process']
        if state['exit_seen'] is None:
            code = process.poll()
            if code is None:
                return
            ops.require(code == 1 and exit_marker(self.acceptance.directory/(name+'.log'), state['log_offset']) == LEASE_EXIT,
                        'SUPERVISOR_UNEXPECTED_NODE_EXIT')
            ops.require(ops.proc_identity(process.pid) != state['identity'], 'SUPERVISOR_OLD_NODE_STILL_LIVE')
            state['exit_seen'] = now
            # Once this exact process has exited it can submit no later worker
            # timestamp. This is a conservative expiry upper bound, not a read
            # of persisted HeartbeatAt. Store CAS/expiry remains final authority.
            state['expiry_bound'] = now + state['lease_seconds']
        ops.require(state['hashes'] == (ops.sha256(state['config']), ops.sha256(state['catalog'])),
                    'SUPERVISOR_CONFIGURATION_CHANGED')
        if state['ready'] is not None:
            ops.require(state['replacement'].poll() is None and
                        ops.proc_identity(state['replacement'].pid) == state['replacement_identity'],
                        'SUPERVISOR_RECOVERED_NODE_EXIT')
            return
        if now < state['expiry_bound']:
            return
        if state['replacement'] is None:
            if live_children(state['runtime'], state['cgroup']):
                return
            if state['children_gone'] is None:
                state['children_gone'] = now
            ops.require(time.monotonic() < self.deadline, 'SUPERVISOR_RECOVERY_DEADLINE')
            ops.require(state['attempts'] < 5, 'SUPERVISOR_START_ATTEMPT_BOUND')
            state['attempts'] += 1
            # Bypass fixture config-generation hooks: launch the byte-identical
            # existing config/catalog, with the same ID and a fresh Node owner.
            state['replacement_log_offset'] = (self.acceptance.directory/(name+'.log')).stat().st_size
            state['replacement'] = ops.Acceptance.start_process(self.acceptance, name,
                [self.acceptance.binary, 'node', '--config', state['config'], '--drain-timeout', '8s'])
            state['restart_started'] = now
            state['attempt_observations'].append({'number': state['attempts'], 'started_seconds': round(now-self.started, 6)})
        replacement = state['replacement']
        code = replacement.poll()
        if code is not None:
            marker = exit_marker(self.acceptance.directory/(name+'.log'), state['replacement_log_offset'])
            state['attempt_observations'][-1].update(exit_code=code, recognized_exit=None if marker is None else marker.decode())
            ops.require(code == 1 and marker in RETRY_EXIT, 'SUPERVISOR_REPLACEMENT_EXIT')
            state['replacement'] = None
            return
        try:
            remaining = self.deadline-time.monotonic()
            ops.require(remaining > 0, 'SUPERVISOR_RECOVERY_DEADLINE')
            code, _ = self.acceptance.call('/ready', node=name, timeout=min(.25, remaining))
        except (OSError, urllib.error.URLError, http.client.HTTPException):
            return
        if code == 200:
            ops.require(time.monotonic() <= self.deadline, 'SUPERVISOR_RECOVERY_DEADLINE')
            state['replacement_identity'] = ops.proc_identity(replacement.pid)
            ops.require(state['replacement_identity'] not in (None, state['identity']), 'SUPERVISOR_FRESH_PROCESS_REQUIRED')
            state['ready'] = time.monotonic()
            state['config_verified'] = state['hashes'] == (ops.sha256(state['config']), ops.sha256(state['catalog']))
            ops.require(state['config_verified'], 'SUPERVISOR_CONFIGURATION_CHANGED')

    def run(self):
        try:
            while not self.stopping.is_set() and time.monotonic() < self.deadline:
                for name, state in self.nodes.items():
                    self.poll(name, state, time.monotonic())
                self.stopping.wait(.05)
        except Exception as error:
            self.errors.append(str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__)

    def stop(self):
        self.stopping.set()
        if self.thread:
            self.thread.join(timeout=2)
            ops.require(not self.thread.is_alive(), 'SUPERVISOR_DID_NOT_STOP')
        for state in self.nodes.values():
            process = state['replacement'] if state['exit_seen'] is not None else state['process']
            identity = state.get('replacement_identity') if state['exit_seen'] is not None else state['identity']
            state['final_running_verified'] = (process is not None and identity is not None and
                process.poll() is None and ops.proc_identity(process.pid) == identity)
            if not state['final_running_verified']:
                self.errors.append('SUPERVISOR_FINAL_NODE_NOT_RUNNING')

    def evidence(self):
        nodes = []
        for name, state in self.nodes.items():
            item = {'node': name, 'fenced_exit_observed': state['exit_seen'] is not None,
                    'restart_attempts': state['attempts'], 'lease_seconds': state['lease_seconds'],
                    'final_running_verified': state['final_running_verified'],
                    'attempts': list(state['attempt_observations'])}
            if state['exit_seen'] is not None:
                item.update({key: None if state.get(field) is None else round(state[field]-self.started, 6)
                             for key, field in (('exit_observed_seconds', 'exit_seen'), ('lease_expiry_upper_bound_seconds', 'expiry_bound'),
                             ('old_children_gone_seconds', 'children_gone'), ('restart_started_seconds', 'restart_started'), ('replacement_ready_seconds', 'ready'))})
                item['same_configuration_verified'] = state['config_verified']
                item['fresh_owner_claimed'] = state['ready'] is not None
            nodes.append(item)
        return {'schema': 1, 'deadline_seconds': DEADLINE, 'expiry_basis': 'observed_exact_parent_exit_plus_configured_lease_upper_bound',
                'original_query_deadlines_unchanged': True, 'query_resubmissions': 0, 'errors': list(self.errors), 'nodes': nodes}


def valid_evidence(value):
    if not isinstance(value, dict) or type(value.get('schema')) is not int or value.get('schema') != 1 or value.get('deadline_seconds') != DEADLINE or value.get('errors') != []:
        return False
    if value.get('expiry_basis') != 'observed_exact_parent_exit_plus_configured_lease_upper_bound' or value.get('original_query_deadlines_unchanged') is not True or type(value.get('query_resubmissions')) is not int or value['query_resubmissions'] != 0:
        return False
    nodes = value.get('nodes', [])
    if not isinstance(nodes, list) or len(nodes) != 2 or not all(isinstance(node, dict) for node in nodes) or {node.get('node') for node in nodes} != {'a1', 'b1'}:
        return False
    for node in nodes:
        if type(node.get('fenced_exit_observed')) is not bool or type(node.get('restart_attempts')) is not int or not 0 <= node['restart_attempts'] <= 5:
            return False
        if type(node.get('lease_seconds')) not in (int, float) or not 5 <= node['lease_seconds'] <= 60:
            return False
        attempts = node.get('attempts')
        if node.get('final_running_verified') is not True or not isinstance(attempts, list) or len(attempts) != node['restart_attempts']:
            return False
        if not node['fenced_exit_observed']:
            if node['restart_attempts'] != 0: return False
            continue
        keys = ('exit_observed_seconds', 'lease_expiry_upper_bound_seconds', 'old_children_gone_seconds', 'restart_started_seconds', 'replacement_ready_seconds')
        if not all(type(node.get(key)) in (int, float) and math.isfinite(node[key]) for key in keys):
            return False
        exited, expiry, children, restart, ready = (node[key] for key in keys)
        if not (0 <= exited <= expiry <= children <= restart <= ready <= DEADLINE and abs(expiry-exited-node['lease_seconds']) <= 0.000002):
            return False
        if node['restart_attempts'] < 1 or node.get('same_configuration_verified') is not True or node.get('fresh_owner_claimed') is not True:
            return False
        previous = children
        for index, attempt in enumerate(attempts):
            if not isinstance(attempt, dict) or type(attempt.get('number')) is not int or attempt['number'] != index+1:
                return False
            started = attempt.get('started_seconds')
            if type(started) not in (int, float) or not math.isfinite(started) or not expiry <= started <= ready or started < previous:
                return False
            previous = started
            if index < len(attempts)-1:
                if type(attempt.get('exit_code')) is not int or attempt['exit_code'] != 1 or attempt.get('recognized_exit') not in {value.decode() for value in RETRY_EXIT}:
                    return False
            elif started != restart or 'exit_code' in attempt or 'recognized_exit' in attempt:
                return False
    return True

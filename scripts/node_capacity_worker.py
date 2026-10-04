#!/usr/bin/env python3
"""Bounded Oracle node profile; private prepared inputs, no source provisioning.

Run on Linux only. The external controller runs queries on a separate host and
creates this profile's stop file after delivery. No telemetry, log or report
contains environment values. Every profile owns one transient service.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import pwd
import re
import signal
import stat
import subprocess
import sys
import time
import uuid

import yaml

import containment_acceptance as identity
import operational_acceptance as ops

MIB = 1 << 20
UNIT_PATTERN = r'kelvo-micro-node-[0-9a-f]{12}'
STOP_SECONDS = 15
RUNTIME_GRACE_SECONDS = 20
INTENT_MAX_AGE_SECONDS = 60
SERVICE_PROPERTIES = ('LoadState', 'Description', 'User', 'ExecStart', 'ControlGroup',
                      'Transient', 'RuntimeMaxUSec', 'TimeoutStopUSec', 'KillMode',
                      'SendSIGKILL', 'Restart')


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as source:
        for block in iter(lambda: source.read(1 << 20), b''):
            value.update(block)
    return value.hexdigest()


def private_json(path):
    info = path.lstat()
    ops.require(path.is_file() and not path.is_symlink() and info.st_uid == os.getuid() and not info.st_mode & 0o077 and info.st_size <= MIB, "PRIVATE_INPUT_REQUIRED")
    return json.loads(path.read_text())


def save(path, data):
    temporary = path.with_suffix('.pending')
    with temporary.open('w') as output:
        json.dump(data, output, indent=2)
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def create_private_json(path, data):
    """An interrupted or reused intent is evidence, never a replaceable file."""
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, 'w') as output:
        json.dump(data, output, sort_keys=True)
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())
    fsync_directory(path.parent)


def safe_path(path):
    # systemctl serializes ExecStart as a tuple, not JSON. Reject ambiguous
    # tokens rather than interpreting shell quoting during ownership checks.
    return isinstance(path, str) and re.fullmatch(r'/[A-Za-z0-9_./-]+', path) is not None


def inside_command(intent):
    return [intent['python'], intent['script'], '--inside', '--directory', intent['directory'],
            '--profile', intent['profile'], '--metrics', intent['metrics'],
            '--runtime', str(intent['runtime']), '--unit', intent['unit'],
            '--expires-at', str(intent['expires_at'])]


def new_intent(args, directory, now):
    expiry = args.expires_at if args.expires_at is not None else now + args.runtime + 60
    intent = {'schema': 1, 'unit': args.unit or 'kelvo-micro-node-' + uuid.uuid4().hex[:12],
              'directory': str(directory), 'profile': args.profile, 'metrics': args.metrics,
              'runtime': args.runtime, 'created_at': now, 'expires_at': expiry,
              'account': pwd.getpwuid(os.getuid()).pw_name, 'uid': os.getuid(),
              'python': os.path.abspath(sys.executable),
              'script': str(Path(__file__).resolve(strict=True)),
              'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
              'token': uuid.uuid4().hex}
    validate_intent(intent, directory / 'profiles' / args.profile, now=now)
    ops.require(expiry - now >= args.runtime + RUNTIME_GRACE_SECONDS + STOP_SECONDS + 5,
                'LAUNCH_EXPIRY_TOO_CLOSE')
    return intent


def validate_intent(intent, profile, *, now=None):
    required = {'schema', 'unit', 'directory', 'profile', 'metrics', 'runtime', 'created_at',
                'expires_at', 'account', 'uid', 'python', 'script', 'boot_id', 'token'}
    ops.require(isinstance(intent, dict) and set(intent) == required, 'LAUNCH_INTENT_INVALID')
    ops.require(type(intent['schema']) is int and intent['schema'] == 1
                and type(intent['runtime']) is int and 60 <= intent['runtime'] <= 1200
                and type(intent['created_at']) is int and type(intent['expires_at']) is int
                and 0 < intent['created_at'] < intent['expires_at'] < 1 << 53
                and type(intent['uid']) is int and intent['uid'] == os.getuid(), 'LAUNCH_INTENT_INVALID')
    for key, pattern in (('unit', UNIT_PATTERN), ('profile', r'[a-z0-9-]{1,48}'),
                         ('account', r'[a-z_][a-z0-9_-]{0,31}'), ('token', r'[0-9a-f]{32}'),
                         ('boot_id', r'[0-9a-f-]{36}')):
        ops.require(isinstance(intent[key], str) and re.fullmatch(pattern, intent[key]), 'LAUNCH_INTENT_INVALID')
    ops.require(all(safe_path(intent[key]) for key in ('directory', 'python', 'script'))
                and intent['metrics'] in ('enabled', 'disabled')
                and Path(intent['directory']).resolve(strict=True) == Path(intent['directory'])
                and profile == Path(intent['directory']) / 'profiles' / intent['profile']
                and intent['account'] == pwd.getpwuid(os.getuid()).pw_name
                and intent['boot_id'] == Path('/proc/sys/kernel/random/boot_id').read_text().strip(), 'LAUNCH_INTENT_INVALID')
    if now is not None:
        ops.require(0 <= now - intent['created_at'] <= INTENT_MAX_AGE_SECONDS, 'LAUNCH_INTENT_STALE')
        ops.require(intent['expires_at'] - now >= intent['runtime'] + RUNTIME_GRACE_SECONDS + STOP_SECONDS,
                    'LAUNCH_EXPIRY_TOO_CLOSE')


def read_intent(profile, *, now=None):
    info = profile.lstat()
    ops.require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077
                and profile.resolve(strict=True) == profile,
                'PRIVATE_PROFILE_REQUIRED')
    path = profile / 'launch-intent.json'
    ops.require(path.lstat().st_nlink == 1, 'PRIVATE_INPUT_REQUIRED')
    intent = private_json(path)
    validate_intent(intent, profile, now=now)
    return intent


def systemd_command(intent):
    return ['sudo', '-n', 'systemd-run', '--unit=' + intent['unit'], '--uid=' + intent['account'],
            '--description=Kelvo capacity ' + intent['token'],
            '--property=Delegate=yes', '--property=MemoryMax=640M', '--property=MemorySwapMax=0',
            '--property=TasksMax=128', '--property=CPUWeight=20', '--property=NoNewPrivileges=yes',
            '--property=CapabilityBoundingSet=', '--property=AmbientCapabilities=',
            '--property=RuntimeMaxSec=' + str(intent['runtime'] + RUNTIME_GRACE_SECONDS),
            '--property=TimeoutStopSec=' + str(STOP_SECONDS), '--property=KillMode=control-group',
            '--property=SendSIGKILL=yes', '--property=Restart=no',
            '--collect', '--wait', '--pipe', *inside_command(intent)]


def parse_service_status(output):
    result = {}
    for line in output.splitlines():
        key, separator, value = line.partition('=')
        ops.require(separator and key in SERVICE_PROPERTIES and key not in result, 'SERVICE_STATUS_INVALID')
        result[key] = value
    ops.require('LoadState' in result, 'SERVICE_STATUS_INVALID')
    if result['LoadState'] != 'not-found':
        ops.require(set(result) == set(SERVICE_PROPERTIES), 'SERVICE_STATUS_INVALID')
    return result


class SystemdService:
    """Small process boundary; controls use a fake manager, not subprocess patches."""
    def status(self, unit):
        result = subprocess.run(['systemctl', 'show', unit + '.service', '--no-pager',
                                 '--property=' + ','.join(SERVICE_PROPERTIES)],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10)
        status = parse_service_status(result.stdout)
        ops.require(result.returncode == 0 or (status['LoadState'] == 'not-found' and result.returncode in (1, 4)),
                    'SERVICE_STATUS_UNAVAILABLE')
        return status

    def group_exists(self, unit):
        return Path('/sys/fs/cgroup/system.slice', unit + '.service').exists()

    def launch(self, intent, log):
        return subprocess.run(systemd_command(intent), stdout=log, stderr=subprocess.STDOUT,
                              timeout=intent['runtime'] + 90).returncode

    def stop(self, unit):
        result = subprocess.run(['sudo', '-n', 'systemctl', 'stop', unit + '.service'],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=20)
        ops.require(result.returncode == 0, 'OWNED_SERVICE_STOP_FAILED')


def require_owned_service(intent, status, group_exists):
    command = inside_command(intent)
    prefix = '{ path=' + command[0] + ' ; argv[]=' + ' '.join(command) + ' ; ignore_errors=no ; '
    expected = {'LoadState': 'loaded', 'Description': 'Kelvo capacity ' + intent['token'],
                'User': intent['account'], 'Transient': 'yes', 'KillMode': 'control-group',
                'SendSIGKILL': 'yes', 'Restart': 'no'}
    ops.require(all(status.get(key) == value for key, value in expected.items())
                and isinstance(status.get('ExecStart'), str) and status['ExecStart'].startswith(prefix)
                and re.fullmatch(r'start_time=[^;{}]* ; stop_time=[^;{}]* ; pid=[0-9]+ ; code=[^;{}]* ; status=[0-9]+(?:/[^;{}]*)? }',
                                 status['ExecStart'][len(prefix):]) is not None,
                'SERVICE_OWNERSHIP_MISMATCH')
    expected_group = '/system.slice/' + intent['unit'] + '.service'
    ops.require(status.get('ControlGroup') == expected_group
                or (status.get('ControlGroup') == '' and not group_exists), 'SERVICE_OWNERSHIP_MISMATCH')
    # The limits are set explicitly by our command; verify their manager values
    # before authorizing cleanup or allowing the inner worker to start.
    ops.require(status.get('RuntimeMaxUSec') == format_systemd_seconds(intent['runtime'] + RUNTIME_GRACE_SECONDS)
                and status.get('TimeoutStopUSec') == format_systemd_seconds(STOP_SECONDS), 'SERVICE_EXPIRY_MISMATCH')


def format_systemd_seconds(seconds):
    minutes, seconds = divmod(seconds, 60)
    return ((str(minutes) + 'min ' if minutes else '') + (str(seconds) + 's' if seconds else '')).strip()


def require_open_launch(profile):
    try:
        (profile / 'launch-closed.json').lstat()
    except FileNotFoundError:
        return
    raise ops.AcceptanceError('LAUNCH_INTENT_CLOSED')


def close_launch(intent):
    """Fence a delayed StartTransientUnit request before claiming absence."""
    profile = Path(intent['directory']) / 'profiles' / intent['profile']
    ops.require(read_intent(profile) == intent, 'LAUNCH_INTENT_MISMATCH')
    expected = {'schema': 1, 'intent_sha256': digest(profile / 'launch-intent.json')}
    path = profile / 'launch-closed.json'
    try:
        create_private_json(path, expected)
    except FileExistsError:
        # A previous interrupted fsync is not evidence of durable cancellation.
        # Revalidate without following symlinks, then fsync both file and parent.
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, 'r') as source:
            info = os.fstat(source.fileno())
            ops.require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid()
                        and not info.st_mode & 0o077 and info.st_nlink == 1 and info.st_size <= MIB,
                        'PRIVATE_CLOSED_INTENT_REQUIRED')
            ops.require(json.load(source) == expected, 'CLOSED_INTENT_MISMATCH')
            os.fsync(source.fileno())
        fsync_directory(profile)


def cleanup_owned_launch(intent, service):
    result = {'owned_service_removed': False, 'owned_cgroup_removed': False, 'launch_closed': False}
    try:
        close_launch(intent)
        result['launch_closed'] = True
    except BaseException as error:
        result['cleanup_failure'] = str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__
    # Even failed cancellation persistence must not skip stopping a live service
    # whose exact identity is known. It can never produce a clean receipt.
    try:
        unit = intent['unit']
        status, exists = service.status(unit), service.group_exists(unit)
        if status['LoadState'] != 'not-found':
            require_owned_service(intent, status, exists)
            service.stop(unit)
            status, exists = service.status(unit), service.group_exists(unit)
        result['owned_service_removed'] = result['launch_closed'] and status['LoadState'] == 'not-found'
        result['owned_cgroup_removed'] = result['launch_closed'] and not exists
    except BaseException as error:
        result.setdefault('cleanup_failure', str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__)
    return result


def cleanup_launch(profile, service=None):
    """Controller recovery, including a launch that never wrote started.json."""
    try:
        return cleanup_owned_launch(read_intent(profile), service or SystemdService())
    except BaseException as error:
        return {'owned_service_removed': False, 'owned_cgroup_removed': False,
                'cleanup_failure': str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__}


def claim_launch(args, profile, service, current_group, now):
    intent = read_intent(profile, now=now)
    require_open_launch(profile)
    ops.require(all(getattr(args, key) == intent[key] for key in ('unit', 'profile', 'metrics', 'runtime', 'expires_at'))
                and intent['script'] == str(Path(__file__).resolve(strict=True))
                and intent['python'] == os.path.abspath(sys.executable), 'LAUNCH_INTENT_MISMATCH')
    require_owned_service(intent, service.status(args.unit), service.group_exists(args.unit))
    ops.require(current_group == '0::/system.slice/' + args.unit + '.service', 'OWNED_SERVICE_REQUIRED')
    create_private_json(profile / 'launch-claimed.json', {'intent_sha256': digest(profile / 'launch-intent.json')})
    require_open_launch(profile)
    return intent


def counters(path):
    return {key: int(value) for key, value in (line.split() for line in path.read_text().splitlines())}


def host_cpu():
    fields = Path('/proc/stat').read_text().splitlines()[0].split()[1:]
    return {'total': sum(int(value) for value in fields[:8]), 'steal': int(fields[7])}


def host_available():
    values = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
    return int(values['MemAvailable'].split()[0]) * 1024


def render_config(template, group, state, scratch, enabled):
    for marker in ('@GROUP@', '@STATE@', '@SCRATCH@', '@METRICS@'):
        ops.require(template.count(marker) == 1, 'CONFIG_TEMPLATE_INCOMPLETE')
    rendered = template.replace('@GROUP@', str(group)).replace('@STATE@', str(state)).replace('@SCRATCH@', str(scratch)).replace('@METRICS@', 'true' if enabled else 'false')
    ops.require(re.search(r'@[A-Z_]+@', rendered) is None, 'CONFIG_TEMPLATE_INCOMPLETE')
    parsed = yaml.safe_load(rendered)
    ops.require(isinstance(parsed, dict) and parsed.get('metrics', {}).get('enabled') is enabled, 'METRICS_MODE_NOT_APPLIED')
    normalized = json.loads(json.dumps(parsed))
    normalized['metrics']['enabled'] = '<compared-mode>'
    normalized['scratch_directory'] = '<profile-scratch>'
    normalized['containment']['root'] = '<profile-group>'
    normalized['containment']['state_directory'] = '<profile-state>'
    return rendered, hashlib.sha256(json.dumps(normalized, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def input_identity(directory):
    names = ['catalog.yml', 'node-template.yml', 'node-environment.json', 'bin/kelvo', 'bin/kelvo-landlock']
    names.extend(str(path.relative_to(directory)) for path in sorted((directory / 'scripts').glob('*.py')))
    files = {name: digest(directory / name) for name in names}
    return {'files': files, 'sha256': hashlib.sha256(json.dumps(files, sort_keys=True, separators=(',', ':')).encode()).hexdigest()}


def nonnegative_integer(value):
    return type(value) is int and value >= 0


class Samples:
    def __init__(self, group, scratch, binary):
        self.group, self.scratch, self.binary = group, scratch, str(binary).encode()
        self.owned = set()
        self.value = {'samples': 0, 'read_errors': [], 'disappeared': 0, 'rss_node_peak_bytes': 0,
                      'rss_workers_peak_bytes': 0, 'rss_combined_peak_bytes': 0, 'max_workers': 0,
                      'memory_current_peak_bytes': 0, 'memory_peak_bytes': 0, 'scratch_peak_bytes': 0,
                      'minimum_host_available_bytes': host_available(), 'maximum_sample_gap_seconds': 0.}
        self.before_cpu = host_cpu()
        self.last = None

    def sample(self):
        value, now = self.value, time.monotonic()
        if self.last is not None:
            value['maximum_sample_gap_seconds'] = max(value['maximum_sample_gap_seconds'], now - self.last)
        self.last = now
        value['samples'] += 1
        try:
            value['memory_current_peak_bytes'] = max(value['memory_current_peak_bytes'], int((self.group / 'memory.current').read_text()))
            value['memory_peak_bytes'] = max(value['memory_peak_bytes'], int((self.group / 'memory.peak').read_text()))
            value['memory_events'] = counters(self.group / 'memory.events')
            value['cpu_stat'] = counters(self.group / 'cpu.stat')
            value['pids_events'] = counters(self.group / 'pids.events')
            value['minimum_host_available_bytes'] = min(value['minimum_host_available_bytes'], host_available())
            pids = set()
            for current, _, _ in os.walk(self.group):
                try:
                    pids.update(int(pid) for pid in (Path(current) / 'cgroup.procs').read_text().split())
                except FileNotFoundError:
                    value['disappeared'] += 1
            node_rss = worker_rss = workers = 0
            for pid in pids:
                try:
                    process = Path('/proc') / str(pid)
                    fields = (process / 'stat').read_text().rsplit(') ', 1)[1].split()
                    if fields[0] in ('Z', 'X', 'x'):
                        continue
                    arguments = (process / 'cmdline').read_bytes().split(b'\0')
                    if len(arguments) < 2 or arguments[0] != self.binary or arguments[1] not in (b'node', b'worker'):
                        continue
                    self.owned.add((pid, fields[19]))
                    ops.require(len(self.owned) <= 10000, 'PROCESS_INVENTORY_BOUND')
                    rss = int(fields[21]) * os.sysconf('SC_PAGE_SIZE')
                    if arguments[1] == b'node':
                        node_rss += rss
                    else:
                        worker_rss += rss
                        workers += 1
                except FileNotFoundError:
                    value['disappeared'] += 1
            value['rss_node_peak_bytes'] = max(value['rss_node_peak_bytes'], node_rss)
            value['rss_workers_peak_bytes'] = max(value['rss_workers_peak_bytes'], worker_rss)
            value['rss_combined_peak_bytes'] = max(value['rss_combined_peak_bytes'], node_rss + worker_rss)
            value['max_workers'] = max(value['max_workers'], workers)
            size = 0
            for current, dirs, files in os.walk(self.scratch, followlinks=False):
                dirs[:] = [name for name in dirs if not (Path(current) / name).is_symlink()]
                for name in files:
                    try:
                        size += (Path(current) / name).lstat().st_size
                    except FileNotFoundError:
                        pass
            value['scratch_peak_bytes'] = max(value['scratch_peak_bytes'], size)
            ops.require(size <= 512 * MIB, 'SCRATCH_BOUND_EXCEEDED')
        except Exception as error:
            value['read_errors'].append(str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__)

    def final(self):
        self.sample()
        after = host_cpu()
        total, steal = after['total'] - self.before_cpu['total'], after['steal'] - self.before_cpu['steal']
        self.value['host_steal_fraction'] = steal / total if total else None
        self.value['observed_worker_identities'] = len(self.owned)
        self.value['live_owned_processes_after_shutdown'] = sum(ops.proc_identity(pid) == (pid, started) for pid, started in self.owned)
        self.value['sampling_scope'] = 'RSS: observed node and worker processes across delegated descendants. Cgroup: node, disposable children and Python observer; excludes source, gateway, NATS and SSH. Shared RSS pages can be counted twice; sampling can miss short peaks.'
        return self.value


def valid_profile(report):
    try:
        resources = report['resources']
        if not all(nonnegative_integer(resources[key]) for key in ('samples', 'rss_node_peak_bytes', 'rss_workers_peak_bytes', 'rss_combined_peak_bytes', 'max_workers', 'scratch_peak_bytes', 'live_owned_processes_after_shutdown')):
            return False
        for category, required in (('memory_events', ('oom', 'oom_kill', 'max')), ('pids_events', ('max',)), ('cpu_stat', ('usage_usec', 'user_usec', 'system_usec'))):
            if not all(nonnegative_integer(resources[category][key]) for key in required):
                return False
        gap, elapsed = resources['maximum_sample_gap_seconds'], report['elapsed_seconds']
        if not (type(gap) in (int, float) and math.isfinite(gap) and 0 <= gap <= 2 and type(elapsed) in (int, float) and math.isfinite(elapsed) and elapsed >= 5):
            return False
        return (report['interrupted'] is False and report.get('failure') is None and type(report['node_exit_code']) is int and report['node_exit_code'] == 0
                and report['forced_node_kill'] is False and report['stop_file_observed'] is True
                and report['config_unchanged'] is True and report['binary_unchanged'] is True
                and report['inputs_unchanged'] is True
                and report['owned_service_removed'] is True and report['owned_cgroup_removed'] is True
                and type(report['service_exit_code']) is int and report['service_exit_code'] == 0
                and type(report['scratch_directories_remaining']) is int and report['scratch_directories_remaining'] == 0
                and type(report['containment_records_remaining']) is int and report['containment_records_remaining'] == 0 and resources['samples'] >= 20
                and resources['rss_node_peak_bytes'] > 0 and resources['rss_workers_peak_bytes'] > 0
                and resources['max_workers'] == 1 and not resources['read_errors']
                and resources['live_owned_processes_after_shutdown'] == 0
                and resources['memory_events']['oom'] == resources['memory_events']['oom_kill'] == 0
                and resources['pids_events']['max'] == 0 and resources['scratch_peak_bytes'] <= 512 * MIB)
    except (KeyError, TypeError):
        return False


def verify_final_inputs(directory, config, config_hash, binary, binary_hash, before_inputs, report):
    checks = [('config_unchanged', lambda: digest(config) == config_hash),
              ('binary_unchanged', lambda: digest(binary) == binary_hash),
              ('inputs_unchanged', lambda: input_identity(directory) == before_inputs)]
    for name, check in checks:
        try:
            report[name] = check()
        except Exception as error:
            report[name] = False
            report.setdefault('identity_verification_errors', []).append({'check': name, 'error': type(error).__name__})
    if not all(report[name] for name, _ in checks):
        report.setdefault('failure', 'INPUT_IDENTITY_VERIFICATION_FAILED')


def finish_observation(process, sampler, profile, report, verify_inputs):
    if process is not None:
        if process.poll() is None:
            process.send_signal(signal.SIGTERM)
            deadline = time.monotonic() + 12
            while process.poll() is None and time.monotonic() < deadline:
                if sampler is not None:
                    sampler.sample()
                time.sleep(.1)
            if process.poll() is None:
                report['forced_node_kill'] = True
                process.kill()
                process.wait(timeout=3)
        report['node_exit_code'] = process.returncode
    if sampler is not None:
        report['resources'] = sampler.final()
        report['scratch_directories_remaining'] = len(list((profile / 'scratch').glob('kelvo-worker-*')))
        report['containment_records_remaining'] = sum(path.name != '.kelvo-containment.lock' for path in (profile / 'containment').iterdir())
        if report['resources'].get('read_errors'):
            report.setdefault('failure', 'RESOURCE_OBSERVATION_FAILED')
            return
        if report['resources']['live_owned_processes_after_shutdown']:
            report.setdefault('failure', 'OWNED_PROCESS_SURVIVED_SHUTDOWN')
            return
    # Identity hashing is outside the active and shutdown sampling interval.
    # A missing final sample or an unreaped node must never reach this stage.
    if process is not None and process.poll() is not None and sampler is not None:
        verify_inputs()


def inside(args, directory, profile):
    group = Path('/sys/fs/cgroup/system.slice') / (args.unit + '.service')
    report = {'interrupted': False, 'metrics_enabled': args.metrics == 'enabled', 'forced_node_kill': False,
              'config_unchanged': False, 'binary_unchanged': False, 'inputs_unchanged': False}
    process = sampler = None
    try:
        claim_launch(args, profile, SystemdService(), Path('/proc/self/cgroup').read_text().strip(), int(time.time()))
        ops.require(identity.zero_capabilities(Path('/proc/self/status').read_text()), 'ZERO_CAPABILITIES_REQUIRED')
        for name, expected in (('memory.max', 640 * MIB), ('memory.swap.max', 0), ('pids.max', 128), ('cpu.weight', 20)):
            ops.require(int((group / name).read_text()) == expected, 'SERVICE_RESOURCE_LIMIT_MISMATCH')
        supervisor = group / 'supervisor'
        supervisor.mkdir()
        (supervisor / 'cgroup.procs').write_text(str(os.getpid()))
        (group / 'cgroup.subtree_control').write_text('+cpu +memory +pids')
        (group / 'jobs').mkdir()
        scratch, state = profile / 'scratch', profile / 'containment'
        scratch.mkdir(mode=0o700)
        state.mkdir(mode=0o700)
        config = profile / 'node.yml'
        before_inputs = input_identity(directory)
        template = (directory / 'node-template.yml').read_text()
        rendered, normalized_hash = render_config(template, group / 'jobs', state, scratch, args.metrics == 'enabled')
        report['normalized_config_sha256'] = normalized_hash
        report['inputs'] = before_inputs
        ops.cf.write(config, rendered)
        config_hash = digest(config)
        env = private_json(directory / 'node-environment.json')
        ops.require(all(key == 'KELVO_NATS_A1' or re.fullmatch(r'KELVO_SOURCE_[A-Z0-9_]{1,100}', key) for key in env), 'UNEXPECTED_ENVIRONMENT_REFERENCE')
        ops.require(all(isinstance(value, str) and '\0' not in value and len(value) <= 65536 for value in env.values()), 'INVALID_ENVIRONMENT_VALUE')
        env.update(PATH='/usr/local/bin:/usr/bin:/bin', HOME=str(profile), TMPDIR=str(scratch), GOMAXPROCS='1', LANG='C.UTF-8')
        binary = directory / 'bin/kelvo'
        binary_hash = digest(binary)
        report['config_sha256'], report['binary_sha256'] = config_hash, binary_hash
        sampler = Samples(group, scratch, binary)
        with (profile / 'node.log').open('w') as log:
            require_open_launch(profile)
            process = subprocess.Popen([str(binary), 'node', '--config', str(config), '--drain-timeout', '8s'], env=env, cwd=profile, stdout=log, stderr=subprocess.STDOUT)
            started = time.monotonic()
            save(profile / 'started.json', {'pid': process.pid, 'unit': args.unit, 'metrics_enabled': args.metrics == 'enabled', 'config_sha256': config_hash, 'binary_sha256': binary_hash})
            while process.poll() is None and not (profile / 'stop').exists() and time.monotonic() - started < args.runtime:
                sampler.sample()
                if sampler.value['read_errors']:
                    raise ops.AcceptanceError('RESOURCE_OBSERVATION_FAILED')
                time.sleep(.1)
            report['stop_file_observed'] = (profile / 'stop').is_file()
            report['elapsed_seconds'] = time.monotonic() - started
    except BaseException as error:
        report['interrupted'] = isinstance(error, (KeyboardInterrupt, InterruptedError))
        report['failure'] = str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__
    finally:
        try:
            finish_observation(process, sampler, profile, report,
                               lambda: verify_final_inputs(directory, config, config_hash, binary, binary_hash, before_inputs, report))
        except BaseException as error:
            report['interrupted'] = report['interrupted'] or isinstance(error, (KeyboardInterrupt, InterruptedError))
            report.setdefault('failure', str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__)
        save(profile / 'inside.json', report)
    return 0 if not report.get('failure') and report.get('node_exit_code') == 0 else 1


def launch_profile(args, directory, service=None, write_report=save):
    service = service or SystemdService()
    profile = directory / 'profiles' / args.profile
    report = {'schema': 1, 'metrics_enabled': args.metrics == 'enabled', 'resource_budget': {'memory_bytes': 640 * MIB, 'swap_bytes': 0, 'tasks': 128, 'cpu_weight': 20}}
    intent = new_intent(args, directory, int(time.time()))
    profile.parent.mkdir(mode=0o700, exist_ok=True)
    info = profile.parent.lstat()
    ops.require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077
                and profile.parent.resolve(strict=True) == profile.parent, 'PRIVATE_PROFILE_REQUIRED')
    profile.mkdir(mode=0o700)  # Existing profiles are never resumed or overwritten.
    intent_persisted = False
    try:
        fsync_directory(profile.parent)
        fsync_directory(directory)
        status = service.status(intent['unit'])
        ops.require(status['LoadState'] == 'not-found' and not service.group_exists(intent['unit']), 'FRESH_SERVICE_REQUIRED')
        create_private_json(profile / 'launch-intent.json', intent)
        intent_persisted = True
        report['launch_intent_sha256'] = digest(profile / 'launch-intent.json')
        validate_intent(intent, profile, now=int(time.time()))
        with (profile / 'service.log').open('w') as log:
            report['service_exit_code'] = service.launch(intent, log)
        if (profile / 'inside.json').is_file():
            report.update(json.loads((profile / 'inside.json').read_text()))
    except BaseException as error:
        report['failure'] = str(error) if isinstance(error, ops.AcceptanceError) else type(error).__name__
        report['interrupted'] = isinstance(error, (KeyboardInterrupt, InterruptedError))
    finally:
        if intent_persisted:
            report.update(cleanup_owned_launch(intent, service))
        report['passed'] = valid_profile(report)
        write_report(profile / 'report.json', report)
    return report


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--profile', required=True)
    parser.add_argument('--metrics', choices=('enabled', 'disabled'), required=True)
    parser.add_argument('--runtime', type=int, default=600)
    parser.add_argument('--inside', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--unit', help='fresh kelvo-micro-node- unit with 12 lowercase hex digits')
    parser.add_argument('--expires-at', type=int, help='absolute Unix deadline reserved for this service, before fixture cleanup')
    args = parser.parse_args()
    ops.require(sys.platform == 'linux' and os.getuid() != 0 and re.fullmatch(r'[a-z0-9-]{1,48}', args.profile) and 60 <= args.runtime <= 1200, 'BOUNDED_NONROOT_LINUX_PROFILE_REQUIRED')
    ops.require(args.unit is None or re.fullmatch(UNIT_PATTERN, args.unit), 'FRESH_SERVICE_REQUIRED')
    directory = args.directory.resolve(strict=True)
    if args.inside:
        ops.require(args.unit is not None and args.expires_at is not None, 'LAUNCH_INTENT_REQUIRED')
        return inside(args, directory, directory / 'profiles' / args.profile)
    report = launch_profile(args, directory)
    print(json.dumps({key: report.get(key) for key in ('passed', 'metrics_enabled', 'failure', 'node_exit_code', 'service_exit_code')}), flush=True)
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())

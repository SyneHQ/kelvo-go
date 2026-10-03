"""Attempt every owned host cleanup despite transport or local receipt failures."""
import copy
import json
import re

REPORT_LIMIT = 16 * 1024
REQUIRED = {
    'oracle': {'owned_workers_removed', 'owned_tunnels_removed', 'owned_tunnel_cgroups_removed',
               'private_key_removed', 'failures'},
    'azure': {'source_user_removed', 'forwarding_key_removed', 'source_data_preserved',
              'brokers_stopped', 'gateway_stopped', 'failures'},
}


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('DUPLICATE_CLEANUP_FIELD')
        result[key] = value
    return result


def cleanup_report(raw, host):
    if not isinstance(raw, (str, bytes)) or len(raw) > REPORT_LIMIT:
        raise ValueError('INVALID_CLEANUP_REPORT')
    if isinstance(raw, str) and len(raw.encode('utf-8')) > REPORT_LIMIT:
        raise ValueError('INVALID_CLEANUP_REPORT')
    value = json.loads(raw, object_pairs_hook=unique_object)
    if (not isinstance(value, dict) or host not in REQUIRED or set(value) != REQUIRED[host]
            or value.get('failures') != []
            or any(not isinstance(key, str) or re.fullmatch(r'[a-z][a-z0-9_]{0,63}', key) is None for key in value)
            or any(flag is not True for key, flag in value.items() if key != 'failures')):
        raise ValueError('INVALID_CLEANUP_REPORT')
    return value


def cleanup_every_host(hosts, invoke_cleanup, invoke_stop_timer, persist):
    """Callbacks take a host key; persist takes (host key, copied outcome).

    Run every remote cleanup before attempting any local persistence. Only a
    zero exit plus a bounded, strictly positive JSON report permits stopping
    that host's expiry timer. No exception in one leg skips another host.
    """
    hosts = tuple(hosts)
    if not hosts or any(not isinstance(host, str) or not host for host in hosts) or len(set(hosts)) != len(hosts):
        raise ValueError('UNIQUE_HOST_KEYS_REQUIRED')
    results = {}
    for host in hosts:
        outcome = {'cleanup_attempted': True, 'cleanup_verified': False,
                   'timer_stop_attempted': False, 'timer_stopped': False,
                   'persistence_attempted': False, 'persisted': False}
        results[host] = outcome
        try:
            completed = invoke_cleanup(host)
            if type(completed.returncode) is not int:
                raise ValueError('INVALID_CLEANUP_EXIT')
            outcome['cleanup_exit_code'] = completed.returncode
            if completed.returncode == 0:
                outcome['cleanup_report'] = cleanup_report(completed.stdout, host)
                outcome['cleanup_verified'] = True
        except BaseException as error:
            outcome['cleanup_error_type'] = type(error).__name__
        if outcome['cleanup_verified']:
            outcome['timer_stop_attempted'] = True
            try:
                completed = invoke_stop_timer(host)
                if type(completed.returncode) is not int:
                    raise ValueError('INVALID_TIMER_EXIT')
                outcome['timer_stop_exit_code'] = completed.returncode
                outcome['timer_stopped'] = completed.returncode == 0
            except BaseException as error:
                outcome['timer_stop_error_type'] = type(error).__name__
    for host in hosts:
        outcome = results[host]
        outcome['persistence_attempted'] = outcome['persisted'] = True
        try:
            persist(host, copy.deepcopy(outcome))
        except BaseException as error:
            outcome['persisted'] = False
            outcome['persistence_error_type'] = type(error).__name__
    return results

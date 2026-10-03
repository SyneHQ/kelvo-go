#!/usr/bin/env python3
"""Run native PostgreSQL/MySQL TLS acceptance on disposable isolated servers."""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
from urllib.parse import quote


PRIVATE_ROOT = Path(__file__).resolve().parents[1] / 'artifacts' / 'relational-private'
DIAGNOSTIC_LIMIT = 8 * 1024 * 1024


class PrivateDiagnostics:
    def __init__(self, run_id, sensitive):
        self.sensitive = sensitive
        self.remaining = DIAGNOSTIC_LIMIT
        self.counter = 0
        # Never follow redirected artifact directories when writing diagnostics.
        for directory in (PRIVATE_ROOT.parent, PRIVATE_ROOT):
            directory.mkdir(mode=0o700, exist_ok=True)
            if directory.is_symlink() or not directory.is_dir():
                raise RuntimeError('Private diagnostic directory is invalid')
        if PRIVATE_ROOT.stat().st_mode & 0o077:
            raise RuntimeError('Private diagnostic directory permissions are invalid')
        self.directory = PRIVATE_ROOT / run_id
        self.directory.mkdir(mode=0o700)

    def record(self, stdout=None, stderr=None):
        if self.remaining <= 0:
            return
        chunks = []
        for value in (stdout, stderr):
            if value:
                chunks.append(value.decode('utf-8', errors='replace') if isinstance(value, bytes) else str(value))
        value = '\n'.join(chunks)
        for secret in sorted(set(self.sensitive), key=len, reverse=True):
            if secret:
                value = value.replace(secret, '[REDACTED]')
        encoded = value.encode('utf-8')[:self.remaining]
        if not encoded:
            return
        self.counter += 1
        path = self.directory / ('output-%03d.log' % self.counter)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, 'wb') as output:
            output.write(encoded)
        self.remaining -= len(encoded)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--go', default='go')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if args.output and (args.output.exists() or args.output.is_symlink()):
        parser.error('--output must be a new file; refusing existing evidence')
    root = Path(__file__).resolve().parents[1]
    docker = ['sudo', '-n', 'docker']
    suffix = secrets.token_hex(5)
    network = 'kelvo-relational-' + suffix
    names = {'postgres': network + '-pg', 'mysql': network + '-mysql'}
    passwords = {name: secrets.token_hex(24) for name in ['admin', 'reader', 'wrong']}
    containers = []
    network_attempted = False
    report = {'schema_version': 2, 'passed': False, 'live_tests_passed': False,
              'worker_ipc_live_tested': False, 'host_trust_modified': False}
    failed = False
    cleanup_failures = []
    sensitive = list(passwords.values())
    diagnostics = None
    stage = 'private_diagnostics'

    def run(argv, *, data=None, timeout=30, check=True):
        try:
            result = subprocess.run(argv, input=data, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired as error:
            if diagnostics:
                diagnostics.record(error.stdout, error.stderr)
            raise RuntimeError('Fixture command timed out') from None
        if result.returncode and diagnostics:
            diagnostics.record(result.stdout, result.stderr)
        if check and result.returncode:
            raise RuntimeError('Fixture command failed')
        return result

    def command(*argv, **kw):
        return run(docker + list(argv), **kw)

    try:
        diagnostics = PrivateDiagnostics(suffix, sensitive)
        stage = 'network_setup'
        network_attempted = True
        command('network', 'create', '--internal', network)
        net = json.loads(command('network', 'inspect', network).stdout)[0]
        subnet = ipaddress.ip_network(net['IPAM']['Config'][0]['Subnet'])
        addresses = {'postgres': str(subnet.network_address + 10), 'mysql': str(subnet.network_address + 11)}
        with tempfile.TemporaryDirectory(prefix=network+'-') as temp:
            work = Path(temp)
            stage = 'certificates'
            cert = work / 'certs'
            cert.mkdir(mode=0o755)
            run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=Kelvo fixture CA','-keyout',str(work/'ca.key'),'-out',str(cert/'ca.crt')])
            for kind in names:
                run(['openssl','req','-newkey','rsa:2048','-nodes','-subj','/CN=Kelvo fixture server','-keyout',str(cert/(kind+'.key')),'-out',str(work/(kind+'.csr'))])
                ext = work / (kind+'.ext')
                ext.write_text('subjectAltName=IP:'+addresses[kind]+'\nextendedKeyUsage=serverAuth\n')
                run(['openssl','x509','-req','-days','1','-in',str(work/(kind+'.csr')),'-CA',str(cert/'ca.crt'),'-CAkey',str(work/'ca.key'),'-CAcreateserial','-extfile',str(ext),'-out',str(cert/(kind+'.crt'))])
            (cert/'postgres.key').chmod(0o600)
            run(['sudo','-n','chown','999:999',str(cert/'postgres.key')])
            (cert/'mysql.key').chmod(0o644)
            env = work / 'database.env'
            env.write_text('POSTGRES_PASSWORD='+passwords['admin']+'\nPOSTGRES_DB=kelvo_native_test\nMYSQL_ROOT_PASSWORD='+passwords['admin']+'\nMYSQL_ROOT_HOST=%\nMYSQL_DATABASE=kelvo_native_test\n')
            env.chmod(0o600)
            stage = 'source_setup'
            common = ['--network',network,'--memory','1g','--memory-swap','1g','--cpus','1','--pids-limit','256','--security-opt','no-new-privileges:true','--env-file',str(env),'--mount','type=bind,src='+str(cert)+',dst=/certs,readonly']
            containers.append(names['postgres'])
            command('create','--pull=never','--name',names['postgres'],'--ip',addresses['postgres'],*common,'postgres:17.6','-c','ssl=on','-c','ssl_cert_file=/certs/postgres.crt','-c','ssl_key_file=/certs/postgres.key','-c','ssl_ca_file=/certs/ca.crt')
            command('start',names['postgres'])
            containers.append(names['mysql'])
            command('create','--pull=never','--name',names['mysql'],'--ip',addresses['mysql'],*common,'mysql:8.4','--require-secure-transport=ON','--ssl-ca=/certs/ca.crt','--ssl-cert=/certs/mysql.crt','--ssl-key=/certs/mysql.key','--innodb-buffer-pool-size=128M')
            command('start',names['mysql'])
            auth = work / 'mysql.cnf'
            auth.write_text('[client]\nuser=root\npassword='+passwords['admin']+'\n')
            auth.chmod(0o600)
            command('cp',str(auth),names['mysql']+':/tmp/kelvo-fixture.cnf')
            def mysql(sql):
                return command('exec','-i',names['mysql'],'mysql','--defaults-extra-file=/tmp/kelvo-fixture.cnf','--batch','--silent',data=sql.encode(),check=False,timeout=15)
            deadline = time.monotonic()+120
            while True:
                pg = command('exec',names['postgres'],'pg_isready','-U','postgres',check=False)
                my = mysql('SELECT 1;')
                if pg.returncode == 0 and my.returncode == 0:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError('TLS database fixtures did not become ready')
                time.sleep(1)
            pgsql = "CREATE ROLE kelvo_reader LOGIN PASSWORD '"+passwords['reader']+"';\n"
            command('exec','-i',names['postgres'],'psql','-v','ON_ERROR_STOP=1','-U','postgres','-d','kelvo_native_test',data=pgsql.encode())
            my_sql = "CREATE USER 'kelvo_reader'@'%' IDENTIFIED BY '"+passwords['reader']+"' REQUIRE SSL; GRANT SELECT ON kelvo_native_test.* TO 'kelvo_reader'@'%';"
            if mysql(my_sql).returncode:
                raise RuntimeError('MySQL fixture reader could not be created')
            pgbase = '@'+addresses['postgres']+':5432/kelvo_native_test?sslmode=verify-full'
            mybase = '@tcp('+addresses['mysql']+':3306)/kelvo_native_test?tls=true&parseTime=true&loc=UTC&time_zone=%27%2B00%3A00%27'
            environment = {key:value for key,value in os.environ.items() if not key.startswith('PG')}
            environment.update({'GOMAXPROCS':'2','SSL_CERT_FILE':str(cert/'ca.crt'),
                'KELVO_TEST_POSTGRES_ADMIN_DSN':'postgres://postgres:'+quote(passwords['admin'])+pgbase,
                'KELVO_TEST_POSTGRES_READER_DSN':'postgres://kelvo_reader:'+quote(passwords['reader'])+pgbase,
                'KELVO_TEST_POSTGRES_BAD_DSN':'postgres://kelvo_reader:'+quote(passwords['wrong'])+pgbase,
                'KELVO_TEST_MYSQL_ADMIN_DSN':'root:'+passwords['admin']+mybase,
                'KELVO_TEST_MYSQL_READER_DSN':'kelvo_reader:'+passwords['reader']+mybase,
                'KELVO_TEST_MYSQL_BAD_DSN':'kelvo_reader:'+passwords['wrong']+mybase})
            sensitive.extend(value for key, value in environment.items() if key.startswith('KELVO_TEST_') and key.endswith('_DSN'))
            stage = 'source_tests'
            try:
                tested = subprocess.run([args.go,'test','-p','2','-count=1','-json','-timeout','120s','-run','Test(Postgres|MySQL)LiveNativeTLS','./internal/sources/sqlnative'],cwd=root,env=environment,capture_output=True,text=True,timeout=150)
            except subprocess.TimeoutExpired as error:
                diagnostics.record(error.stdout, error.stderr)
                raise RuntimeError('Relational TLS tests timed out') from None
            diagnostics.record(tested.stdout, tested.stderr)
            required = {root_name + suffix for root_name in ('TestPostgresLiveNativeTLS', 'TestMySQLLiveNativeTLS')
                        for suffix in ('', '/typed_access_and_refresh', '/typed_access_and_refresh/authentication', '/typed_access_and_refresh/permission')}
            passed, rejected = set(), set()
            package_passed = False
            package = 'github.com/SYNEHQ/kelvo-go/internal/sources/sqlnative'
            for line in tested.stdout.splitlines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(event, dict) or event.get('Package') != package:
                    continue
                name, action = event.get('Test', ''), event.get('Action')
                if name in required and action == 'pass':
                    passed.add(name)
                if isinstance(name, str) and name.split('/', 1)[0] in ('TestPostgresLiveNativeTLS', 'TestMySQLLiveNativeTLS') and action in ('fail', 'skip'):
                    rejected.add(name)
                if not name and action == 'pass':
                    package_passed = True
            report['missing_tests'] = sorted(required - passed)
            report['failed_tests'] = sorted({name if name in required else name.split('/', 1)[0] for name in rejected})
            if tested.returncode or not package_passed or rejected or passed != required:
                # Driver diagnostics can name private users/servers; public evidence
                # records named pass proof, never raw subprocess output.
                raise RuntimeError('Relational TLS acceptance failed or required access tests did not pass')
            report = {'schema_version': 2, 'passed': False, 'live_tests_passed': True,
                'scope': 'Disposable PostgreSQL/MySQL TLS fixtures: native driver and in-process Manager refresh; isolated worker IPC is not live-tested here',
                'postgres_image':'postgres:17.6','mysql_image':'mysql:8.4','published_ports':False,'internal_network':True,
                'assertions':['verified TLS','database read-only credentials','PostgreSQL read-only transaction','native prepared parameters','integer widths and extrema','exact decimal values','NULL values','microsecond timestamp wall time and UTC instant','UUID/JSON source metadata','MySQL BIT bytes','row limit','query cancellation','unconstrained PostgreSQL NUMERIC rejected',
                    'wrong passwords produce UNAUTHENTICATED', 'revoked SELECT produces PERMISSION_DENIED',
                    'native public errors exclude SQL, server, username and password details',
                    'authentication and revoked-grant refresh failures preserve snapshot generation, digest, rows and freshness'],
                'required_tests': sorted(required), 'passed_tests': sorted(passed), 'missing_tests': [], 'failed_tests': [],
                'worker_ipc_live_tested': False, 'host_trust_modified': False}
            stage = 'fixture_metadata'
            for kind,name in names.items():
                detail = json.loads(command('inspect',name).stdout)[0]
                report[kind+'_image_id']=detail['Image']
    except Exception:
        # Exceptions may carry command arguments, DSNs or driver output. Keep
        # failure evidence static and never print the exception or traceback.
        failed = True
        report['failure'] = 'Relational TLS acceptance or fixture setup failed'
        report['failure_stage'] = stage
    finally:
        # Track attempted creation too: a timeout can leave a resource behind.
        # Each cleanup is independent so one failure cannot skip the others.
        for name in reversed(containers):
            try:
                if command('rm', '-f', '-v', name, check=False).returncode:
                    cleanup_failures.append('container removal failed')
            except Exception:
                cleanup_failures.append('container removal failed')
        if network_attempted:
            try:
                if command('network', 'rm', network, check=False).returncode:
                    cleanup_failures.append('network removal failed')
            except Exception:
                cleanup_failures.append('network removal failed')
    if cleanup_failures and not failed:
        report['failure_stage'] = 'cleanup'
    report['diagnostics_retained'] = bool(diagnostics and diagnostics.counter)
    report['cleanup_passed'] = not cleanup_failures
    report['cleanup_failures'] = cleanup_failures
    report['passed'] = bool(report['live_tests_passed'] and not failed and not cleanup_failures)
    encoded = json.dumps(report, indent=2, sort_keys=True) + '\n'
    if args.output:
        try:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            # Exclusive creation also closes the race after the initial check.
            with args.output.open('x') as output:
                output.write(encoded)
        except Exception:
            report['passed'] = False
            report['failure'] = 'Could not write new acceptance evidence file'
            encoded = json.dumps(report, indent=2, sort_keys=True) + '\n'
    print(encoded, end='')
    return 0 if report['passed'] else 1

if __name__ == '__main__':
    sys.exit(main())

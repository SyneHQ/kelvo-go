#!/usr/bin/env python3
"""Run native PostgreSQL/MySQL TLS acceptance on disposable isolated servers."""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
from urllib.parse import quote


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--go', default='go')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    docker = ['sudo', '-n', 'docker']
    suffix = secrets.token_hex(5)
    network = 'kelvo-relational-' + suffix
    names = {'postgres': network + '-pg', 'mysql': network + '-mysql'}
    passwords = {name: secrets.token_hex(24) for name in ['admin', 'reader']}
    containers = []
    network_created = False

    def run(argv, *, data=None, timeout=30, check=True):
        result = subprocess.run(argv, input=data, capture_output=True, timeout=timeout)
        if check and result.returncode:
            raise RuntimeError('Fixture command failed: ' + argv[0])
        return result

    def command(*argv, **kw):
        return run(docker + list(argv), **kw)

    try:
        command('network', 'create', '--internal', network)
        network_created = True
        net = json.loads(command('network', 'inspect', network).stdout)[0]
        subnet = ipaddress.ip_network(net['IPAM']['Config'][0]['Subnet'])
        addresses = {'postgres': str(subnet.network_address + 10), 'mysql': str(subnet.network_address + 11)}
        with tempfile.TemporaryDirectory(prefix=network+'-') as temp:
            work = Path(temp)
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
            common = ['--network',network,'--memory','1g','--memory-swap','1g','--cpus','1','--pids-limit','256','--security-opt','no-new-privileges:true','--env-file',str(env),'--mount','type=bind,src='+str(cert)+',dst=/certs,readonly']
            command('create','--pull=never','--name',names['postgres'],'--ip',addresses['postgres'],*common,'postgres:17.6','-c','ssl=on','-c','ssl_cert_file=/certs/postgres.crt','-c','ssl_key_file=/certs/postgres.key','-c','ssl_ca_file=/certs/ca.crt')
            containers.append(names['postgres'])
            command('start',names['postgres'])
            command('create','--pull=never','--name',names['mysql'],'--ip',addresses['mysql'],*common,'mysql:8.4','--require-secure-transport=ON','--ssl-ca=/certs/ca.crt','--ssl-cert=/certs/mysql.crt','--ssl-key=/certs/mysql.key','--innodb-buffer-pool-size=128M')
            containers.append(names['mysql'])
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
                'KELVO_TEST_MYSQL_ADMIN_DSN':'root:'+passwords['admin']+mybase,
                'KELVO_TEST_MYSQL_READER_DSN':'kelvo_reader:'+passwords['reader']+mybase})
            tested = subprocess.run([args.go,'test','-p','2','-count=1','-v','-timeout','90s','-run','Test(Postgres|MySQL)LiveNativeTLS','./internal/sources/sqlnative'],cwd=root,env=environment,capture_output=True,text=True,timeout=120)
            output = tested.stdout+tested.stderr
            for password in passwords.values():
                output = output.replace(password,'<redacted>')
            if tested.returncode:
                raise RuntimeError('Relational TLS acceptance failed:\n'+output[-8000:])
            report = {'postgres_image':'postgres:17.6','mysql_image':'mysql:8.4','published_ports':False,'internal_network':True,
                'assertions':['verified TLS','database read-only credentials','PostgreSQL read-only transaction','native prepared parameters','integer widths and extrema','exact decimal values','NULL values','microsecond timestamp wall time and UTC instant','UUID/JSON source metadata','MySQL BIT bytes','row limit','query cancellation','unconstrained PostgreSQL NUMERIC rejected'],
                'test_output':output.strip()}
            for kind,name in names.items():
                detail = json.loads(command('inspect',name).stdout)[0]
                report[kind+'_image_id']=detail['Image']
            encoded = json.dumps(report,indent=2,sort_keys=True)+'\n'
            if args.output:
                args.output.parent.mkdir(parents=True,exist_ok=True)
                args.output.write_text(encoded)
            print(encoded,end='')
    finally:
        for name in reversed(containers):
            command('rm','-f','-v',name,check=False)
        if network_created:
            command('network','rm',network,check=False)

if __name__ == '__main__':
    main()

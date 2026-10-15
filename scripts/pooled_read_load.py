#!/usr/bin/env python3
"""Production-like load test for Kelvo's pooled read workers. Linux, non-root.

One run is one node size (--profile) and one scenario file. It
  - fills a disposable PostgreSQL with 20 customer databases and roles,
  - puts a latency proxy in front of it (80 ms RTT, no privileges needed),
  - starts a single-node TLS JetStream broker,
  - runs the real gateway, node, contained read workers and a credential resolver
    stub (300 ms per resolve) inside a delegated, bounded systemd service,
  - drives it from 50 virtual users with benchmarks/pooled-read-load/loadgen,
    outside the service, through the Go client's single-call read,
  - writes report.json: latency per workload class, errors by code, node CPU and
    RSS, worker counts, spawns and evictions, cache hit rate, source queries per
    dashboard refresh, resolver calls.

It installs nothing and downloads only the checksum-pinned NATS release. The
caller supplies prebuilt binaries and a running PostgreSQL container.
"""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import socket
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
PROFILES = {
    # CPUQuota and MemoryMax are the cgroup limits the node runs under.
    "2c1g": {"cpus": 2, "memory": "1G", "scenario": "2c1g.yaml"},
    "4c8g": {"cpus": 4, "memory": "8G", "scenario": "4c8g.yaml"},
}
NATS_VERSION = "2.15.0"
NATS_SHA256 = "5d2c51caca950333aba84911df7d377f826f3a59ec36061c6539105084f65c92"
CUSTOMERS = 20


def sh(argv, **kw):
    return subprocess.run(argv, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kw)


def inside(args):
    """Runs in the delegated service: own the cgroup tree, run the node until stopped."""
    out = Path(args.output)
    relative = Path("/proc/self/cgroup").read_text().strip().split(":", 2)[2]
    if relative != "/system.slice/" + args.unit + ".service" or os.geteuid() == 0:
        raise SystemExit("runner does not own the expected non-root service")
    root = Path("/sys/fs/cgroup") / relative.lstrip("/")
    supervisor, jobs, state = root / "supervisor", root / "jobs", out / "state"
    supervisor.mkdir(mode=0o755)
    (supervisor / "cgroup.procs").write_text(str(os.getpid()))
    (root / "cgroup.subtree_control").write_text("+cpu +memory +pids")
    jobs.mkdir(mode=0o755)
    state.mkdir(mode=0o700)
    env = {k: v for k, v in os.environ.items() if k in ("PATH", "HOME", "LANG", "LC_ALL", "TMPDIR")}
    env.update(json.loads((out / "node-env.json").read_text()))
    env.update(KELVO_LOAD_CGROUP_ROOT=str(jobs), KELVO_LOAD_CGROUP_STATE=str(state))
    with (out / "node.test.log").open("wb") as log:
        result = subprocess.run([str(out / "cluster.test"), "-test.v", "-test.run=^TestPooledReadLoadNode$", "-test.timeout=45m"],
                                env=env, stdout=log, stderr=subprocess.STDOUT)
    (out / "inside.json").write_text(json.dumps({"exit": result.returncode, "remaining_job_groups": sum(p.is_dir() for p in jobs.iterdir())}))
    return result.returncode


def fetch_nats(dest):
    import hashlib, io, tarfile, urllib.request
    url = f"https://github.com/nats-io/nats-server/releases/download/v{NATS_VERSION}/nats-server-v{NATS_VERSION}-linux-amd64.tar.gz"
    with urllib.request.urlopen(url, timeout=60) as response:
        archive = response.read((64 << 20) + 1)
    if len(archive) > 64 << 20 or hashlib.sha256(archive).hexdigest() != NATS_SHA256:
        raise SystemExit("NATS release checksum mismatch")
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
        member = bundle.getmember(f"nats-server-v{NATS_VERSION}-linux-amd64/nats-server")
        dest.write_bytes(bundle.extractfile(member).read())
    dest.chmod(0o700)


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(predicate, what, timeout=60):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        value = predicate()
        if value:
            return value
        time.sleep(0.2)
    raise SystemExit("timed out waiting for " + what)


def cpu_times():
    fields = [int(x) for x in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
    return sum(fields), fields[3] + fields[4]  # total, idle+iowait


def set_override(tree, dotted, value):
    keys = dotted.split(".")
    for key in keys[:-1]:
        tree = tree.setdefault(key, {})
    tree[keys[-1]] = value


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--profile", choices=PROFILES, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new private directory")
    parser.add_argument("--duration", type=int, default=300, help="load seconds")
    parser.add_argument("--label", default="")
    parser.add_argument("--postgres-container", default="kelvo-load-postgres")
    parser.add_argument("--postgres-port", type=int, default=55433)
    parser.add_argument("--postgres-password-file", type=Path)
    parser.add_argument("--rtt-ms", type=int, default=80)
    parser.add_argument("--resolve-ms", type=int, default=300)
    parser.add_argument("--kelvo", type=Path, help="kelvo binary (one-shot children)")
    parser.add_argument("--launcher", type=Path)
    parser.add_argument("--adapter", type=Path)
    parser.add_argument("--cluster-test", type=Path, help="go test -c ./internal/cluster")
    parser.add_argument("--loadgen", type=Path)
    parser.add_argument("--latproxy", type=Path)
    parser.add_argument("--nats-server", type=Path, help="default: download the pinned release")
    parser.add_argument("--set", action="append", default=[], metavar="KEY=VALUE", help="override a scenario key, e.g. warm_pool.max_workers=20")
    parser.add_argument("--loadgen-arg", action="append", default=[], help="extra loadgen flag, e.g. --loadgen-arg=-explorers=10")
    parser.add_argument("--inside", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--unit", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.inside:
        return inside(args)
    if sys.platform != "linux" or os.geteuid() == 0:
        parser.error("run on Linux as a non-root account")
    import yaml
    os.umask(0o077)
    out = args.output.resolve()
    out.mkdir(mode=0o700)
    profile = PROFILES[args.profile]
    password = args.postgres_password_file.read_text().strip()
    pg = ["docker", "exec", "-i", "-e", "PGPASSWORD=" + password, args.postgres_container, "psql", "-h", "127.0.0.1", "-U", "postgres", "-X", "-q", "-v", "ON_ERROR_STOP=1", "-At"]
    started = []

    def run_psql(sql, db="postgres"):
        sh(pg + ["-d", db], input=sql.encode())

    report = {"schema": 1, "profile": args.profile, "label": args.label, "passed": False,
              "runner": {"cpus": os.cpu_count(), "memory_kb": int(re.search(r"MemTotal:\s+(\d+)", Path("/proc/meminfo").read_text())[1])},
              "conditions": {"rtt_ms_to_source": args.rtt_ms, "resolve_ms": args.resolve_ms, "customers": CUSTOMERS, "duration_s": args.duration,
                             "cgroup": {"cpus": profile["cpus"], "memory": profile["memory"]}}}
    try:
        # 1. Source: 20 customer databases, each with its own role.
        run_psql("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
        for n in range(CUSTOMERS):
            name = "cust%02d" % n
            sql = f"CREATE ROLE {name} LOGIN PASSWORD '{password}';\nCREATE DATABASE {name} OWNER {name};\n"
            run_psql(sql)
            body = ["SET ROLE %s;" % name]
            for t in range(8):
                body.append(f"CREATE TABLE public.explorer_t{t} (id bigint PRIMARY KEY, name text NOT NULL, amount numeric(12,2), created_at timestamptz NOT NULL DEFAULT now());"
                            f"CREATE INDEX ON public.explorer_t{t} (created_at);"
                            f"INSERT INTO public.explorer_t{t} (id, name, amount) SELECT g, 'row ' || g, g * 1.25 FROM generate_series(1, 500) g;")
            for p in range(10):
                body.append(f"CREATE TABLE public.dash_p{p} (id bigint PRIMARY KEY, region text NOT NULL, amount numeric(12,2));"
                            f"INSERT INTO public.dash_p{p} SELECT g, 'r' || (g % 8), (g % 97) * 1.5 FROM generate_series(1, 5000) g;")
            run_psql("\n".join(body), db=name)
        print("source ready: %d databases" % CUSTOMERS, flush=True)

        # 2. Latency proxy in front of the source.
        ready = out / "proxy.addr"
        proxy_log = (out / "latproxy.log").open("wb")
        proxy = subprocess.Popen([str(args.latproxy), "-listen", "127.0.0.1:0", "-upstream", "127.0.0.1:%d" % args.postgres_port, "-one-way", "%dms" % (args.rtt_ms // 2),
                                  "-ready-file", str(ready), "-stats-file", str(out / "latproxy.stats.json")], stdout=proxy_log, stderr=proxy_log)
        started.append(proxy)
        proxy_port = int(wait_for(lambda: ready.exists() and ready.read_text().split(":")[-1], "latency proxy"))

        # 3. Broker: one JetStream node over TLS, as production but not replicated.
        certs = out / "certs"
        certs.mkdir()
        sh(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(certs / "ca.key"), "-out", str(certs / "ca.pem"), "-days", "2", "-subj", "/CN=Kelvo load CA",
            "-addext", "basicConstraints=critical,CA:true", "-addext", "keyUsage=critical,keyCertSign,cRLSign"])
        (certs / "broker.ext").write_text("subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
        sh(["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout", str(certs / "broker.key"), "-out", str(certs / "broker.csr"), "-subj", "/CN=broker"])
        sh(["openssl", "x509", "-req", "-in", str(certs / "broker.csr"), "-CA", str(certs / "ca.pem"), "-CAkey", str(certs / "ca.key"), "-CAcreateserial", "-out", str(certs / "broker.pem"),
            "-days", "2", "-extfile", str(certs / "broker.ext")])
        nats_bin = args.nats_server or out / "nats-server"
        if not args.nats_server:
            fetch_nats(nats_bin)
        nats_port = free_port()
        users = {role: secrets.token_hex(24) for role in ("admin", "worker", "gateway")}
        nats_conf = {"server_name": "kelvo-load", "listen": "127.0.0.1:%d" % nats_port, "max_payload": 2 << 20,
                     "tls": {"cert_file": str(certs / "broker.pem"), "key_file": str(certs / "broker.key"), "min_version": "1.3"},
                     "jetstream": {"store_dir": str(out / "nats-state"), "max_file_store": 2 << 30, "max_memory_store": 256 << 20},
                     "accounts": {"a": {"jetstream": {"max_memory": 256 << 20, "max_file": 1 << 30, "max_streams": 32, "max_consumers": 32},
                                        "users": [{"user": u, "password": p} for u, p in users.items()]}}}
        (out / "nats.conf").write_text(json.dumps(nats_conf))
        nats_log = (out / "nats.log").open("wb")
        nats = subprocess.Popen([str(nats_bin), "-c", str(out / "nats.conf")], stdout=nats_log, stderr=nats_log)
        started.append(nats)
        wait_for(lambda: socket.socket().connect_ex(("127.0.0.1", nats_port)) == 0, "broker")
        time.sleep(2)

        # 4. Scenario: the Kelvo settings under test.
        scenario = yaml.safe_load((ROOT / "benchmarks/pooled-read-load/scenarios" / profile["scenario"]).read_text())
        scenario["source"].update(host="127.0.0.1", port=proxy_port, password=password, customers=CUSTOMERS)
        scenario["resolver"]["delay"] = "%dms" % args.resolve_ms
        for item in args.set:
            key, value = item.split("=", 1)
            set_override(scenario, key, yaml.safe_load(value))
        (out / "scenario.yaml").write_text(yaml.safe_dump(scenario))
        public = json.loads(json.dumps(scenario))
        public["source"]["password"] = "<redacted>"
        report["scenario"] = public
        shutil_bins = {name: Path(getattr(args, name)).resolve() for name in ("kelvo", "launcher", "adapter", "cluster_test", "loadgen")}
        for name, path in shutil_bins.items():
            if not path.is_file():
                parser.error("missing --" + name.replace("_", "-"))
        import shutil
        for name, dest in (("kelvo", "kelvo"), ("launcher", "kelvo-landlock"), ("adapter", "kelvo-adapter-go"), ("cluster_test", "cluster.test")):
            shutil.copyfile(shutil_bins[name], out / dest)
            (out / dest).chmod(0o555)
        env = {"KELVO_LOAD_SCENARIO": str(out / "scenario.yaml"), "KELVO_LOAD_OUT": str(out), "KELVO_LOAD_BINARY": str(out / "kelvo"), "KELVO_LOAD_SANDBOX": str(out / "kelvo-landlock"),
               "KELVO_LOAD_ADAPTER_BINARY": str(out / "kelvo-adapter-go"), "KELVO_LOAD_NATS_URL": "tls://127.0.0.1:%d" % nats_port, "KELVO_LOAD_NATS_CA_FILE": str(certs / "ca.pem")}
        for role, prefix in (("admin", "NATS"), ("worker", "NATS_WORKER"), ("gateway", "NATS_GATEWAY")):
            env["KELVO_LOAD_%s_USER" % prefix], env["KELVO_LOAD_%s_PASSWORD" % prefix] = role, users[role]
        (out / "node-env.json").write_text(json.dumps(env))

        # 5. The node, in a delegated service with production-like limits.
        unit = "kelvo-pooled-load-" + secrets.token_hex(6)
        limits = {"MemoryMax": profile["memory"], "MemorySwapMax": "0", "CPUQuota": "%d%%" % (profile["cpus"] * 100), "TasksMax": "4096",
                  "RuntimeMaxSec": str(args.duration + 900), "KillMode": "control-group"}
        report["resource_limits"] = limits
        command = ["sudo", "-n", "systemd-run", "--unit=" + unit, "--uid=" + str(os.geteuid()), "--property=Delegate=yes", *["--property=%s=%s" % kv for kv in limits.items()],
                   "--property=NoNewPrivileges=yes", "--property=CapabilityBoundingSet=", "--property=AmbientCapabilities=", "--collect", "--wait", "--pipe",
                   sys.executable, "-B", str(Path(__file__).resolve()), "--inside", "--profile", args.profile, "--unit", unit, "--output", str(out)]
        service_log = (out / "service.log").open("wb")
        service = subprocess.Popen(command, stdout=service_log, stderr=subprocess.STDOUT)
        started.append(service)
        info = wait_for(lambda: (out / "node.json").exists() or service.poll() is not None or None, "node start", timeout=300)
        if not (out / "node.json").exists():
            raise SystemExit("node did not start; see node.test.log")
        print("node ready", flush=True)

        # 6. Load, with runner-wide samplers beside it.
        samples = {"runner_cpu_busy_pct": [], "source_backends": []}
        stop_sampling = threading.Event()

        def sample_runner():
            total, idle = cpu_times()
            while not stop_sampling.wait(1):
                t, i = cpu_times()
                samples["runner_cpu_busy_pct"].append(round(100 * (1 - (i - idle) / max(t - total, 1)), 1))
                total, idle = t, i

        def sample_source():
            while not stop_sampling.wait(5):
                try:
                    count = sh(pg + ["-d", "postgres", "-c", "SELECT count(*) FROM pg_stat_activity WHERE datname LIKE 'cust%'"]).stdout.decode().strip()
                    samples["source_backends"].append(int(count))
                except Exception:
                    pass

        threads = [threading.Thread(target=f, daemon=True) for f in (sample_runner, sample_source)]
        for t in threads:
            t.start()
        admin_dsn = "postgres://postgres:%s@127.0.0.1:%d/postgres?sslmode=disable" % (password, args.postgres_port)
        load = [str(shutil_bins["loadgen"]), "-node", str(out / "node.json"), "-out", str(out / "loadgen-report.json"), "-duration", "%ds" % args.duration, "-pg-admin-dsn", admin_dsn,
                "-label", args.label or args.profile, *args.loadgen_arg]
        with (out / "loadgen.log").open("wb") as log:
            load_exit = subprocess.run(load, stdout=log, stderr=subprocess.STDOUT).returncode
        stop_sampling.set()
        for t in threads:
            t.join(timeout=10)
        (out / "stop").touch()
        service_exit = service.wait(timeout=300)

        # 7. Assemble.
        node = json.loads((out / "node-report.json").read_text()) if (out / "node-report.json").exists() else {}
        loadgen = json.loads((out / "loadgen-report.json").read_text()) if (out / "loadgen-report.json").exists() else {}
        node_log = (out / "node.test.log").read_text(errors="replace")
        failed_reads = {}
        for code, klass, stage in re.findall(r"kelvo read failed: code=(\S+) class=(\S+) stage=(\S+)", node_log):
            key = "%s/%s/%s" % (code, klass, stage)
            failed_reads[key] = failed_reads.get(key, 0) + 1
        duration = max(node.get("duration_s", 1), 1)
        series = node.pop("samples", [])
        (out / "node-samples.json").write_text(json.dumps(series))
        (out / "node-metrics.prom").write_text(node.pop("metrics_text", ""))
        cores = node.get("cpu_usage_usec", 0) / 1e6 / duration
        top = lambda key: max([s.get(key, 0) for s in series] or [0])
        mean = lambda key: sum(s.get(key, 0) for s in series) / max(len(series), 1)
        report["node"] = {**node, "cpu_cores_mean": round(cores, 2), "cpu_pct_of_limit": round(100 * cores / profile["cpus"], 1),
                          "memory_peak_mib": round(node.get("memory_peak_bytes", 0) / 2**20, 1),
                          "rss_node_peak_mib": round(top("rss_node_bytes") / 2**20, 1), "rss_workers_peak_mib": round(top("rss_workers_bytes") / 2**20, 1),
                          "rss_workers_mean_mib": round(mean("rss_workers_bytes") / 2**20, 1), "job_cgroups_peak": top("job_cgroups"), "job_cgroups_mean": round(mean("job_cgroups"), 1),
                          "failed_reads_logged": failed_reads, "samples_file": "node-samples.json"}
        report["loadgen"] = loadgen
        report["runner_cpu_busy_pct"] = {"mean": round(sum(samples["runner_cpu_busy_pct"]) / max(len(samples["runner_cpu_busy_pct"]), 1), 1), "max": max(samples["runner_cpu_busy_pct"] or [0])}
        report["source_backends"] = {"max": max(samples["source_backends"] or [0]), "samples": samples["source_backends"]}
        proxy_stats = out / "latproxy.stats.json"
        if proxy_stats.exists():
            report["source_proxy"] = json.loads(proxy_stats.read_text())
        capacity = loadgen.get("capacity_errors")
        gate = {"loadgen_exit_zero": load_exit == 0, "node_exit_zero": service_exit == 0, "no_capacity_errors": capacity == 0, "no_oom_kill": node.get("memory_events_oom", 0) == 0,
                "no_capacity_failures_logged_by_node": not any(k.split("/")[1] == "capacity" for k in failed_reads)}
        report["gate"] = gate
        report["passed"] = all(gate.values())
    finally:
        for proc in reversed(started):
            if proc.poll() is None:
                proc.terminate()
        for proc in started:
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
        for secret in ("node-env.json", "scenario.yaml"):
            (out / secret).unlink(missing_ok=True)
        (out / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: report.get(k) for k in ("profile", "passed", "gate")}, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())

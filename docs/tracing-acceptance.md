# Cluster trace acceptance

This fixture runs two real gateways, one worker and three local TLS NATS peers. It checks trace continuity and failure handling; it does not measure throughput or overhead.

1. Build the frozen Kelvo, [Landlock launcher](../sandbox/launcher.c) and [fixture collector](../scripts/fixtures/otlp-collector/main.go) on the designated Linux VM.
2. Prepare a disposable certificate with [prepare_tracing_fixture.py](../scripts/prepare_tracing_fixture.py). Keep host trust unchanged.
3. Run [tracing_cluster_acceptance.py](../scripts/tracing_cluster_acceptance.py) in a bounded private network and mount namespace. Bind its prepared CA bundle read-only over `/etc/ssl/certs/ca-certificates.crt` inside that service.
4. Supply the frozen source, executable hashes, cached pinned NATS archive, private fixture directory and fresh report path. Use `--help` for arguments.
5. Retain the report and logs, then independently verify service/cgroup removal and the unchanged host CA hash.

Use 1 CPU, 3 GiB RAM, no swap and a watchdog. The fixture installs no packages. Generated keys, tokens, broker state and span IDs remain private.

| Gate | Required evidence |
| --- | --- |
| Durable context | Submit with the worker stopped; shut down that gateway; receive the result through another gateway |
| Trace graph | One durable query tree and a separate startup-probe tree, with exact roles and parent links |
| Negative control | The same captured events reject a deliberately wrong parent |
| Authorization | Reject missing credentials, forged trace bodies and another tenant's handle access; ignore incoming trace headers |
| Result | Exact integer, decimal, timestamp and NULL values, complete Arrow framing and durable success |
| Collector faults | Queries succeed during stalls and 503 responses; both exporters stop within the tested bound |
| Privacy and limits | Reject unexpected fields, attributes and canaries; fail on collector limits without truncating evidence |

A missing queued-wait span makes the run inconclusive and failed; it is not proof of a product defect. Failed runs retain their failure and cleanup state. No cross-host timestamp subtraction or pure source/compute attribution is claimed.

See [tracing configuration](tracing.md), [worker timing metrics](child-timings.md) and [validation rules](validation.md).

## Recorded result

Source `93339e7` passed all seven gates on Azure: trace continuity across gateway restart, exact parent links, authorization, Arrow values, collector faults, privacy and cleanup. Collector race tests (5 tests, 28 test/subtest pass events), vet and all three builds passed. [Receipt](evidence/otlp-acceptance-93339e7.json).

The correctness trace contained 11 durable spans and 7 startup-probe spans. The same events rejected a wrong parent. Queries still succeeded during collector stalls and HTTP 503 responses; the slowest service shutdown stayed below 2.971 seconds against an 8-second bound. Service/cgroup removal and unchanged host trust were independently verified.

The [first run](evidence/otlp-acceptance-4b54b16-failed.json) failed its result check. Its SQL used the unquoted identifier `at`; the corrected fixture quotes it and retains bounded private result diagnostics. Both runs remain recorded, including their [component timing and overlap observations](evidence/otlp-acceptance-overlap.json). The first overlap observation arrived after that component had exited; the second observed both services running. This gate measures correctness, not telemetry overhead or deployment capacity.

# Export lifecycle diagnostics

The isolated reproduction passed all six diagnostic-parser controls and all
five export lifecycle scenarios. The earlier [hosted failure](evidence/export-ci-37144625480-failure.json)
remains unresolved; the new [reproduction receipt](evidence/export-diagnostics-reproduction-1513901.json)
records a separate result.

| Check | Observed result |
| --- | --- |
| Diagnostic parser | 6 controls passed; zero failures, errors or skips |
| Actual worker lifecycle | 5 scenarios and their parent passed in one attempt |
| Lost completion | Cancelled before the existing wait deadline; durable receipt present; 3 total executions |
| Public diagnostics | 3 bounded records; zero rejected or truncated records |
| Resource boundary | 1 CPU, 3 GiB, no swap, 256 tasks; non-root private network |
| Teardown | Original service exit 0 retained; exact service unloaded and cgroup absent |

The test and harness source was `151390182698735a7f65dab1a486c5d7e83b9f5a`.
The unchanged worker binary was built from `0da7523527477df905b9e68dd329b719c161a753`
and used NATS 2.14.7. The receipt retains separate source manifests and complete
binary hashes, verified before and after the run. The service budget included
race-instrumented test compilation and the loopback broker.

The first helper attempt stopped during archive extraction because the host's
Python lacked a requested API. It started no parser test, broker or lifecycle
test. Its original failed service status and verified cleanup are preserved in
the same evidence, separately from the corrected helper's successful attempt.

CI receipts now retain a closed schema: expected and observed state, deadline
and receipt flags, a finite error class, execution count, and an allowlisted
test-file location. Private logs retain full assertions. The existing 20-second
state wait, cancellation requirement and execution-count checks remain in force.
Future occurrences can therefore identify the failing phase without publishing
SQL, credentials, job identifiers or arbitrary error text.

This evidence covers one isolated single-broker reproduction. Sustained load,
multi-broker recovery and live-provider acceptance remain separate gates.

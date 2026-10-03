# Export dispatch conflicts

A worker reads a queued export before reserving it with a conditional update.
The supervising gateway may legally renew that export's authority between the
read and the update, keeping it queued but changing its revision. A revision
conflict alone cannot tell the worker whether the job was assigned or is still
waiting.

The dispatcher retries every failed assignment update after releasing its local
reservation. Redelivery rereads durable state: queued work can be assigned;
already assigned, terminal, or missing jobs are acknowledged without execution.
A failed update that marks an invalid or expired queued job as failed also
retains its delivery until the transition is confirmed or later state is read.

Export retries request a broker delay of `min(lease_duration / 3, 250ms)`.
Interactive query deliveries retain their existing behavior. Queue deadlines,
authority deadlines, one-minute publication deduplication, and consumer delivery
limits remain unchanged. A retry does not extend a job's execution authority.

## Validation

The [scoped validation receipt](evidence/export-dispatch-conflicts-64dc69a.json)
records one Linux run against source `64dc69a`, with verified source and native
build inputs, one CPU, 3 GiB memory, no swap, and a private loopback network.

| Check | Recorded result |
| --- | --- |
| Old dispatch logic with the new regression | Expected failure: a legal queued renewal was followed by an acknowledgment instead of a retry |
| Fixed dispatcher with the race detector | Four top-level tests and two subtests passed |
| Fixture permissions and diagnostic parser | 18 controls passed |
| Cluster package vet | Passed |
| NATS 2.14.7 dispatch gate with the race detector | Passed: deduplicated republish, delayed redelivery, one assignment, zero SQL calls before claim and after cancellation |
| Original service exit and cleanup | Exit 0; owned service and cgroup removed |

The control snapshot differs from the fixed snapshot only in the restored
dispatch logic. Its gate requires the exact expected assertion after the legal
renewal and rejects compilation, setup, panic, race, or timeout failures as
substitutes. NATS 2.15.0 is selected in CI but was not measured in this run.
The broker discriminator never submits an execution claim. The fixed in-memory
scenario runs one execution, then confirms that terminal redelivery cannot
repeat it.

The deterministic dispatcher tests cover a real legal gateway renewal, failure
update conflicts and transient errors, duplicate assignment, terminal duplicate
delivery after one execution, and reservation cleanup. A separate real-broker
gate uses the actual dispatcher with restricted initializer, gateway, and worker
identities. It confirms that the same-ID republish is deduplicated and the
original delivery is delayed, redelivered, and assigned once.

Run the broker gate through `scripts/export_broker_acceptance.py --mode dispatch`
using a fresh fixture directory and the same explicit binary checksum, broker
version, source manifest, Go path, module file, and build tags as the other
broker gates. It uses a synthetic Arrow executor and does not substitute for
the sandboxed-worker lifecycle gate. CI selects this separate test explicitly
for both supported broker versions.

This is a reproducible dispatch defect. It is not evidence that this race caused
the earlier lifecycle CI failure; its original assertion was not retained.
See [the retained CI failure receipt](evidence/export-ci-37144625480-failure.json)
for that unresolved failure's recorded scope.

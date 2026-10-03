# Production delivery process

The [Kelvo Production Delivery board](https://github.com/orgs/SyneHQ/projects/3)
and [roadmap tracker #32](https://github.com/SyneHQ/kelvo-go/issues/32) organize
implementation and release acceptance. The [production checklist](production-status.md)
remains the readable capability summary; the [validation record](validation.md)
contains the evidence behind current claims.

## Plan work

Every deliverable has a GitHub issue with an observable outcome, explicit
acceptance criteria, dependencies, priority, area and milestone. The tracker uses
real sub-issues and blocking relationships. Keep provider access requirements
explicit, and record only sanitized fixture/account requirements in public issues.

| Priority | Milestone | Intent |
| --- | --- | --- |
| P0 | Production foundation | Isolation, credentials, lifecycle, recovery and release acceptance |
| P1 | Durable analytics and efficient scale | Durable exports/cache, remote reader safety, maintenance and selective refresh |
| P2 | Interoperability and continuous data | Standard clients and CDC after durability prerequisites |

Milestones are delivery groups, not calendar promises. A feature's implementation
and the acceptance of a deployment are different tasks. Split a ticket when they
need different ownership, access or evidence; do not quietly relax its criteria.

## Move work through the board

| Status | Meaning |
| --- | --- |
| Todo | Accepted backlog; dependencies determine when work can start |
| In Progress | Active engineering on a bounded ticket |
| Review | Implementation awaits independent review |
| Validation | Review is complete; required runtime or release evidence is pending |
| Blocked | A named dependency or provider-access requirement prevents progress |
| Done | Published implementation, review and every required acceptance criterion passed |

Use the Delivery board for active status, Backlog for the full inventory,
Release acceptance for test and compatibility work, and External dependencies
for blocked work. Issue labels and the project Priority field must agree.
Link related pull requests and evidence before moving a card to Done. A merged
change with a pending provider gate stays in Validation or a linked open
acceptance ticket.

## Implement and review

Use purpose-based topic branches and focused commits of at most ten changed
files. Keep shared interfaces owned by an integration lead while independent
modules run in parallel. Review authorization, exact Arrow values, cancellation,
resource ownership, durability and failure paths before optimizing throughput.
Contributor build restrictions are in [AGENTS.md](../AGENTS.md).

Pull requests explain the concrete behavior change, linked issues, validation
and operational impact. Use closing references only when the entire linked
issue is satisfied. Reviewers should identify required deployment or provider
evidence separately from implementation defects.

## Attach evidence and close

1. Record source revision, executable/fixture hashes and the exact environment.
2. Run relevant correctness, race, compatibility and recovery checks. Use the
   designated Linux test hosts for builds and package downloads.
3. Preserve failed attempts, skipped gates and limits alongside passing reports.
4. Complete independent review and required CI on the revision being published.
5. Update the operator documentation, checklist, issue criteria and board status.

Protocol fixtures are not live cloud-provider conformance. A short functional
campaign is not a sustained production-capacity result. Snapshot compatibility
is not rolling cluster compatibility. Never transfer an older binary's
throughput or memory measurements to a new runtime without rerunning the
applicable workload.

Current operational gates include [process-loss recovery](process-loss-acceptance.md),
[release upgrades](release-upgrades.md), [mixed load](operational-acceptance.md),
[storage conformance](storage-conformance.md) and [key rotation](gateway-key-rotation.md).
Additional TLS identity and managed-scratch contracts are documented in
[TLS rotation](tls-identity-rotation.md) and [worker scratch](worker-scratch.md).

## Keep tracking reproducible

Use `gh api` REST operations for issue/sub-issue/dependency updates and GraphQL
for ProjectV2 items, fields and views. Read existing state first and make
idempotent updates. Restrict queries and mutations to this repository and project;
never copy unrelated organization content into the board. Repository issue
permissions and GitHub Projects permissions are separate. Keep workflow tokens
read-only unless a specific reviewed automation needs more access.

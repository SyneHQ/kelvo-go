# Production delivery process

[Board](https://github.com/orgs/SyneHQ/projects/3) · [Tracker #32](https://github.com/SyneHQ/kelvo-go/issues/32) · [Capability checklist](production-status.md) · [Evidence](validation.md)

## Plan work

Create one issue per deliverable: outcome, acceptance criteria, dependencies, owner, priority and milestone. Record provider access needs without secrets.

| Priority | Milestone |
| --- | --- |
| P0 | Production foundation: isolation, credentials, lifecycle, recovery |
| P1 | Durable analytics and efficient scale: exports, cache, storage maintenance |
| P2 | Interoperability and continuous data: standard clients and CDC |

Milestones are delivery groups, not dates. Split implementation and deployment acceptance when they need different owners or evidence.

## Move work through the board

| Status | Use when |
| --- | --- |
| Todo | Accepted, waiting for work or dependencies |
| In Progress | Implementation is active |
| Review | Independent review is pending |
| Validation | Review passed; required runtime evidence is pending |
| Blocked | A named dependency or access requirement prevents progress |
| Done | Published, reviewed and all acceptance criteria passed |

Keep labels, priority and linked PRs current. A merge with outstanding provider acceptance needs an open acceptance ticket.

## Implement and review

Use purpose-based branches, stacked PRs for dependent changes, and **at most ten files per commit**. Coordinate shared interfaces; review authorization, exact values, cancellation, ownership and durability before optimizing.

PRs state the behavior change, tests and operational impact. Use closing references only for fully satisfied issues. Follow [AGENTS.md](../AGENTS.md) for build restrictions.

## Attach evidence and close

1. Record the revision, binary/fixture hashes and environment.
2. Run required correctness, race, upgrade and recovery checks.
3. Keep failures, skipped gates and limitations.
4. Complete review and CI on the published revision.
5. Close the PR with **rebase merging**. Verify the resulting linear commits and tree; do not create merge commits or squash the history.
6. Update docs, issue criteria and board status.

Fixtures do not prove live-provider compatibility; short trials do not prove sustained capacity. Snapshot upgrades do not prove rolling cluster upgrades. Old benchmark results belong to their recorded binaries.

Release gates: [process loss](process-loss-acceptance.md), [snapshot upgrades](release-upgrades.md), [rolling applications](rolling-upgrades.md), [mixed load](operational-acceptance.md), [storage](storage-conformance.md), [key rotation](gateway-key-rotation.md).

## Keep tracking reproducible

Use `gh api`: REST for issues/dependencies, GraphQL for ProjectV2. Read existing state, make idempotent updates, and scope requests to this repo and board. Keep workflow tokens read-only unless reviewed automation needs more access.

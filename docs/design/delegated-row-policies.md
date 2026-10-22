# Design: row and column policies for delegated users

Status: Kelvo side implemented, except the audit receipt (see "Implementation notes"). The db-api side is not.

## Problem

Row and column policies exist for principals defined in Kelvo's own policy
(`access.principals.<id>.row_column_policy`). The federated engine enforces
them: it hides unlisted tables and columns and filters Arrow rows before joins,
CTEs, aggregates and windows.

Most real users do not reach Kelvo as Kelvo principals. They reach it through
db-api, a delegated service principal that signs a short-lived grant for each
query on behalf of one end user. Policy validation refuses a row/column policy
on a delegated principal (`internal/cluster/principal.go`, the
`DelegatedResolver != nil && RowColumnPolicy != nil` check). Today, then, the
only per-user control for dashboard users is db-api's own check of which saved
connections a user may open. A user who may open a connection sees every row
and column it returns.

The goal is per-user row and column restrictions for delegated users, enforced
by Kelvo with the same engine and the same fail-closed rules as principal
policies.

## Decision already taken

Enforce only in the federated engine (option 2 of the #28 discussion). Kelvo
does not rewrite native SQL in source dialects. A restricted native query
returns `UNSUPPORTED` naming federated mode (PR #248). Pooled read workers and
the result cache are skipped for restricted reads (`access.Restricted(ctx)` in
`internal/worker/read_worker.go`). That rule stays.

## Proposal

### Who decides, who enforces

db-api already decides, per user, which saved connections a user may use, and
Kelvo trusts that decision through the signed grant. The per-user restriction
is the same kind of decision, so db-api owns it and signs it. Kelvo verifies
the signature and enforces it. Kelvo stores no per-user policy, so no
enrollment or cutover is needed in Kelvo when a user's policy changes.

### Wire format: delegation claims version 2

Add one optional field to the signed claims (`delegation/delegation.go`):

```json
{
  "version": 2,
  "...": "all version 1 fields unchanged",
  "row_column_policy": {
    "revision": "sha256 of the policy as db-api stored it",
    "sources": {
      "<source alias from claims.sources>": {
        "tables": {
          "<table alias from claims.sources[].tables>": {
            "columns": ["order_id", "amount"],
            "rows": {"kind": "comparison", "column": "account_id", "type": "int64", "op": "eq", "value": "42"}
          }
        }
      }
    }
  }
}
```

- The body reuses `access.Policy` exactly (`sources -> tables -> columns, rows | all_rows`), so validation and enforcement are the existing code. Source and table keys are the aliases already present in `claims.sources[].tables`. A key that is not there is a validation error.
- The policy is part of the signed payload. It cannot be added, removed or changed without db-api's key.
- **Version bump is the fail-closed switch.** Version 1 Kelvo rejects version 2 tokens: strict JSON decoding (`DisallowUnknownFields` in `StrictJSONLimit`) refuses the unknown field, and `validateClaims` refuses any version other than 1. An old node can never run a restricted grant unrestricted. db-api must send version 2 only once every Kelvo node has been upgraded.
- **Nesting.** Claims parsing allows JSON depth 16; this policy nests 7 levels deep, so it fits.
- **Size.** Tokens are capped at `MaxTokenBytes` (32 KiB). Cap the policy at the same limits `access.Validate` already applies, and reject grants over the cap in db-api, not by truncation.

### Enforcement in Kelvo

1. **Gateway.** Verify the token as today. If `row_column_policy` is present, `access.Validate` it against `claims.sources` and require `mode: federated`. A native request with a policy gets `UNSUPPORTED`, as in PR #248.
2. **Node.** Where `executionAuthorityContext` builds the restriction for principals, build it from the verified claims for delegated executions and call `access.WithPolicy`. From then on, the existing restricted path applies: no read workers, no result cache, no time-range routing (`route.go` already excludes restricted reads), no raw files or unguarded readers, and scan diagnostics off.
3. **Exports.** A delegated export carries the claims, so the same policy applies at execution, and authority is rechecked at download as today.
4. **Audit.** Record the policy `revision` in the durable audit receipt for every restricted read, so a result can be tied to the exact policy that produced it.

### Revocation

- Grants live at most five minutes (claims `exp`), so a stricter policy applies to every new query within one grant lifetime. db-api can make that immediate by refusing to sign old-revision grants.
- In-flight reads keep the policy they started with. That matches the principal-policy limit already documented: final authority checks withhold EOS, but bytes already delivered cannot be recalled.
- A changed policy needs no Kelvo restart or cutover. That is the main gain over Kelvo-side principal policies.

### db-api side

- Store per-user (or per-team, per-role) policies keyed by saved connection, with a revision hash.
- When signing a grant, attach the policy for the selected connections and send `mode: federated` whenever any policy applies.
- Send version 2 only behind a feature flag that is turned on after every Kelvo node is upgraded.
- The UI for editing policies is out of scope for the first change. An admin API and seed data are enough to qualify it.

## Rejected alternatives

- **Kelvo stores per-user policies.** That needs user enrollment and revocation in Kelvo, which is exactly the open "coordinated enrollment/revocation" part of #28. It also duplicates db-api's user model.
- **Native SQL rewriting.** Per-dialect parsing is an attack surface with no fail-safe default.
- **Source-database RLS.** It depends on each customer's database setup.

## Tests required before shipping

- Adversarial cross-user tests. Two users share a connection with different row predicates, and neither can see the other's rows through joins, CTEs, aggregates, windows, `SELECT *`, or column aliases. Extend the existing principal row tests to delegated grants.
- A tampered policy (any byte) fails signature verification.
- A version 2 token on a version 1 node is rejected, demonstrated with the previous release binary.
- A restricted delegated read never touches a read worker, the result cache or a routed live source.
- Exports and downloads under a policy.
- The audit receipt carries the policy revision.

## Open questions

1. Does db-api need predicates beyond the current `access.Predicate` (comparison, null checks)? For example, `IN` lists for multi-account users. That would extend `access.Predicate` for principals too.
2. Should a missing table entry mean hidden (as for principals) or unrestricted? This proposal keeps "hidden", which fails closed.
3. Policy per user, or per role with users mapped to roles in db-api? This changes only db-api; the claim format is the same.

## Implementation notes

Owner decisions for the open questions: no new predicate kinds (`access.Predicate` as is); a table missing from a restricted source's policy is hidden; the claim format is the same whether db-api maps policies per user or per role.

- **Types.** `delegation.DelegatedPolicy` mirrors `access.Policy` instead of reusing it: `delegation` is public API and an `internal/access` type cannot appear in it. `access.FromDelegated` converts field by field and runs `access.Validate`.
- **Versions.** Version 1 must not carry `row_column_policy`; version 2 must. `Sign` and verification both refuse the other combinations, and any policy source or table key that is not a `claims.sources` alias or one of its `tables` names.
- **Source names.** Claim aliases are the source IDs of the per-request resolved catalog (`validateConnectionCatalog` requires `source.ID == alias`), so the policy applies to them unchanged. `connectionSourceIDs` only names quota keys.
- **Modes.** A native request under a policy-carrying grant is refused at admission with `UNSUPPORTED` and the principal-policy message. Bound verification already refuses a native request with table-scoped sources; the node reports the restricted case instead of a bare denial.
- **Worker check.** `delegatedExecution` used to refuse any restricted context. It now accepts a restriction only when it equals the policy of the grant being executed, and refuses a version 2 grant that runs without it.
- **Exports.** Query and export execution both build their context in `executionAuthorityContext`, which re-verifies the grant and applies its policy. Exports do not attach the delegation execution binding today, so a delegated export is refused by the worker before any source is read; the policy is enforced regardless and no export runs unrestricted.
- **Nesting.** The strict JSON reader allows depth 16. A simple predicate needs 7 levels; each nested `and`/`or` adds 2, so about four levels of combined rules fit. Flatten deeper rules in db-api.
- **Not done: audit revision.** The `Binding` of an audit start frame is decoded with unknown fields refused and re-encoded for comparison. A new field would make every older reader treat a journal containing it as corrupt, so the revision is not recorded yet. It needs an agreed journal compatibility step first.
- **Rollout.** Upgrade every gateway and node before db-api sends version 2. An older node rejects the token on its version and on the unknown field.

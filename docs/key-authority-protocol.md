# Key-authority protocol

`internal/authfence` checks a fixed fleet's key-document authority through NATS. It is an internal protocol: gateway authentication does not use it yet.

## Operations

A scope fixes its ID, tenants and gateways. Canonical YAML binds that membership to a positive revision and the exact key-document SHA-256. Limits: **16 KiB, 256 tenants, 16 gateways**. Constructors detach inputs.

| Method | Result |
| --- | --- |
| `Read` | Observational snapshot; never authorizes keys. |
| `Verify` | Expected document plus a fresh, guarded gateway witness. |
| `Initialize` | Creates missing authority; refuses overwrite. |
| `Advance` | Checks the caller's expected document revision, then conditionally publishes a higher revision. |
| `Check` | Reports `current_verified` only after a fresh control witness. |

Witness proofs check reply correlation, stream, authority sequence, scope, document and original attempt. Gateway adoption still needs local durable admission and final freshness checks.

## Broker and credentials

Use one TLS 1.3 endpoint, a configured CA and separate identities. Only NATS Server **2.14.7/2.15.0** and the expected plain, file-backed, three-replica stream with default persistence are accepted. Unknown versions/settings fail closed.

| Identity | Allowed publications |
| --- | --- |
| Gateway | Stream INFO, normal GET and `auth.witness.<its-roster-ID>`. |
| Control | Stream INFO, normal GET, `auth.current` and `auth.witness.control`. |
| Initializer | Control permissions plus stream creation; reserve for setup. |

Each identity subscribes only to `_INBOX.kelvo-authfence.<replica>.*`; the initializer uses replica ID `control`. Grant no reply publication, peer inbox access, response grants, stream changes/deletion, consumers, wildcard witnesses or subject/import mappings. Credentials use protected files or a named password environment variable.

The operator must enforce these broker permissions. Membership enrollment and migration are not implemented.

## Deadlines and failures

1. Freeze the original attempt before private-file work: **two seconds maximum**.
2. Use one operation per client. Overlap returns `ErrBusy`; no reconnect or automatic replay.
3. Missing, malformed, duplicate or late mutation ACK means `unknown`. A later `Check` proves current state, not who wrote it.
4. Close, then retain ownership until `Quiesced`. Cleanup can outlast bounded caller wait; uncertainty permanently fences that client.

`Quiesced` covers tracked custody, not every library goroutine, operating-system reclamation or erasure of secret copies.

## Validation

[R2 evidence](evidence/key-authority-protocol.json) at `c39f637` passed seven Linux VM stages: regular/race builds, ordinary regular/race tests, real R3 regular/race tests and vet. Independent receipts reconcile **30 ordinary roots and 15 R3 paths per profile**, both broker versions, hashes and cleanup. This validates the protocol only.

R1 remains a failed trial in the evidence. Its recovery assertion ran before later broker term changes, and its ACK proxy could fault an error response. R2 added bounded, witnessed recovery from all three endpoints and faults only validated success ACKs. Production protocol code was unchanged by that correction.

Gates cover guarded CAS, partitions/quorum loss, concurrency, response faults, retention/ACLs and cleanup. Recovery requires the original document; a successful proof with the wrong binding fails.

## Reproduce on an isolated VM

Use Linux amd64 with cached dependencies and the checksum-pinned broker archives named in `r3_fixture_linux_test.go`. Run inside a finite cgroup v2 service. The reviewed runner uses **2 CPU, 2 GiB, no swap, 256 tasks and a 25-minute ceiling**; these are test limits, not production sizing.

Use a private `0700` output directory and private logs. The fixture starts its own loopback brokers; never target an application cluster.

```sh
export GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off
unset KELVO_TEST_AUTHFENCE_R3
go test -count=1 ./internal/authfence
go test -race -count=1 ./internal/authfence
go vet ./internal/authfence

export KELVO_TEST_AUTHFENCE_R3=1
export KELVO_TEST_AUTHFENCE_R3_DIR=/absolute/private/results
export KELVO_TEST_AUTHFENCE_R3_ARCHIVE_2_14_7=/absolute/cached/2.14.7.tar.gz
export KELVO_TEST_AUTHFENCE_R3_ARCHIVE_2_15_0=/absolute/cached/2.15.0.tar.gz
go test -count=1 -timeout=8m -run '^TestAuthFenceR3$' ./internal/authfence
go test -race -count=1 -timeout=8m -run '^TestAuthFenceR3$' ./internal/authfence
```

Retain hashes, exact test coverage, both version receipts, credential removal and process/cgroup cleanup evidence. Ordinary tests run fuzz seeds, not a sustained fuzz campaign.

## Limits

The package owns no accepted revision floors, local auth-state writer, gateway policy binding or global retired-token history. Quorum ACKs do not protect against correlated storage loss. Passing these gates does not establish global revocation bounds, atomic export publication, recall of delivered bytes or a safe deployment/rollback procedure.

Existing gateway features: [key rotation](gateway-key-rotation.md) and [local authentication history](gateway-auth-state.md). Check the [production status](production-status.md) before deployment.

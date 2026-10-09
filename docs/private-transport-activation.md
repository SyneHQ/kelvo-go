# Private transport activation

The transport implementation is opt-in. Qualify your application authority, worker and customer tunnel together before enabling traffic.

| Component | Required behavior |
| --- | --- |
| Application resolver | Check delegated access, current source metadata and the live execution lease. Sign a parent-only proof for the source revision, original hostname, worker certificate and exact execution. |
| Application issuer | Authenticate worker mTLS. Repeat source/team access checks. Find the live Rabbit route and issue one physical-open ticket. Check revocation on each renewal. |
| Kelvo parent | Verify and retain proofs outside child input and credentials. Bind sockets to the admitted child and execution. Reject unsupported adapters before launch. |
| Native adapter | Preserve source TLS hostname verification. Register PostgreSQL cancellation with the parent. Decode original-session cancellation before confirming source stop. |
| Deployment | Pin tested artifacts. Provision the exact trust keys, identities and network peers. Test the complete customer-client path. |

The application must retain enough signed grant data to repeat live lease validation. A grant hash, tenant name or worker certificate alone cannot replace that proof.

The source-to-tunnel mapping belongs to the application catalog. Rabbit route discovery identifies a live tunnel. It does not authorize the user to query that source.

## Acceptance sequence

1. Configure the [worker transport](private-operation-transport.md) and the application issuer while user traffic remains disabled.
2. Test real source TLS through the customer client, Rabbit, Kelvo and your application authority.
3. Test two tenants. Reject crossed source, route, worker, certificate and execution identities.
4. Test queued PostgreSQL cancellation, source revocation, actual child crash, near-expiry cancellation and duplicate requests.
5. Test saturation and shutdown. Check the source backend, both stream directions and retained execution capacity.
6. Enable only the source engines and operation kinds that passed the complete deployment test.

## Current evidence and limits

Queued PostgreSQL TLS fixtures passed cancellation, child crash, source rebinding and near-expiry checks. Confirmed cancellation required source protocol evidence and physical cleanup. A child crash returned `cleanup_unknown`; the source statement timeout bounded remaining work.

These fixture results do not establish production authentication or routing. Private MySQL cancellation remains unqualified. Analytical federation through private transport is not supported.

Never retry a mutation automatically after an uncertain write. A terminal response or closed tunnel does not prove source work stopped.

# Private source proof

Status: protocol preparation. Workers reject private-proof responses until the
parent broker and child IPC path qualify. Ordinary direct sources are unchanged.

The application signs one `sourceproof.Envelope` for each authorized private
source. The envelope carries the original signed grant and a separate Ed25519
proof. The proof binds tenant, source revision, original database authority,
execution ID, owner, claim, worker ID and the worker's TLS identity/fingerprint.
Its maximum lifetime is 15 seconds. The application must use the earlier of the
grant, live lease, source and verified certificate deadlines.

1. Verify the original grant and canonical request.
2. Read current source authorization and the authoritative private-route mapping.
3. Check the exact execution lease and authenticated worker certificate.
4. Sign the proof. Return it separately from credentials in `private_sources`
   (query alias map) or `private_source` (operation).
5. Before issuing each connection ticket, verify the proof with a pinned key,
   reverify the original grant and recheck metadata and the live lease.

`sourceproof.Verify` checks the signature, time and exact supplied scope. It does
not authenticate the original grant or renew a lease. Build expected scope from
verified authority, never from the received proof itself.

Keep envelopes in the trusted parent. Do not place them in source options,
child input, environment variables, files, logs or transport ticket claims.
Maximum grant: 32 KiB. Maximum proof token: 8 KiB. Bound the enclosing JSON body.

Activation still needs authoritative source-to-route metadata, issuer and lease
endpoints, inherited socketpair IPC, cancellation and saturation qualification.
See [activation gates](private-transport-activation.md).

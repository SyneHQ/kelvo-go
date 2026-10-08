# Refresh proof before each connection

`rabbitconnect.NewWithSourceProof` creates a parent-owned opener for one admitted execution and source. It requires a pinned application public key, an independently verified source scope and a resolver refresh callback.

For every physical open:

1. Match the request against the fixed execution, source and worker scope.
2. Obtain a fresh resolver proof. Never reuse a cached proof.
3. Verify its signature, original grant digest, route, token and binding version.
4. Send the original grant and proof to the issuer over mTLS.
5. Verify the returned ticket, including its token ID and admission deadline.
6. Open the configured proxy. No direct-database fallback exists.

Proof expiry limits new connection admission. It does not shorten the separate cleanup lifetime of an established stream. The issuer must independently validate the original grant, current source mapping and live execution lease. The HTTP client alone grants no authority.

Azure normal and race tests cover fresh checks on repeated opens, revocation, scope substitution, expired proofs, mTLS proof delivery and overlong ticket rejection. The issuer request is bounded to 64 KiB, including the original grant and proof.

No production runtime constructs this opener. Issuer authority and renewal endpoints, native driver integration and paired database qualification remain required.

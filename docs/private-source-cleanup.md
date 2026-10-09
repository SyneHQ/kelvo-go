# Private source cancellation

Private source production use remains disabled until the real queued PostgreSQL cancellation gate passes.

1. The trusted worker verifies Rabbit's signed acceptance receipt and registers its original DATA ticket digest, acceptance ID and expiry in the operation ledger.
2. Registration must succeed before the adapter receives the DATA stream. A lost acknowledgement or concurrent cancellation blocks dispatch.
3. Cancelling an accepted read changes its state to `cancelling`. Its first cancellation fixes the cleanup deadline: at most five seconds, bounded by the original source and operation deadlines.
4. Normal execution leases and renewals are denied. The separate cleanup lease checks the exact original worker, owner, claim, digest and acceptance ID. It cannot extend the deadline.
5. The trusted parent sends a typed PostgreSQL cancellation request with the original verified source TLS settings. Rabbit blocks further writes on the original DATA stream and permits only bounded completion reads.
6. A cancel packet write is not proof of a stopped query. The trusted adapter must observe completion on the original authenticated PostgreSQL session, and the parent must join physical cleanup.
7. A crash, expired custody, hard revocation or unconfirmed source stop produces `cleanup_unknown`. It never claims that the source stopped or makes the operation eligible for replay.

MySQL cleanup and private writes are not enabled by this contract. They require separate source-stop and transaction-effect evidence.

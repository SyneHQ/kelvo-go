# Object snapshot writer shutdown

Closing an object backend rejects new writers, cancels admitted writers and waits for their cleanup before closing the reader client. Pending client factories count as admitted work, including late or partial client results.

## Finish borrowed files first

1. Stop source work when the writer context is canceled.
2. Finish using `File()` or the current multipart file.
3. Call `Commit` or `Abort`; always defer `Abort` immediately after a successful `Begin`.
4. Join `Close` before releasing the backend's owner.

`Close` does not abort a file while its caller may still write to it. A caller or provider that ignores cancellation can keep shutdown waiting. Managers already defer transaction cleanup on their error paths.

Owned writer clients close after renewal, lease release and staging cleanup. An injected shared client closes once, after its writers finish. Cancellation after publication does not undo the committed generation; `Commit` still reports cleanup errors.

This contract covers object writer lifetime. Query readers, durable reader leases and remote garbage collection have separate contracts; remote deletion stays disabled.

## Validation

[Source `997157e`](evidence/object-writer-drain-997157e.json) passed all 45 focused tests with race detection: five new writer scenarios plus existing upload, lease, publication and staging controls (141 test/subtest pass events). Vet passed for both packages. The service and cgroup were independently absent after exit.

The frozen source, external module files and cached bridge stayed unchanged. The gate used 1 CPU, 3 GiB RAM, no swap and a private network; these are test limits, not deployment sizing. No live-provider or throughput claim follows.

No old-code behavioral control ran: restoring only the old backend file would be incompatible with the new helper. That static finding is not counted as a reproduced runtime failure.

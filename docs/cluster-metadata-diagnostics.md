# Cluster metadata startup diagnostics

When opening cluster state fails with `cluster metadata unavailable`, Kelvo's
`cluster-init`, gateway, node and refresh commands emit one preceding diagnostic:

```text
KELVO_CLUSTER_METADATA {"schema":1,"phase":"bucket_create","class":"other","api_status":0,"api_code":0}
cluster metadata unavailable
```

This example shows the format, not the cause of a recorded failure. Refresh
commands retain their existing final `Tenant refresh state is unavailable`
message. The final error line, exit behavior, requests, permissions and shared
five second `OpenStore` deadline remain unchanged. No retry is added.

| Phase | Failed operation |
| --- | --- |
| `bucket_create` | Create the metadata key-value bucket after its lookup failed. |
| `value_read` | Read the immutable cluster policy from the metadata bucket. |
| `bucket_reopen` | Reopen the metadata bucket after its direct-access configuration check. |
| `value_create` | Create the previously absent immutable cluster policy. |
| `unknown` | A phase outside the known diagnostic set; no arbitrary phase text is emitted. |

The fixed classes are `api_error`, `context_canceled`, `deadline_exceeded`,
`timeout`, `no_responders`, `permission_denied`, `authorization`,
`connection_closed` and `other`. Classification uses typed errors rather than
matching message text. `api_status` contains a typed API status from 400 through
599, or zero when absent or outside that range. `api_code` is the NATS client's
unsigned 16-bit API code, or zero when unavailable. These values describe the
observed failure; they do not establish whether retrying a write is safe.

The diagnostic contains no broker description, URL, subject, tenant name,
configuration, credentials or wrapped error text. Ordinary missing-policy,
policy-mismatch and direct-access refusals keep their existing errors. The
core store library returns the typed diagnostic without logging; CLI callers
choose to print it before their normal terminal error.

Healthy broker monitoring establishes a prerequisite for initialization. It
does not prove that the subsequent authenticated metadata API operations will
succeed. Retain the failed attempt and its diagnostic when investigating
initialization; do not treat a later successful attempt as an explanation of
the earlier failure.

## Validation

The [combined `b47ce87` build](evidence/sustained-build-b47ce87.json) passed all
59 Python controls and nine focused Go race tests, plus vet. Its
[600-second smoke](evidence/sustained-smoke-b47ce87.json) passed all ten lifecycle
gates and exact cleanup. This does not identify the cause of the earlier
[`40965ef` startup failure](evidence/sustained-startup-diagnostics-40965ef.json)
or close the separate TLS test and multi-hour acceptance gates.

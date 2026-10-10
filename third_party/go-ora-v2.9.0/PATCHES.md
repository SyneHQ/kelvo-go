# Oracle long-password capability

This directory contains the complete `github.com/sijms/go-ora/v2` module at `v2.9.0`.
Kelvo changes one capability bit in `data_type_nego.go`.
The original MIT [license](LICENSE) remains unchanged.

## Source

- Upstream tag: [v2.9.0](https://github.com/sijms/go-ora/tree/v2.9.0).
- Upstream commit: `f8dc23ec168d75fba8e70cccffdaa43023feee07`.
- Module checksum: `h1:+iQbUeTeCOFMb5BsOMgUhV8KWyrv9yjKpcK4x7+MFrg=`.
- Module file checksum: `h1:QgFInVi3ZWyqAiJwzBQA+nbKYKH77tdp1PYoCqhR2dU=`.

## Patch

The client advertises `LONG_PASSWORD` through bit `0x80` in `CompileTimeCaps[4]`.
The original value, `106` (`0x6a`), does not contain this bit.
The patched value, `234` (`0xea`), adds this bit and preserves every other bit.

```diff
--- upstream/data_type_nego.go
+++ patched/data_type_nego.go
@@ -85,1 +85,1 @@
-            6, 1, 0, 0, 106, 1, 1, 11,
+            6, 1, 0, 0, 234, 1, 1, 11,
```

All other upstream files remain unchanged. This directory includes no other dependency source.
The root and Go adapter modules select this directory through a version-specific local `replace`.
A workspace that includes both modules must select this directory with its own `replace`.
Build from the repository checkout or a complete source archive. A downloaded parent module does not include this nested module.

## Reason

The original driver rejected the existing 48-byte fixture password with `ORA-01017`.
The same credentials worked through Oracle SQLPlus over verified TLS.
The patched driver authenticates with the unchanged password. An incorrect password still fails.

Official `go-ora/v3 v3.0.1` advertises this capability, but it returns an empty database type for every result column.
Kelvo cannot safely decode these results. Upstream metadata fixes remain unmerged.
The v2 patch preserves the existing type metadata, exact-number conversion, and transaction behavior.

## Update gate

Remove this directory when an upstream version passes the same Oracle checks without this patch.
Check the original 48-byte password, an incorrect password, repeated connections, and verified TLS.
Check exact integers, decimals, timestamps, NULL values, and table relationships.
Run the app approval, write, replay, and cleanup checks before replacing the runtime.

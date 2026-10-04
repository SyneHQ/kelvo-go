# Bind principals to a catalog

Source grants name IDs. An optional catalog binding also pins what those IDs mean, so a worker rejects a different catalog before serving that tenant.

## 1. Compute the fingerprint

Run against the worker's catalog on every replica:

```sh
kelvo catalog-fingerprint --config kelvo.yml
```

The command prints one SHA-256 digest. It loads and normalizes YAML, including local path checks. It does not resolve secret values, read source data, open database engines or contact providers.

Replicas must produce the same digest. Use identical normalized paths, including resolved local source paths; copying the same relative YAML into different directories can produce different bindings.

## 2. Add the binding

Copy the digest into the existing principal policy on the gateway and every worker:

```yaml
policy:
  access:
    revision: 1
    catalog_binding:
      version: 1
      sha256: REPLACE_WITH_THE_64_CHARACTER_LOWERCASE_DIGEST
    principals:
      analyst:
        kind: user
        federated_sources: [sales]
```

Keep the remaining tenant, resource and principal settings. Omitting `catalog_binding` preserves legacy unbound behavior; this check is opt-in.

## 3. Cut over together

Follow the existing [drained principal-policy rollout](principal-access.md#3-roll-out-safely). Install matching policy and catalog definitions across the tenant's gateway and workers; verify allowed and denied queries before switching traffic.

Changing a bound definition requires a new fingerprint and another drained cutover. Do not update policy metadata underneath running workers.

## What the digest covers

The fingerprint identifies normalized catalog definitions, including secret-reference **names**. It does not identify secret values, file contents, remote data or the live database behind a credential. Rotating a secret value keeps the binding; changing where that secret points does too. Treat credential retargeting and grant changes as authority changes: raise `policy.access.revision` and use the same drained cutover, even when the catalog digest stays unchanged.

This is configuration consistency, not a signature or independent database-identity check. Operators, configurations and secret stores remain trusted.

## Validation

[Recorded Linux acceptance](evidence/catalog-authority.json) on `bcbb3dc` passed all nine stages: build, correctness, race/cgocheck2, stub, vet and actual sandboxed query/export/snapshot checks. Every required control passed; ordinary fixture-dependent skips remain listed. Source and bridge inputs stayed unchanged, and independent cleanup verified the owned service was gone.

This does not validate live-provider identity, deployment capacity or coordinated policy rollout.

[Principal access](principal-access.md) · [Row and column rules](row-column-access.md) · [Cluster setup](cluster.md)

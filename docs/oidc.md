# OIDC tokens

Cluster gateways can accept OIDC/JWT bearer tokens from a self-hosted or SaaS identity provider. This is opt-in and requires [file authentication](gateway-key-rotation.md) and a [principal access policy](principal-access.md) on the mapped tenant.

A valid token resolves to **one existing principal**. Kelvo does not create principals, users or grants from tokens. The principal's grants, [row/column rules](row-column-access.md), delegated users and handle ownership apply unchanged.

## Configure

```yaml
authentication:
  keys_file: /etc/kelvo/gateway-keys.yml   # still required; API keys keep working
  oidc:
    issuer: https://idp.example.com/realms/analytics
    # jwks_url: https://idp.example.com/realms/analytics/protocol/openid-connect/certs
    audiences: [kelvo-gateway]
    tenant: analytics
    claim: sub                  # default: sub
    principals:                 # exact claim value -> principal ID
      2f1c9a4e-7d3b-4c8e-9a51-0b6f3e2d1c7a: analyst
    groups_claim: groups        # default: groups
    groups:                     # group name -> principal ID
      finance-analysts: analyst
    algorithms: [RS256, ES256, EdDSA]   # default
    clock_skew: 30s             # default 30s, at most 5m
    cache_ttl: 10m              # default 10m, 1m..1h
```

Prefer a stable, provider-assigned identifier such as `sub` (or `oid` on Microsoft Entra ID). Emails can be reassigned. With `claim: email`, a token is accepted only when `email_verified` is the JSON boolean `true`.

Without `jwks_url`, the gateway reads `<issuer>/.well-known/openid-configuration` and requires its `issuer` to match exactly. Issuer and JWKS URLs must be HTTPS, and discovery and JWKS requests never follow redirects. Every mapped principal must exist in the tenant's `policy.access` at startup.

Send the token the same way as an API key: `Authorization: Bearer <token>`. The gateway first checks API keys, then OIDC.

## Validation

A token is rejected unless all of these hold:

- Its `alg` is in `algorithms`. `none` is never accepted. `HS256` is accepted only when listed and `hmac_secret_file` names a private secret file of at least 32 bytes; that secret is the only HS256 key, so a provider public key cannot be used as an HMAC secret.
- The signature verifies with a JWKS key of the matching type (RSA ≥ 2048 bits, P-256, Ed25519). `kid` is required when the key set holds more than one key. Headers with `crit` are rejected.
- `iss` equals `issuer`; `aud` contains one of `audiences`; `exp` is present and not past; `nbf` and `iat`, when present, are not in the future. `clock_skew` applies to each time check.
- Header and claims are JSON objects with no duplicate keys; claim names match exactly (`Exp` is not `exp`).
- The identity claim and groups claim map to exactly one principal. No match returns `401`. Two different principals (for example an email mapping and a group mapping that disagree) also return `401`.

Authority ends at token expiry: a request still running at `exp` is canceled, and authority checks within the request compare against `exp` directly. If the gateway's API-key authority is stale, closed or poisoned, OIDC tokens are rejected too. Tokens are never logged.

## Key rotation and outages

The gateway refreshes the key set every `cache_ttl / 2` and replaces it wholesale. A token with an unknown `kid` is rejected immediately, without waiting for the provider; it starts a background refresh (at most one at a time, at most once every 10 seconds), so a newly published key is accepted on a later request. A key removed by the provider stops verifying after the next successful refresh, and at most `cache_ttl` (capped at 1 hour) after the last one. If the provider is unreachable, the last key set is used until `cache_ttl` after its fetch; after that every token is rejected. An unreachable provider at startup does not stop the gateway; tokens are rejected until a fetch succeeds.

## Limits

- One issuer and one tenant per gateway. Up to 1,024 mappings, 64 JWKS keys and 16 audiences.
- Exports are not available to OIDC callers yet: export fills bind to an API key, so OIDC requests to start or download exports are denied.
- Revoking a user at the provider takes effect when their token expires. Keep token lifetimes short. Removing a mapping requires a gateway restart.
- Gateway replicas fetch keys independently.

[Principal access](principal-access.md) · [Security model](../SECURITY.md)

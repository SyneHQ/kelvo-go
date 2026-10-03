# Selected cloud source secrets

Kelvo resolves mapped source credentials from AWS Secrets Manager, Azure Key Vault and Google Secret Manager in the trusted parent. Children receive only selected source values; catalogs retain environment-reference names.

This covers query and refresh credentials on cluster workers. It does not rotate existing query credentials, parent object-storage clients, NATS, gateway keys or TLS. Protocol fixtures and worker-boundary tests are not live-cloud acceptance.

## Configure explicit mappings

Add mappings to worker node YAML. Provider credential documents are private, service-owned YAML files, rotated by atomic replacement.

```yaml
secrets:
  ttl: 30s
  timeout: 5s
  providers:
    aws-production:
      type: aws_secrets_manager
      region: us-east-1
      credentials_file: /run/kelvo/aws-provider.yml
    azure-production:
      type: azure_key_vault
      vault: example-vault
      credentials_file: /run/kelvo/azure-provider.yml
    gcp-production:
      type: gcp_secret_manager
      credentials_file: /run/kelvo/gcp-provider.yml
  references:
    KELVO_SOURCE_WAREHOUSE_PASSWORD:
      provider: aws-production
      secret: arn:aws:secretsmanager:us-east-1:123456789012:secret:warehouse-ABC123
    KELVO_SOURCE_REPORTING_TOKEN:
      provider: azure-production
      secret: reporting-token
    KELVO_SOURCE_EVENTS_PASSWORD:
      provider: gcp-production
      secret: projects/123456789012/secrets/events-password
```

Configure only mapped providers. `secrets.files` can coexist without duplicate references. Use least-privilege grants and separate credentials for unrelated tenants.

| Provider | Locator and version | Required provider authority |
| --- | --- | --- |
| AWS Secrets Manager | Full ARN in the configured region; default `AWSCURRENT`, optional `version` is a 32–64-character version ID | `secretsmanager:GetSecretValue`; `kms:Decrypt` when using a customer-managed KMS key |
| Azure Key Vault | Secret name; latest by default, optional 32-character hexadecimal version | Secrets `get` or equivalent RBAC permission |
| Google Secret Manager | `projects/<project-number>/secrets/<name>`; latest by default, optional positive numeric version | Secret-version access permission and a bearer token with `cloud-platform` scope |

GCP requires the canonical **project number**, so response identity can be checked exactly. AWS supports standard, China and GovCloud regional endpoints. Azure currently supports public-cloud `vault.azure.net`; GCP uses its global public endpoint. Arbitrary endpoints, redirects, ambient credential chains and environment proxies are disabled. Private DNS routing must retain the configured service's TLS identity.

## Supply short-lived provider credentials

AWS credential document:

```yaml
access_key_id: REPLACE_WITH_PROVIDER_ACCESS_KEY
secret_access_key: REPLACE_WITH_PROVIDER_SECRET_KEY
session_token: REPLACE_WITH_SESSION_TOKEN
expires_at: 2026-10-03T12:00:00Z
```

Omit `session_token` for a static AWS key. `expires_at` is still required as an operator-imposed validity bound. Azure and GCP credential documents use:

```yaml
token: REPLACE_WITH_PROVIDER_BEARER_TOKEN
expires_at: 2026-10-03T12:00:00Z
```

Replace example timestamps with actual expiry, at most 24 hours ahead. Kelvo does not mint/refresh tokens; an operator-managed refresher must replace the document before expiry. Keep provider credentials out of catalogs, source mappings, logs and tickets.

The document must be a private regular file owned by the service user, under trusted directories. Symlinks, extra hardlinks, group/other access, unknown YAML fields, aliases, missing expiry and oversized documents fail closed. Exact source-secret bytes are preserved; NULs and values over 16 KiB are rejected. AWS binary secrets and GCP payloads are base64-decoded; GCP CRC32C is verified.

## Cache, rotation and revocation

The default lookup timeout is 5 seconds; the maximum is 30 seconds. At most 16 provider/file reads execute concurrently. Overlapping reads of one reference coalesce; cancellation of one waiter does not cancel another. There are at most 128 combined mappings, 16 cloud providers and 2 MiB of cached value bytes. Request, decode and waiting-call buffers add memory.

TTL is zero by default, with a maximum of 5 minutes. Zero coalesces current reads without retaining completed values. A cached value never outlives the earlier of its TTL, provider-credential expiry or an expiry returned by the provider. Azure disabled, not-yet-valid or expired secrets are rejected. Expired cache entries are never served as a fallback when a provider denies, times out or fails.

Remote revocation and credential-file replacement are observed on the next uncached lookup; there is no cloud event watcher. Nonzero TTL creates a bounded observation delay. For immediate **local** revocation, a trusted controller can call `Provider.Invalidate(reference)`: it clears the cache and fences reads that have not passed the final delivery check. Stop affected running operations separately; already admitted strings and child credentials cannot be recalled.

Errors expose no response bodies, resource IDs, credentials or paths. Configured failures never fall back to stale environment values; unmapped references retain selected-environment behavior. Credential loading requires an accurate host clock; admitted expiry durations use the monotonic clock.

## Validation and deployment gate

Package tests cover TLS protocol requests, AWS signing, resource/version checks, checksum and size rejection, timeout/coalescing, rotation/expiry, invalidation, sanitized failures and a real child-process environment boundary. Before deployment acceptance, exercise each chosen provider with actual IAM/RBAC grants, token renewal, version rotation, denial/revocation and network loss. Record the tested binary and provider configuration without credentials.

API references: [AWS GetSecretValue](https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html), [Azure Get Secret](https://learn.microsoft.com/en-us/rest/api/keyvault/secrets/get-secret/get-secret), [Google AccessSecretVersion](https://cloud.google.com/secret-manager/docs/reference/rest/v1/projects.secrets.versions/access) and [SecretPayload integrity](https://cloud.google.com/secret-manager/docs/reference/rest/v1/SecretPayload).

# Security

Kelvo Go is an early developer preview for a single configured trust domain. Its source selection, read-only checks and subprocesses do not constitute a hardened hostile-SQL sandbox.

Use least-privilege database accounts, dedicated worker/container identity, restricted mounts, outbound network policy, CPU/memory/disk quotas and TLS termination. SQL can invoke functions beyond ordinary table reads; database permissions and network restrictions remain necessary. Do not attach unrelated tenants to one configured server.

Source secrets are resolved from explicitly configured environment-variable names. They are excluded from API responses and intentionally not copied into public test fixtures. Never include secrets or customer data in issues.

Use GitHub private vulnerability reporting on this repository for sensitive reports. Supported-version and response-time guarantees have not yet been established for this preview.

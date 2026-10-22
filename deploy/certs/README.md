# Source trust anchors

`aws-rds-global-bundle.pem` is Amazon's global RDS certificate bundle, from
https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem (fetched
2026-10-22, 114 certificates, SHA-256
`90a6f1a7978a1b1b096dca6c57511ca48d3b8cf052f8e704baf358f34ea2f21b`).

The `worker` image target adds it to the system trust so workers can verify
RDS sources with `sslmode=verify-full`. Refresh it when Amazon adds or rotates
an RDS certificate authority.

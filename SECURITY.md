# Security

Do not publish secrets, test credentials, private repository URLs, internal deployment details, or customer data in this repository.

Use SDK credentials from trusted server code only. Do not expose API keys,
service-account access tokens, or JWTs in browser applications.

The SDK sends ingestion requests only to the configured Medallion API origin. API
keys and optional server-side bearer tokens are provisioned outside the SDK and
must be bound to the client's immutable workspace.

The SDK sends either `Authorization: Bearer <token>` or
`X-Medallion-API-Key: <key>`, never both. It rejects HTTP redirects so these
credentials, workspace selectors, and event bodies cannot be forwarded to a
different origin.

OpenTelemetry spans intentionally omit bearer tokens, API keys, request bodies, event payloads, and metadata. Do not add sensitive values as span attributes.

To report a security issue, use the repository security reporting flow on GitHub.

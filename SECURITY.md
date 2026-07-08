# Security

Do not publish secrets, test credentials, private repository URLs, internal deployment details, or customer data in this repository.

The SDKs send bearer credentials from trusted server code only. Do not expose API keys, service-account access tokens, or JWTs in browser applications.

For deployed use, Medallion services should share one auth path: either a gateway exchanges API keys into stable `service_account:*` principals, or each service accepts the same service-account OAuth/JWT credential.

The SDK sends `Authorization: Bearer <token>`. It does not send legacy local-development backend key headers.

OpenTelemetry spans intentionally omit bearer tokens, API keys, request bodies, event payloads, and metadata. Do not add sensitive values as span attributes.

To report a security issue, use the repository security reporting flow on GitHub.

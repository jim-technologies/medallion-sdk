# Medallion Python SDK

Install directly from Git:

```sh
uv add "medallion @ git+https://github.com/jim-technologies/medallion-sdk.git@vX.Y.Z#subdirectory=python"
```

The repository-root `VERSION` is shared by every SDK and released through one
plain root `vX.Y.Z` tag. Use a full commit SHA instead of the tag when a
production build requires commit-level pinning.

The Python SDK currently implements the Connect-backed server integration path:

- `client.connect.register_datasource(...)`
- `client.datasources.register(...)`
- `client.audit.record(...)`
- `client.audit.trail(...)`
- `client.cdc.record(...)`

Audit and CDC use separate typed Connect contracts: audit methods publish
`AuditEvent` values through `PublishAuditEvents`, while CDC remains on
`PublishCdcEvents`.

Low-level Connect request/response types are generated from the vendored public protobuf contract and are exported as:

```python
from medallion import connect_pb2
```

Use this SDK from server-side Python services only. Do not embed service API keys in browser or mobile clients.

OpenTelemetry tracing is off by default and can be enabled without SDK-specific exporter setup:

```python
client = MedallionClient(
    base_url=os.environ["MEDALLION_BASE_URL"],
    access_token=os.environ["MEDALLION_SERVICE_ACCOUNT_TOKEN"],
    tracing=True,
)
```

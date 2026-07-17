# Medallion Go SDK

Install directly from Git:

```sh
go get github.com/jim-technologies/medallion-sdk/go@vX.Y.Z
```

The repository-root `VERSION` is shared by every SDK and released through one
plain root `vX.Y.Z` tag. The `/go` suffix identifies this package inside the
root module; it is not a separate version or tag namespace. Use a full commit
SHA instead of the tag when a production build requires commit-level pinning.

If the consumer already pins the pre-unification nested `/go` module, migrate
it once before installing the shared root tag:

```sh
go mod edit -droprequire=github.com/jim-technologies/medallion-sdk/go
go get github.com/jim-technologies/medallion-sdk/go@vX.Y.Z
go mod tidy
```

The Go SDK currently implements the Connect-backed server integration path:

- `client.Connect.RegisterDatasource`
- `client.Datasources.Register`
- `client.Audit.Record`
- `client.Audit.Trail`
- `client.CDC.Record`

Audit and CDC use separate typed Connect contracts: audit methods publish
`AuditEvent` values through `PublishAuditEvents`, while CDC remains on
`PublishCdcEvents`.

Low-level Connect request/response types are generated from the vendored public protobuf contract and are available from:

```go
import connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
```

Use this SDK from server-side Go services only. Do not embed service API keys in browser or mobile clients.

OpenTelemetry tracing is off by default and can be enabled without SDK-specific exporter setup:

```go
client, err := medallion.NewClient(medallion.ClientConfig{
	BaseURL:     os.Getenv("MEDALLION_BASE_URL"),
	AccessToken: os.Getenv("MEDALLION_SERVICE_ACCOUNT_TOKEN"),
	Tracing:    medallion.TracingConfig{Enabled: true},
})
```

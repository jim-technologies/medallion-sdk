# Medallion Go SDK

Install the SDK directly from Git using the repository-wide release tag. Go,
Python, and TypeScript use the same plain `vX.Y.Z` version:

```sh
go get github.com/jim-technologies/medallion-sdk/go@v0.3.1
```

That tag is created at the repository root; there are no language-specific Go,
Python, or TypeScript tag namespaces. Pin a full Git commit SHA instead when a
deployment requires commit-level immutability.

This branch prepares the unreleased 0.5.0 candidate. Its active Go surface is
`medallion.ingest.v1`: seven generated table, append and query RPCs through
`client.Ingest`. CDC/audit clients, types and generated bindings are removed.
See [migration and release blockers](../docs/migration-0.5.md). The install
command above names the latest actual tag, whose older API differs from this
source candidate.

An operator provisions the immutable workspace, API key and API origin. The SDK
does not provide administration or credential provisioning.

## Durable execution

Medallion can also back a Temporaless workflow runtime. That surface is
**not** part of this Go module, deliberately: Temporaless's Go module brings
the AWS SDK, `gocloud.dev`, the Temporal SDK, and OpenDAL's native bindings,
which would take this module's dependency graph from 25 modules to roughly 78
for every consumer — including one that only appends tables. Go has no
optional dependencies, and a nested module would need its own tag, which this
repository's single-root-tag release model does not allow.

A Go application that wants Medallion as a durable backend therefore depends
on Temporaless directly and adds the two Medallion identity headers with a
standard ConnectRPC interceptor:

```go
import (
	"net/http"

	"connectrpc.com/connect"
	"github.com/jim-technologies/temporaless/adapters/go/connectstore"
)

func medallionIdentity(apiKey, workspaceID string) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("X-Medallion-API-Key", apiKey)
			req.Header().Set("X-Medallion-Workspace-Id", workspaceID)
			return next(ctx, req)
		}
	})
}

store := connectstore.NewHTTPClientStore(
	http.DefaultClient,
	os.Getenv("MEDALLION_BASE_URL"),
	connect.WithInterceptors(medallionIdentity(apiKey, workspaceID)),
)
```

The Python SDK ships the richer factory, including the
`GetStoreCapabilities` handshake; see the repository README.

## Tables and queries

`client.Ingest` passes generated protobuf requests through with header-only
workspace identity. A table is one declared tabular collection: an ordered
schema (`BOOL`, `INT64`, `FLOAT64`, `STRING`, `BYTES`, `TIMESTAMP`, `DATE`,
`JSON`), a `TIMESTAMP` time column, and an optional sort key. `UpdateTable`
evolves it additively: send the FULL desired schema with the existing columns
unchanged, then the new nullable ones.

`CreateTable`, `UpdateTable`, and `AppendRows` always carry a batch
idempotency key. The SDK generates one per call, or
`medallion.WithIngestIdempotencyKey(ctx, key)` pins a caller key; either way
it is sent as the `Idempotency-Key` header and stamped into the request's
`request_id` when that field is empty, because `request_id` is what the
contract deduplicates on. An exact replay under the same key is absorbed
without duplication and re-acknowledged with the original counts.

Queries pass one statement through verbatim in the declared ClickHouse SQL
dialect; poll `GetQueryResults` while the state is `RUNNING`, waiting between
polls with a bounded backoff rather than back to back, and follow
`next_page_token` until it is empty. A `FAILED` state carries its cause in
`error`. See [`examples/tables.go`](../examples/tables.go) for the complete
flow.

## Configure a client

Every client requires one canonical workspace ID (`ws_` followed by 26
lowercase canonical base32 characters). Authenticate with exactly one API key
or JWT access token. The workspace is immutable for the lifetime of the client;
create another client to use another workspace. API keys are workspace-bound,
and every request is scoped to that exact configured workspace.
Set `MEDALLION_BASE_URL` to the supplied Medallion API origin, for example
`https://api.example.com`.

```go
client, err := medallion.NewClient(medallion.ClientConfig{
	BaseURL:            os.Getenv("MEDALLION_BASE_URL"),
	APIKey:             os.Getenv("MEDALLION_API_KEY"),
	WorkspaceID:        os.Getenv("MEDALLION_WORKSPACE_ID"),
	Timeout:            20 * time.Second,
	Retry: medallion.RetryConfig{
		MaxAttempts:    3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
	},
})
if err != nil {
	log.Fatal(err)
}
```

For JWT authentication, set `AccessToken` instead of `APIKey`. Ingest workspace
identity is carried only in the configured request header.

## Delivery semantics

Keep an append batch's exact rows, options and idempotency key stable across
retries. A complete acknowledgement permits marking its durable outbox entry
delivered. Server batch replay semantics do not make network attempts exactly once.

## Errors and retries

```go
var apiErr *medallion.APIError
if errors.As(err, &apiErr) {
	fmt.Println(apiErr.Code, apiErr.RequestID)
	if apiErr.ErrorInfo != nil {
		fmt.Println(apiErr.ErrorInfo.Reason, apiErr.ErrorInfo.Metadata)
	}
	if apiErr.Retryable(true) {
		// Reschedule only because this exact operation is safely idempotent.
	}
}
```

Retries are disabled by default and capped at five total attempts. When
enabled, retries apply to safe reads and declared idempotent writes. The exact
serialized body and batch key are reused. Deadlines and cancellation stop retry waits.
Client backoff is exponential and jittered; valid `Retry-After` seconds or HTTP
dates are honored without shortening or jitter. Passing `false` to
`APIError.Retryable` always returns false.

API errors retain the HTTP status, Connect code, request ID, sanitized message,
decoded `google.rpc.ErrorInfo`, and additive detail bytes. Raw HTTP error bodies
and credentials are never retained.

Generated protobuf request and response types are available from:

```go
import ingestv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/ingest/v1"
```

Use this SDK only from trusted server-side Go services. Never embed service
credentials in browser or mobile applications.

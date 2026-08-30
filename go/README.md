# Medallion Go SDK

Install the SDK directly from Git using the repository-wide release tag. Go,
Python, and TypeScript use the same plain `vX.Y.Z` version:

```sh
go get github.com/jim-technologies/medallion-sdk/go@vX.Y.Z
```

That tag is created at the repository root; there are no language-specific Go,
Python, or TypeScript tag namespaces. Pin a full Git commit SHA instead when a
deployment requires commit-level immutability.

The Go SDK speaks two bounded surfaces and nothing else:

- `medallion.ingest.v1` — the tabular tables, append, and query surface
  (`CreateTable`, `GetTable`, `ListTables`, `UpdateTable`, `AppendRows`,
  `RunQuery`, `GetQueryResults`) through the deliberately thin `client.Ingest`,
  built on the generated bindings in `go/gen/medallion/ingest/v1`. The richer
  convenience layer ships Python-first.
- `medallion.connect.v1` — the DEPRECATED CDC/audit publish surface
  (`PublishCdcEvents`, `ListCdcEvents`, `PublishAuditEvents`,
  `ListAuditEvents`), which keeps working for existing integrations.

An operator uses Medallion's control plane to provision the workspace, API
key, and Medallion API base URL (plus a connector ID for the deprecated
publish surface) before a server-side application starts the SDK. The SDK
does not automate that provisioning or expose broader platform administration
APIs.

## Durable execution

Medallion can also back a Temporaless workflow runtime. That surface is
**not** part of this Go module, deliberately: Temporaless's Go module brings
the AWS SDK, `gocloud.dev`, the Temporal SDK, and OpenDAL's native bindings,
which would take this module's dependency graph from 25 modules to roughly 78
for every consumer — including one that only publishes CDC events. Go has no
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
dialect; poll `GetQueryResults` while the state is `RUNNING` and follow
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
	DefaultConnectorID: os.Getenv("MEDALLION_CONNECTOR_ID"),
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

For JWT authentication, set `AccessToken` instead of `APIKey`. List requests
carry the same configured workspace in both the header and request body;
publish event bodies omit their server-derived workspace field.

## Delivery semantics

Publishing is at-least-once, not exactly-once. Medallion durably deduplicates an
event by its idempotency key, so every retry must preserve the exact event and
key. A successful receipt reports each event as accepted or duplicate; a
duplicate receipt is successful delivery, not an error.

For database-backed producers, use a transactional outbox: write the business
change and its event to the outbox in the same database transaction, retry the
unchanged outbox event until the SDK returns a complete receipt, and only then
mark the outbox row delivered. Never mint a new idempotency key for a retry.
Durable server deduplication makes repeated delivery safe, but it does not make
network attempts exactly-once.

## Publish CDC

Use a durable outbox ID, database log position, or provider event ID as the
idempotency key. The optional helper produces the same deterministic UUIDv5
key as the other language SDKs when a source has a compound identity. Keys are
preserved exactly and must be valid UTF-8 between 1 and 512 bytes:

```go
key, err := medallion.StableIdempotencyKey("orders", "partition-3", "lsn-9A/BC")
if err != nil {
	return err
}

receipt, err := client.CDC.Record(ctx, medallion.CDCEvent{
	StreamName:     "orders",
	EntityType:     "order",
	EntityID:       int64(42),
	Operation:      "update",
	IdempotencyKey: key,
	Payload:        map[string]any{"status": "paid"},
})
```

Publish up to 1000 events atomically with `RecordBatch`:

```go
receipt, err := client.CDC.RecordBatch(ctx, []medallion.CDCEvent{
	{
		StreamName:     "orders",
		EntityType:     "order",
		EntityID:       "order_1",
		Operation:      "insert",
		IdempotencyKey: "outbox_1",
		Payload:        map[string]any{"status": "created"},
	},
	{
		StreamName:     "orders",
		EntityType:     "order",
		EntityID:       "order_2",
		Operation:      "update",
		IdempotencyKey: "outbox_2",
		PayloadJSON:    `{"status":"paid"}`,
	},
})
```

`Record` and `RecordBatch` use `ClientConfig.DefaultConnectorID`. To select a
connector for one batch, put it on the request-level input:

```go
receipt, err := client.CDC.PublishBatch(ctx, medallion.CDCBatchInput{
	ConnectorID: "connector_123",
	Events:      events,
})
```

Connector scope never belongs to a nested `CDCEvent`; the generated protobuf
event sent on the wire therefore omits `connectorId`.

`Payload` and `PayloadJSON` are mutually exclusive. `PayloadJSON` must contain
exactly one valid JSON value and is retained byte-for-byte. The older
`Table`/`PrimaryKey` shape remains available only as a compatibility path.

## Publish audit events

The action and authoritative outcome are separate. The workspace, connector,
source system, observer, origin, durable ID, and observed time remain
server-derived.

```go
receipt, err := client.Audit.Record(ctx, medallion.AuditRecord{
	Actor:          medallion.ActorRef{Type: "user", ID: "user_123"},
	Action:         "approve",
	Outcome:        medallion.AuditOutcomeSucceeded,
	ResourceType:   "invoice",
	ResourceID:     "invoice_42",
	IdempotencyKey: "billing-audit:evt-9921",
	Payload:        map[string]any{"approvalPolicy": "four-eyes"},
})
```

`client.Audit.RecordBatch(ctx, records)` publishes an atomic batch of up to
1000 audit events using `ClientConfig.DefaultConnectorID`. Use `PublishBatch`
with an `AuditBatchInput` for an explicit request-level connector. Connector
scope is not part of `AuditRecord` and must not be supplied on a nested
protobuf event.

## Read back and paginate

`List` returns one lossless page. Durable `int64` IDs are decimal strings and
JSON payload numbers are `json.Number` values.

```go
page, err := client.CDC.List(ctx, medallion.CDCListQuery{
	StreamName: "orders",
	Limit:      100,
})

iterator := client.Audit.Iterate(ctx, medallion.AuditTrailQuery{
	ResourceType: "invoice",
	ResourceID:   "invoice_42",
})
for iterator.Next() {
	event := iterator.Event()
	fmt.Println(event.ID, event.Action, event.Outcome)
}
if err := iterator.Err(); err != nil {
	return err
}
```

Iterators reject repeated cursors and stop after 10,000 pages rather than
looping forever. Page sizes are capped at 500. List responses fail closed if an
event omits the configured workspace or reports a different workspace.

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
enabled, the SDK retries only the four validated ingestion calls: lists are
read-only, while publishes require complete event-level idempotency keys. The
exact serialized body is reused. Deadlines and cancellation stop retry waits.
Client backoff is exponential and jittered; valid `Retry-After` seconds or HTTP
dates are honored without shortening or jitter. Passing `false` to
`APIError.Retryable` always returns false.

API errors retain the HTTP status, Connect code, request ID, sanitized message,
decoded `google.rpc.ErrorInfo`, and additive detail bytes. Raw HTTP error bodies
and credentials are never retained.

Generated protobuf request and response types are available from:

```go
import connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
```

Use this SDK only from trusted server-side Go services. Never embed service
credentials in browser or mobile applications.

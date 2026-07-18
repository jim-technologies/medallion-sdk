# Medallion SDK

Public multi-language SDK for Medallion. TypeScript is currently the broadest client; Go and Python cover the Connect-backed server integration path for datasource registration, audit, and CDC.

Status: pre-1.0 public SDK. Treat pinned tags or SHAs as the stable install boundary, and integration-test the auth path against the target Medallion service environment before using it for production traffic. The TypeScript protocol layer uses invariantprotocol v0.8.3 with vendored Connect, Ontology, and Storage descriptors.

The TypeScript SDK currently includes:

- datasource registration through `medallion-connect`
- dedicated audit and CDC event ingestion through `medallion-connect`
- audit trail reads through `medallion-connect`
- ontology queries and action planning/execution through `medallion-ontology`
- file upload through `medallion-storage`

Ontology writes, storage catalog management beyond upload, and broad action administration are intentionally not part of this first SDK surface.

Go and Python currently include the Connect-backed server integration path
only. Their low-level Connect message types are generated from the bounded
initial client subset under `proto/medallion/connect/v1/connect.proto`. Shared
types and RPCs are wire- and validation-identical to the canonical Connect
contract; connector installation, secret, authorization, and broad connection
administration APIs are outside this initial SDK surface.

## Install

Medallion-owned packages are distributed only from Git. They are not published
to npm, PyPI, crates.io, or another language registry. Third-party dependencies
still resolve through their normal registries, and Go module proxies may cache
public Git revisions.

The repository-root `VERSION` is the source of truth for TypeScript, Python,
and Go. Every SDK ships together from exactly one plain root `vX.Y.Z` tag;
there are no language-specific versions or tags. Release examples below use
`vX.Y.Z` as a placeholder. Pin that release tag or, for the strongest
reproducibility, replace it with a full commit SHA. Never install a production
build from a branch name.

TypeScript:

```sh
npm install --allow-git=all "github:jim-technologies/medallion-sdk#vX.Y.Z"
```

The npm command permits nested Git dependencies because invariantprotocol is
also Git-distributed and pinned by commit.

Python:

```sh
uv add "medallion @ git+https://github.com/jim-technologies/medallion-sdk.git@vX.Y.Z#subdirectory=python"
```

Go:

```sh
go get github.com/jim-technologies/medallion-sdk/go@vX.Y.Z
```

Go code continues to import `github.com/jim-technologies/medallion-sdk/go`.
The `/go` suffix is a package directory inside the root module, not a separate
module, version, or tag namespace. This path covers the current v0 release line
and a future v1. Before v2, Go requires the standard `/v2` semantic-import-path
migration; the release will still use the same single root `v2.X.Y` tag as the
other SDKs.

Existing consumers that pinned the historical nested `/go` module by commit
must remove that old module requirement before adding the root-tagged release;
otherwise Go sees both modules as providers of the same package:

```sh
go mod edit -droprequire=github.com/jim-technologies/medallion-sdk/go
go get github.com/jim-technologies/medallion-sdk/go@vX.Y.Z
go mod tidy
```

Current development baselines are Node.js 24+, Python 3.14+, and Go 1.26.5+.

## Server-Side Only

Use this SDK from trusted server code only. It sends `Authorization: Bearer <token>` and never supports browser API keys.

For a coherent deployed API path, put the services behind one gateway that exchanges API keys for stable service-account identity, or pass a service-account OAuth access token/JWT that every target service accepts. Direct service calls are only coherent when Connect, Ontology, and Storage are configured to accept the same bearer credential.

Do not expose server API keys or service-account tokens in browser apps.

Credential terms used in this repo:

- Medallion service-account JWT/access token: preferred single bearer credential for a deployed SDK path when every target service accepts it.
- Connect API key: bearer API key accepted by `medallion-connect` and mapped server-side to a stable `service_account:*` principal.
- Legacy local-development backend key: backend-only credential. The SDK does not send `X-Medallion-Connect-Key`.

## Service-Account Bootstrap

Before handing this SDK to a client app, create or choose one stable service-account principal for the integration, grant it only the target organization/tenant and connector/datasource permissions it needs, and issue a short-lived OAuth access token/JWT for server-side use. The SDK sends that token as `Authorization: Bearer <token>`.

Do not put service-account tokens or Connect API keys in browser code. Browser apps should call your server, and your server should call Medallion.

## Quickstart

Required server-side environment:

```sh
export MEDALLION_BASE_URL="https://api.example.com"
export MEDALLION_SERVICE_ACCOUNT_TOKEN="<service-account-oauth-jwt>"
export MEDALLION_ORGANIZATION_ID="org_123"
export MEDALLION_CONNECTOR_ID="conn_123"
export MEDALLION_EXPECTED_INGESTER_PRINCIPAL="service_account:orders-worker"
```

```ts
import { MedallionClient } from "@jimtech/medallion";

const medallion = new MedallionClient({
  baseUrl: process.env.MEDALLION_BASE_URL!,
  accessToken: process.env.MEDALLION_SERVICE_ACCOUNT_TOKEN!,
  organizationId: process.env.MEDALLION_ORGANIZATION_ID!,
  defaultConnectorId: process.env.MEDALLION_CONNECTOR_ID!,
});

await medallion.audit.record({
  actor: {
    type: "user",
    id: "user_123",
  },
  action: "cancel",
  outcome: "succeeded",
  resource: {
    type: "order",
    id: "order_123",
  },
  before: { status: "confirmed" },
  after: { status: "cancelled" },
  metadata: { reason: "user_request" },
  evidenceUrl: "https://example.com/deployments/orders-worker/abc123",
  idempotencyKey: "order_123_cancelled",
  sourceEventId: "orders.order_123.cancelled.v1",
});

const trail = await medallion.audit.trail({
  actor: { type: "user", id: "user_123" },
  resourceType: "order",
  resourceId: "order_123",
  action: "cancel",
  origin: "external_provider",
  outcome: "succeeded",
  limit: 25,
});

const event = trail.events.find(
  (candidate) => candidate.sourceEventId === "orders.order_123.cancelled.v1",
);

if (!event) throw new Error("recorded audit event was not returned");
if (event.actor?.id !== "user_123") throw new Error("source actor mismatch");
if (event.ingesterPrincipal !== process.env.MEDALLION_EXPECTED_INGESTER_PRINCIPAL) {
  throw new Error("ingester principal mismatch");
}
if (event.targetType !== "order" || event.targetId !== "order_123") {
  throw new Error("resource mismatch");
}
if (event.evidenceUrl !== "https://example.com/deployments/orders-worker/abc123") {
  throw new Error("evidence URL mismatch");
}
```

The bearer credential identifies the ingester service account. `audit.record({ actor })` is the source application actor, such as the user who cancelled an order. Every audit and CDC publish requires a stable, source-derived idempotency key; reuse that key for retries of the same source event and never generate a new one per attempt. Control-plane mutations such as datasource registration likewise require a stable idempotency key for the logical mutation. Every audit publish also requires the explicit typed `outcome`; keep `action` as one stable source-operation key rather than encoding the outcome in the verb. The SDK maps the source actor to Connect `actor_principal`. Connect records the authenticated ingester as `ingested_by_principal`; `audit.trail()` exposes it as `ingesterPrincipal`, plus server-derived `origin` and the authoritative `outcome`.

The same flow is available as [examples/audit-trail.ts](./examples/audit-trail.ts).

## CDC Events

```ts
const registered = await medallion.datasources.register({
  name: "primary_postgres",
  type: "postgres",
  idempotencyKey: "register_primary_postgres",
  displayName: "Primary Postgres",
});

await medallion.cdc.record({
  connectorId: registered.datasource.id,
  source: "primary_postgres",
  table: "orders",
  operation: "update",
  primaryKey: {
    id: "order_123",
  },
  before: {
    status: "confirmed",
  },
  after: {
    status: "cancelled",
  },
  idempotencyKey: "orders_order_123_update_1",
});
```

Datasource `metadata` is an optional caller-side annotation copied into the
returned ergonomic object; the current Connect `RegisterConnector` contract
does not persist it.

## Event Source Examples

Use a datasource/connector per source namespace, then set event fields consistently:

- Postgres: `type: "postgres"`, `name: "primary_postgres"`, CDC `source: "primary_postgres"`, `table: "orders"`.
- Application audit logs: `type: "medallion_audit_logs"`, resources like `order/order_123`, stable action keys like `cancel`, and an explicit outcome.
- GitHub: mirror source-state changes through CDC; record access or mutations through audit actions such as `github.pull_request.merge` on a repository resource.
- Google Analytics: treat observations as typed metrics rather than CDC or audit events.
- Vercel: mirror deployment state through CDC; record operator actions through audit on the deployment resource.

## Who Cancelled Order 123?

This example runs against a local or staging endpoint where Connect is reachable with the same bearer credential used by the SDK:

```ts
const medallion = new MedallionClient({
  baseUrl: process.env.MEDALLION_BASE_URL!,
  accessToken: process.env.MEDALLION_SERVICE_ACCOUNT_TOKEN!,
  organizationId: "org_123",
  defaultConnectorId: "conn_123",
});

await medallion.audit.record({
  actor: { type: "user", id: "user_123" },
  action: "cancel",
  outcome: "succeeded",
  resource: { type: "order", id: "order_123" },
  after: { status: "cancelled" },
  idempotencyKey: "order_123_cancelled",
});

const trail = await medallion.audit.trail({
  resourceType: "order",
  resourceId: "order_123",
  actor: { type: "user", id: "user_123" },
  action: "cancel",
  origin: "external_provider",
  outcome: "succeeded",
  limit: 10,
});

const cancellation = trail.events.find(
  (event) => event.action === "cancel" && event.outcome === "succeeded",
);
console.log(cancellation?.actor);
console.log(cancellation?.ingesterPrincipal);
```

`audit.trail()` reads Connect `ListAuditEvents`, so a just-recorded SDK audit event is readable without waiting for an Ontology projection. Audit writes use `PublishAuditEvents`; CDC uses its separate `PublishCdcEvents` path.

Connect now separates source actor from server provenance. The SDK sends source actors as `actor_principal`, stores the structured actor in `payload_json.actor` for ergonomic round-tripping, and reads server provenance from `ingested_by_principal`.

Always pass `limit` or `pageSize` to `audit.trail()`. For full history scans, page explicitly:

```ts
let cursor: string | undefined;
do {
  const page = await medallion.audit.trail({
    resourceType: "order",
    resourceId: "order_123",
    limit: 100,
    cursor,
  });
  for (const event of page.events) {
    // process event
  }
  cursor = page.nextCursor;
} while (cursor);
```

## Python

```python
import os

from medallion import MedallionClient

medallion = MedallionClient(
    base_url="http://127.0.0.1:7799",
    access_token=os.environ["MEDALLION_SERVICE_ACCOUNT_TOKEN"],
    organization_id="org_123",
)

registered = medallion.connect.register_datasource(
    name="primary_postgres",
    type="postgres",
    idempotency_key="register_primary_postgres",
    display_name="Primary Postgres",
)

medallion.audit.record(
    connector_id=registered.datasource.id,
    actor={"type": "user", "id": "user_123"},
    action="cancel",
    outcome="succeeded",
    resource={"type": "order", "id": "order_123"},
    after={"status": "cancelled"},
    idempotency_key="order_123_cancelled",
)
```

Low-level Connect protobuf types are available as:

```python
from medallion import connect_pb2
```

## Go

```go
client, err := medallion.NewClient(medallion.ClientConfig{
	BaseURL:        "http://127.0.0.1:7799",
	AccessToken:    os.Getenv("MEDALLION_SERVICE_ACCOUNT_TOKEN"),
	OrganizationID: "org_123",
})
if err != nil {
	return err
}

registered, err := client.Connect.RegisterDatasource(ctx, medallion.DatasourceRegistration{
	Name:           "primary_postgres",
	Type:           "postgres",
	IdempotencyKey: "register_primary_postgres",
	DisplayName:    "Primary Postgres",
})
if err != nil {
	return err
}

_, err = client.Audit.Record(ctx, medallion.AuditRecord{
	ConnectorID: registered.Datasource.ID,
	Actor:      medallion.ActorRef{Type: "user", ID: "user_123"},
	Action:     "cancel",
	Outcome:    medallion.AuditOutcomeSucceeded,
	Resource:   medallion.ResourceRef{Type: "order", ID: "order_123"},
	After:      map[string]any{"status": "cancelled"},
	IdempotencyKey: "order_123_cancelled",
})
```

Low-level Connect protobuf types are available as:

```go
import connectv1 "github.com/jim-technologies/medallion-sdk/go/gen/medallion/connect/v1"
```

## Ontology And Actions

```ts
const answer = await medallion.ontology.query({
  question: "What happened to order_123?",
});

const plan = await medallion.ontology.planAction({
  actionName: "order.cancel",
  input: { order_id: "order_123" },
});

const execution = await medallion.ontology.executeAction({
  actionName: "order.cancel",
  input: { order_id: "order_123" },
  idempotencyKey: "order_123_cancel_action",
});
```

`executeAction()` reports the persisted invocation status. It intentionally
does not invent a `duplicate` flag because the current Ontology response does
not expose replay metadata. Its idempotency key is also required and must be
reused for retries of the same intended action.

## Storage Upload

```ts
await medallion.storage.upload({
  org: "org_123",
  path: "exports/order_123.json",
  contentType: "application/json",
  data: JSON.stringify({ id: "order_123" }),
  idempotencyKey: "upload_order_123_export",
});
```

This first storage helper uses the descriptor-backed `StorageService.Upload` RPC. It does not yet expose progress callbacks, resumable uploads, multipart helpers, or retry orchestration. Use it for small audit evidence and ordinary server-side artifacts; large-file UX should use medallion-storage native raw/chunked upload surfaces until SDK multipart/resume semantics are stable.

## Service URLs

Use `baseUrl` when the services are exposed behind one gateway. Override per service when they run on separate hosts:

```ts
const medallion = new MedallionClient({
  baseUrl: "https://api.example.com",
  connectBaseUrl: "https://connect.example.com",
  ontologyBaseUrl: "https://ontology.example.com",
  storageBaseUrl: "https://storage.example.com",
  accessToken: process.env.MEDALLION_SERVICE_ACCOUNT_TOKEN!,
  organizationId: "org_123",
  defaultConnectorId: "conn_123",
});
```

The SDK calls the real client-facing backend routes:

- `POST /medallion.connect.v1.MedallionConnectService/RegisterConnector`
- `POST /medallion.connect.v1.MedallionConnectService/PublishCdcEvents`
- `POST /medallion.connect.v1.MedallionConnectService/ListCdcEvents`
- `POST /medallion.connect.v1.MedallionConnectService/PublishAuditEvents`
- `POST /medallion.connect.v1.MedallionConnectService/ListAuditEvents`
- `POST /rpc/medallion.ontology.v1.MedallionOntologyService/Query`
- `POST /rpc/medallion.ontology.v1.MedallionOntologyService/PlanAction`
- `POST /rpc/medallion.ontology.v1.MedallionOntologyService/ExecuteAction`
- `POST /medallion.storage.v1.StorageService/Upload`

RPC calls are encoded and decoded through vendored invariantprotocol descriptors. Outbound RPC JSON uses protobuf JSON field names, such as `connectorId`, `payloadJson`, and `contentType`.

Low-level protocol-shaped clients are available at `medallion.protocol.*`, but the primary user surface is the ergonomic wrapper API. A deprecated `medallion.generated.*` alias remains for the prototype.

## IDs

Canonical IDs sent over the wire are strings. SDK inputs may use `string`, `number`, or `bigint` for ID fields.

The SDK converts safe integer numbers and bigint values to strings. It rejects unsafe numeric IDs and asks callers to pass them as strings, so precision is not lost. String IDs are preserved exactly, including leading zeros.

This applies to actor IDs, resource IDs, datasource external IDs, CDC primary keys, entity IDs, source event IDs, and other ID-bearing fields.

For CDC, a one-field primary key uses that field's value as `entity_id`. A composite primary key must include an explicit, source-canonical `entityId` (`entity_id` in Python) so identity does not depend on language-specific object serialization. The complete primary key remains in `payload_json`.

## Idempotency

Write helpers accept `idempotencyKey`. The SDK sends it in the request body
where the backend contract requires it and as the `Idempotency-Key` HTTP
header. Datasource registration, audit, CDC, and action execution require a caller-supplied stable key;
reuse the same key for every retry of one logical operation. Storage upload
currently leaves the key optional, but callers should provide one whenever the
write may be retried. The SDK never invents a new event or action key.

## Tracing

OpenTelemetry tracing is supported and off by default. The SDKs depend on the OpenTelemetry API only; applications still own provider, sampler, processor, and exporter setup.

When enabled, the SDK creates client spans around outbound Medallion requests and injects the active trace context into request headers. Spans include method, SDK language, route path, HTTP status, and Medallion request ID when available. They do not include API keys, bearer tokens, request bodies, event payloads, or object metadata.

TypeScript:

```ts
const medallion = new MedallionClient({
  baseUrl: process.env.MEDALLION_BASE_URL!,
  accessToken: process.env.MEDALLION_SERVICE_ACCOUNT_TOKEN!,
  tracing: true,
});
```

Python:

```python
medallion = MedallionClient(
    base_url=os.environ["MEDALLION_BASE_URL"],
    access_token=os.environ["MEDALLION_SERVICE_ACCOUNT_TOKEN"],
    tracing=True,
)
```

Go:

```go
client, err := medallion.NewClient(medallion.ClientConfig{
	BaseURL:     os.Getenv("MEDALLION_BASE_URL"),
	AccessToken: os.Getenv("MEDALLION_SERVICE_ACCOUNT_TOKEN"),
	Tracing:    medallion.TracingConfig{Enabled: true},
})
```

Advanced callers can pass a custom tracer and span prefix:

```ts
new MedallionClient({
  baseUrl,
  accessToken,
  tracing: {
    enabled: true,
    tracer,
    spanPrefix: "medallion-sdk",
  },
});
```

## Contributor Setup

Git consumers do not need Flox to install or use an SDK. For repository
development and CI, Flox is the only toolchain bootstrap:

```sh
flox activate -- make install
flox activate -- make check
flox activate -- make audit
```

The root Flox lock supplies every language runtime, package manager, formatter,
linter, schema checker, secret scanner, and shell/workflow checker. It also
runs version-pinned Go and Python dependency scanners through the locked Go and
uv toolchains. Flox resolves the project-pinned pnpm 11.13.1, while GitHub
Actions uses the same lock plus the small `.ci/node-24` Flox lock to verify the
oldest supported Node runtime.

## Deployed Smoke

Run this only against a locked-down test tenant:

```sh
export MEDALLION_SMOKE_BASE_URL="https://api.example.com"
export MEDALLION_SMOKE_ACCESS_TOKEN="<service-account-oauth-jwt>"
export MEDALLION_SMOKE_ORGANIZATION_ID="org_123"
export MEDALLION_SMOKE_CONNECTOR_ID="conn_123" # optional; omitted means the test registers one
export MEDALLION_SMOKE_EXPECTED_INGESTER_PRINCIPAL="service_account:orders-worker"
export MEDALLION_SMOKE_DENIED_ORGANIZATION_ID="org_denied" # optional cross-tenant denial check

flox activate -- make test-deployed
```

The smoke registers a datasource when needed, records an audit event, reads it back through `audit.trail()`, and asserts source actor, ingester, resource, action, before/after, and evidence URL fields.

GitHub Actions exposes the same check through the manual `deployed_smoke`
workflow input. Configure the protected `sdk-smoke` environment with the four
required variables above as environment secrets; the connector, denied
organization, and evidence URL variables remain optional.

## Validation

```sh
flox activate -- make help
flox activate -- make format
flox activate -- make check
flox activate -- make audit
flox activate -- make git-install-check
```

`test/compatibility.test.ts` fails if SDK route constants drift from the vendored public client-facing route manifest in `proto/client-facing-routes.json` or the generated Connect, Ontology, and Storage descriptors. `test/integration.test.ts` runs the SDK against an in-process HTTP server and verifies the public client methods hit actual backend route shapes with auth and idempotency headers.

When vendored proto contracts change, regenerate every checked-in binding and
descriptor, then verify there is no drift:

```sh
flox activate -- make proto-bindings proto-descriptor generated-check
```

Go and Python already use generated Connect protobuf bindings for their Connect-backed surface.

## License

Apache-2.0. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).

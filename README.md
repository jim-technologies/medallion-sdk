# Medallion external-ingestion SDK

This repository is the thin, server-side customer SDK for publishing CDC and
audit events into Medallion through the stable
`medallion.connect.v1.MedallionConnectService` ingestion identity. Configure the
Medallion API base URL supplied with your integration.

Medallion control-plane workflows provision an integration and provide a
workspace-bound API key, workspace ID, and connector ID. This SDK does not
create credentials or connectors, and it is not an administration, storage,
query, provider, OAuth, or provisioning client.

The stable external-ingestion v1 surface is exactly:

- `PublishCdcEvents`
- `PublishAuditEvents`
- `ListCdcEvents`
- `ListAuditEvents`

Customer code must run on a trusted server. Never put a Medallion API key in a
browser, mobile application, or shipped client bundle.

## Install from Git

All language SDKs use the repository-root [`VERSION`](./VERSION) and the same
Git tag. They are installed from Git only; this project is not published to
npm, PyPI, or another language registry.

Release tags use `vX.Y.Z` and must match `VERSION` for TypeScript, Go, and
Python. Repository development is pinned to Go 1.26.5, pnpm 11.13.1, and
invariantprotocol v0.14.0 (commit
`f924e602582402476f257571852fabb0e1e1cf43`).

```bash
pnpm add 'github:jim-technologies/medallion-sdk#v0.2.0'
npm install --allow-git=all 'git+https://github.com/jim-technologies/medallion-sdk.git#v0.2.0'
```

Go and Python installation commands are documented in
[`go/README.md`](./go/README.md) and [`python/README.md`](./python/README.md).
Pin a release tag or immutable commit rather than a branch.

## Configure one immutable workspace

One client represents exactly one credential/workspace combination. The
workspace ID is required at construction and must match
`^ws_[0-9a-hjkmnp-tv-z]{26}$`. To target another workspace, use a separately
bound credential and construct another client.

```ts
import { MedallionClient } from "@jimtech/medallion";

const medallion = new MedallionClient({
  baseUrl: "https://api.example.com",
  apiKey: process.env.MEDALLION_API_KEY!,
  workspaceId: process.env.MEDALLION_WORKSPACE_ID!,
  defaultConnectorId: process.env.MEDALLION_CONNECTOR_ID!,
  timeoutMs: 20_000,
});
```

The SDK sends the API key only as `X-Medallion-API-Key` and the configured
workspace only as `X-Medallion-Workspace-Id`. A bearer `accessToken` is also
supported for an authorized server-side flow; configure exactly one of
`apiKey` or `accessToken`. The SDK does not acquire or refresh tokens.

The base URL must be the Medallion API HTTPS origin with no path, credentials,
query, or fragment. Plain HTTP is accepted only for explicit loopback
development. Canonical RPC paths are used directly; `/rpc`, REST aliases,
redirects, and fallback hosts are not used.

## Publish CDC

Use a stable key derived from the source record or durable outbox row. Every
event requires its own key.

```ts
const result = await medallion.cdc.record({
  streamName: "orders",
  entityType: "order",
  entityId: "order_8421",
  operation: "insert",
  idempotencyKey: "orders:partition-7:offset-184392",
  sourceEventId: "partition-7/184392",
  occurredAt: "2026-08-03T18:04:12.123456789Z",
  payload: { status: "created", totalCents: 4200 },
});

console.log({
  eventId: result.events[0]?.eventId,
  duplicate: result.events[0]?.duplicate,
});
```

Durable event IDs are returned as decimal strings, so JavaScript never loses
64-bit precision. Event payload objects are encoded as compact deterministic
JSON without mutation. Cyclic values, non-finite numbers, and unsupported JSON
values are rejected locally.

A batch contains between 1 and 1,000 events. Input order and receipt order are
preserved, and duplicate keys in one batch are rejected before network I/O.

```ts
const batch = await medallion.cdc.publishBatch({
  events: [
    {
      streamName: "orders",
      entityType: "order",
      entityId: "order_8421",
      operation: "update",
      idempotencyKey: "orders:partition-7:offset-184393",
      payload: { status: "paid" },
    },
    {
      streamName: "orders",
      entityType: "order",
      entityId: "order_8422",
      operation: "insert",
      idempotencyKey: "orders:partition-7:offset-184394",
      payload: { status: "created" },
    },
  ],
});

for (const receipt of batch.events) {
  console.log(receipt.idempotencyKey, receipt.eventId, receipt.duplicate);
}
```

## Publish audit events

`actor` identifies the source/application actor. The authenticated principal
is the service account submitting the event. `action` is a stable operation
key; `outcome` records the authoritative result and must be concrete.

```ts
const audit = await medallion.audit.record({
  resourceType: "invoice",
  resourceId: "invoice_314",
  action: "invoice.approve",
  outcome: "succeeded",
  actor: { type: "user", id: "user_2718" },
  idempotencyKey: "billing-outbox:982451653",
  payload: {
    approvalId: "approval_1618",
    evidenceRef: "object://audit-evidence/approval_1618.json",
  },
});

if (audit.duplicate) {
  console.log("Already accepted as", audit.events[0]?.eventId);
}
```

Store large evidence externally and send a stable reference. Payload contents
are not logged or traced by default.

## Delivery, retries, and an outbox

Delivery from your application is at least once. Medallion provides durable
server-side idempotency and duplicate detection, so replaying the exact event
with the same source-derived key is safe. This is not transport-level or
end-to-end exactly-once delivery.

When losing an event is unacceptable, write the business change and an outbox
row in the same database transaction. A worker should publish that row, retain
its stable key, and mark it delivered only after decoding a valid receipt.
Replaying an unknown-outcome batch must preserve its exact content and order.

Automatic retry is disabled by default. Enable it deliberately; at most five
total attempts are allowed. Retries use bounded exponential backoff with
jitter, honor `Retry-After`, preserve the exact serialized bytes, and obey the
request deadline and cancellation signal.

```ts
const retrying = new MedallionClient({
  baseUrl: "https://api.example.com",
  apiKey: process.env.MEDALLION_API_KEY!,
  workspaceId: process.env.MEDALLION_WORKSPACE_ID!,
  defaultConnectorId: process.env.MEDALLION_CONNECTOR_ID!,
  timeoutMs: 30_000,
  retry: {
    maxAttempts: 3,
    initialDelayMs: 200,
    maxDelayMs: 2_000,
    jitterRatio: 0.2,
  },
});

const stableKey = "orders:partition-7:offset-184395";
await retrying.cdc.record({
  streamName: "orders",
  entityType: "order",
  entityId: "order_8423",
  operation: "insert",
  idempotencyKey: stableKey,
  payload: { status: "created" },
});
```

Validation, authentication, authorization, scope conflicts, entitlement
failures, and idempotency mismatches are terminal. Unknown structured error
reasons are preserved and never retried automatically.

## Structured errors and backpressure

```ts
import { MedallionApiError } from "@jimtech/medallion";

try {
  await medallion.cdc.record({
    streamName: "orders",
    entityType: "order",
    entityId: "order_8424",
    operation: "update",
    idempotencyKey: "orders:partition-7:offset-184396",
    payload: { status: "fulfilled" },
  });
} catch (error) {
  if (error instanceof MedallionApiError) {
    console.error({
      connectCode: error.connectCode,
      reason: error.errorInfoReason,
      requestId: error.requestId,
    });

    if (error.errorInfoReason === "BACKPRESSURE") {
      // Leave the durable outbox row pending and retry the exact event later.
    }
  }
  throw error;
}
```

Errors expose the Connect/gRPC code, HTTP status, sanitized message, request
ID, decoded `google.rpc.ErrorInfo`, and unknown detail envelopes. They do not
retain raw response bodies, credentials, or event payloads. Branch on code,
domain, and reason—not human-readable message text.

## Read back for verification

Page cursors are opaque and confidential. Pass them back unchanged; do not
decode, store in logs, or infer ordering from them.

```ts
const page = await medallion.cdc.list({
  connectorId: process.env.MEDALLION_CONNECTOR_ID!,
  streamName: "orders",
  limit: 100,
});

for (const event of page.events) {
  console.log(event.eventId, event.idempotencyKey);
}

for await (const event of medallion.audit.iterate({
  resourceType: "invoice",
  resourceId: "invoice_314",
  limit: 100,
})) {
  console.log(event.eventId, event.action, event.outcome);
}
```

`limit: 0` selects the server default of 100; the maximum is 500. Iterators
continue across empty pages with a continuation cursor, reject repeated
cursors, and enforce a maximum page count. The original workspace and filters
remain fixed for the whole iteration.

## Cancellation, deadlines, and tracing

```ts
const controller = new AbortController();

await medallion.audit.list(
  { limit: 100 },
  { signal: controller.signal, timeoutMs: 5_000 },
);
```

`timeoutMs` is sent as `Connect-Timeout-Ms` and covers retries and backoff.
Cancellation propagates through requests and retry waits.

Tracing is optional and uses the application’s OpenTelemetry provider; the SDK
does not install or require a dedicated exporter.

```ts
const traced = new MedallionClient({
  baseUrl: "https://api.example.com",
  apiKey: process.env.MEDALLION_API_KEY!,
  workspaceId: process.env.MEDALLION_WORKSPACE_ID!,
  defaultConnectorId: process.env.MEDALLION_CONNECTOR_ID!,
  tracing: true,
});
```

Telemetry is limited to safe transport metadata. Credentials, authorization
headers, cursors, and payload contents are never attached.

## Migration

Replace a separately configured Connect URL with the Medallion API origin:

```diff
 const medallion = new MedallionClient({
-  connectBaseUrl: process.env.MEDALLION_CONNECT_URL,
+  baseUrl: process.env.MEDALLION_API_URL,
   apiKey: process.env.MEDALLION_API_KEY,
   workspaceId: process.env.MEDALLION_WORKSPACE_ID,
 });
```

Do not append `/rpc`; the SDK uses
`/medallion.connect.v1.MedallionConnectService/<Method>` directly. Workspace
selection is immutable at client construction, and event-level workspace or
connector fields are not accepted.

## Development

Use Flox for generation and verification:

```bash
flox activate -- make contract-sync
flox activate -- make contract-check
flox activate -- make validate
flox activate -- make git-install-check
flox activate -- make audit
```

The vendored, minimal external-ingestion contract permits normal offline
checks. Release validation also requires its immutable release attestation.

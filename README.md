# Medallion SDK

This repository is the server-side customer SDK for getting data into and out
of Medallion. Its main surface is `medallion.ingest.v1.MedallionIngestService`:
declared tables, idempotent batch appends, and read-back SQL queries in the
declared ClickHouse dialect. The shapes follow the widely understood BigQuery
analogs — append is the insertAll analog, and queries are the modernized,
synchronous-first jobs.query analog with transparent poll-and-paginate.

The stable ingest v1 surface is exactly:

- `CreateTable`
- `GetTable`
- `ListTables`
- `UpdateTable`
- `AppendRows`
- `RunQuery`
- `GetQueryResults`

The resource is a TABLE, not a dataset: a workspace already plays the role
BigQuery gives a dataset, so the resource one level down is a table.

The older `medallion.connect.v1` CDC/audit publish surface is DEPRECATED. Its
four RPCs keep working and stay documented [below](#deprecated-cdc-and-audit-publishing),
but new integrations should land data through tables.

## Boundaries

- **Server-side only.** Customer code must run on a trusted server. Never put
  a Medallion API key in a browser, mobile application, or shipped client
  bundle.
- **Not an ORM and not a query builder.** SQL passes through verbatim as the
  declared ClickHouse SQL; the SDK never rewrites, parameterizes, or
  dialect-translates a statement.
- **No Iceberg client wrapper.** A future read-only Iceberg catalog door is
  served by standard clients such as PyIceberg, not by this SDK.
- The SDK does not create credentials or workspaces, and it is not an
  administration, storage, provider, OAuth, or provisioning client.

## Install from Git

All language SDKs use the repository-root [`VERSION`](./VERSION) and the same
Git tag. They are installed from Git only; this project is not published to
npm, PyPI, or another language registry.

Release tags use `vX.Y.Z` and must match `VERSION` for TypeScript, Go, and
Python. Repository development is pinned to Go 1.26.5, pnpm 11.13.1, and
invariantprotocol v0.14.0 (commit
`f924e602582402476f257571852fabb0e1e1cf43`).

```bash
pnpm add 'github:jim-technologies/medallion-sdk#v0.3.0'
npm install --allow-git=all 'git+https://github.com/jim-technologies/medallion-sdk.git#v0.3.0'
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
  timeoutMs: 30_000,
});
```

The SDK sends the API key only as `X-Medallion-API-Key` and the configured
workspace only as `X-Medallion-Workspace-Id`. A bearer `accessToken` is also
supported for an authorized server-side flow; configure exactly one of
`apiKey` or `accessToken`. The SDK does not acquire or refresh tokens.
Workspace identity comes only from the verified transport on the ingest
surface: no ingest request body carries a workspace field.

The base URL must be the Medallion API HTTPS origin with no path, credentials,
query, or fragment. Plain HTTP is accepted only for explicit loopback
development. Canonical RPC paths are used directly; `/rpc`, REST aliases,
redirects, and fallback hosts are not used.

## Tables and appends

A table is one declared tabular collection in the configured workspace: an
ordered schema, a `TIMESTAMP` column carrying event time, and an optional sort
key. Column types are `BOOL`, `INT64`, `FLOAT64`, `STRING`, `BYTES`,
`TIMESTAMP`, `DATE`, and `JSON`. Declare a table, then append batches of JSON
rows:

```ts
await medallion.tables.create({
  tableId: "app_events",
  columns: [
    { name: "at", type: "TIMESTAMP" },
    { name: "level", type: "STRING" },
    { name: "message", type: "STRING", nullable: true },
  ],
  timeColumn: "at",
});

const appended = await medallion.tables.append(
  "app_events",
  [
    { at: "2026-08-29T01:00:00Z", level: "info", message: "service started" },
    { at: "2026-08-29T01:00:02Z", level: "warn", message: "cache is cold" },
  ],
  { insertIds: ["boot:1", "boot:2"] },
);

console.log(appended.acceptedRows, appended.idempotencyKey);
for (const rowError of appended.rowErrors) {
  console.error("rejected", rowError.index, rowError.message);
}
```

Every write carries a batch idempotency key, sent both as the Stripe-style
`Idempotency-Key` header and as the contract's `request_id` field. The SDK
generates one automatically and returns it; pass the same key back to replay
the exact batch safely after a crash or timeout, and the server absorbs the
replay without duplication and re-acknowledges the original counts. Reusing a
key with different content fails with the `IDEMPOTENCY_MISMATCH` reason.
Optional `insertIds` pass a per-row identifier through as each row's
`insert_id`; it correlates row errors and does not deduplicate rows.

Rows can also arrive as one pre-encoded Arrow IPC stream (`Uint8Array` in
TypeScript, `bytes`, a `pyarrow` table, or a `polars.DataFrame` in Python).
An append batch holds 1 through 50,000 rows and at most 16 MiB; per-row
failures are surfaced in `rowErrors` with the row's index and cause. By
default one invalid row rejects the whole batch; `skipInvalidRows` commits the
valid remainder instead.

Schema evolution is additive only. `update()` takes the FULL desired schema —
the existing columns repeated unchanged and in order, then the new columns,
which must be nullable. Resending the current schema is a no-op success, so
retries are safe:

```ts
await medallion.tables.update({
  tableId: "app_events",
  columns: [...existing, { name: "trace_id", type: "STRING", nullable: true }],
});
```

## Queries

`query()` runs one statement in the declared ClickHouse SQL dialect. The call
is synchronous first: if the server finishes within the request's synchronous
budget, rows come back immediately; otherwise the SDK transparently polls
`GetQueryResults` while the query state is `RUNNING`. Iterating the result
walks every page — callers never touch a page token.

```ts
const result = await medallion.tables.query(
  "SELECT level, count() AS events FROM app_events GROUP BY level",
  { serverTimeoutMs: 10_000 },
);

console.log(result.columns); // [{ name: "level", type: "STRING" }, ...]
for await (const row of result) {
  console.log(row.level, row.events);
}
```

`dryRun: true` validates the statement and reports the result schema without
executing it, and without a query resource name to poll. A query that ends in
the `FAILED` state raises the reported cause. Query results are
single-consumption; run the query again to re-read it.

Result rows arrive as JSON objects keyed by output column name. `INT64`
columns come back as a JSON number inside the IEEE-754 safe range and as a
decimal string outside it, so read them through a normalizer if the values can
be large.

## Python first: dataframes in, dataframes out

Ingest users are data people, so the richest convenience layer ships in the
Python SDK (install the `medallion[polars]` extra):

```python
import polars as pl
from medallion import MedallionClient

client = MedallionClient(
    base_url=os.environ["MEDALLION_BASE_URL"],
    api_key=os.environ["MEDALLION_API_KEY"],
    workspace_id=os.environ["MEDALLION_WORKSPACE_ID"],
)

frame = pl.DataFrame({"level": ["info", "warn"], "count": [1, 2]})
client.tables.append("app_events", frame)  # rides as one Arrow IPC stream

frame = client.tables.query(
    "SELECT level, count() AS events FROM app_events GROUP BY level",
).to_polars()
```

TypeScript has full base parity (append, query, table management); its
dataframe conveniences follow when a real consumer needs them. Go ships
generated bindings plus a deliberately thin client.

## Structured errors and retries

```ts
import { MedallionApiError } from "@jimtech/medallion";

try {
  await medallion.tables.append("app_events", rows);
} catch (error) {
  if (error instanceof MedallionApiError) {
    console.error({
      connectCode: error.connectCode,
      reason: error.errorInfoReason,
      requestId: error.requestId,
    });
  }
  throw error;
}
```

Errors expose the Connect/gRPC code, HTTP status, sanitized message, request
ID, decoded `google.rpc.ErrorInfo`, and unknown detail envelopes. They do not
retain raw response bodies, credentials, or row payloads. Branch on code,
domain, and reason — not human-readable message text.

Automatic retry is disabled by default and capped at five total attempts.
Retries use bounded exponential backoff with jitter, honor `Retry-After`,
preserve the exact serialized bytes, and obey the request deadline and
cancellation signal. Appends are retry-safe because the batch idempotency key
makes an exact replay a no-op, never a double write.

## Cancellation, deadlines, and tracing

```ts
const controller = new AbortController();

await medallion.tables.query("SELECT count() FROM app_events", {
  signal: controller.signal,
  timeoutMs: 5_000,
});
```

`timeoutMs` is sent as `Connect-Timeout-Ms` and covers retries and backoff.
Cancellation propagates through requests, polling, and retry waits.

Tracing is optional and uses the application's OpenTelemetry provider; the
SDK does not install or require a dedicated exporter. Telemetry is limited to
safe transport metadata: credentials, tokens, row payloads, and SQL text are
never attached.

## Deprecated: CDC and audit publishing

The `medallion.connect.v1.MedallionConnectService` surface — exactly
`PublishCdcEvents`, `PublishAuditEvents`, `ListCdcEvents`, and
`ListAuditEvents` — is deprecated but still served. Existing integrations
keep working unchanged through `medallion.cdc`, `medallion.audit`, and the
low-level `medallion.connect` clients:

```ts
const receipt = await medallion.cdc.record({
  streamName: "orders",
  entityType: "order",
  entityId: "order_8421",
  operation: "insert",
  idempotencyKey: "orders:partition-7:offset-184392",
  payload: { status: "created" },
});
```

Publishing is at-least-once with durable server-side idempotency: replaying
the exact event with the same source-derived key is safe. Batches hold 1
through 1,000 events, receipts preserve input order, durable event IDs return
as decimal strings, and page cursors on the list RPCs are opaque. When losing
an event is unacceptable, pair the business write with an outbox row in one
database transaction and mark it delivered only after decoding a valid
receipt. The connect surface requires a provisioned `defaultConnectorId`;
the tables surface does not use connectors.

## Development

Use Flox for generation and verification:

```bash
flox activate -- make contract-sync
flox activate -- make contract-check
flox activate -- make validate
flox activate -- make git-install-check
flox activate -- make audit
```

The vendored contracts permit normal offline checks. The ingest contract
tracks the released upstream contract, with three documented deviations
recorded in [`proto/README.md`](./proto/README.md); release validation
additionally requires the immutable release attestation.

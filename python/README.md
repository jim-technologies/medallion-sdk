# Medallion Python SDK

This server-side SDK gets data into and out of Medallion. Its main surface is
`medallion.ingest.v1.MedallionIngestService` — declared tables, idempotent
batch appends (including polars DataFrames), and read-back SQL queries in the
declared ClickHouse dialect with results collected straight into polars.

The deprecated `medallion.connect.v1.MedallionConnectService` publish surface
(`PublishCdcEvents`, `PublishAuditEvents`, `ListCdcEvents`,
`ListAuditEvents`) keeps working and stays documented below. Connector and
credential provisioning remain control-plane operations and are intentionally
absent here.

Install the SDK directly from Git (add the `polars` extra for the dataframe
conveniences):

```sh
uv add "medallion @ git+https://github.com/jim-technologies/medallion-sdk.git@vX.Y.Z#subdirectory=python"
uv add "medallion[polars] @ git+https://github.com/jim-technologies/medallion-sdk.git@vX.Y.Z#subdirectory=python"
```

All language SDKs use the repository-root version and the same `vX.Y.Z` tag.
Use a full commit SHA when a production build requires commit-level pinning.

An administrator first provisions the integration through Medallion's control
plane. Your server application receives a Medallion API base URL, scoped API
key, workspace ID, and connector ID:

Set `MEDALLION_BASE_URL` to that origin, for example
`https://api.example.com`.

```python
import os

from medallion import MedallionClient

client = MedallionClient(
    base_url=os.environ["MEDALLION_BASE_URL"],
    api_key=os.environ["MEDALLION_API_KEY"],
    workspace_id=os.environ["MEDALLION_WORKSPACE_ID"],
    default_connector_id=os.environ["MEDALLION_CONNECTOR_ID"],
)
```

One client represents one immutable, canonical `ws_...` workspace. To target a
different workspace, obtain an appropriately bound credential and construct a
separate client. Per-call workspace overrides are intentionally unavailable.
For an allowed service-account flow, use `access_token` instead of `api_key`;
configuring both is rejected. Never put either credential in browser or mobile
code. `default_connector_id` matters only for the deprecated publish surface;
the tables surface does not use connectors, and workspace identity comes only
from the verified transport.

## Durable execution

Medallion can back a Temporaless workflow runtime. This SDK ships no storage
client of its own: `medallion.workflows` returns Temporaless's own clients,
pointed at your Medallion endpoint with this client's credential and
workspace attached as headers. Install the `medallion[workflows]` extra,
which pins Temporaless v0.10.7.

```python
store = client.workflows.store()        # temporaless ConnectStore
query = client.workflows.query_store()  # temporaless ConnectQueryStore

capabilities = await client.workflows.require_capabilities()
print(capabilities.claim_capability_name)
print(capabilities.event_delivery_capability_name)

greeting = await run(
    store, Options(workflow_id="greet", run_id="1"), request, Reply, greet
)
```

`capabilities()` runs the `GetStoreCapabilities` handshake;
`require_capabilities()` turns a backend that cannot atomically create claims
or deliver events exactly once into a startup error rather than a correctness
bug under concurrency. A runtime that depends on either must not be pointed at
a backend that does not advertise it.

These credentials can delete runs. Keep them server-side, and provision a
separate operator credential for the operator-only RPCs
(`medallion.workflows.OPERATOR_METHODS`): `PutEvent`, the bounded deletions,
and `Sweep`. This SDK ships no operator client.

A caveat when installing: Temporaless depends on protovalidate, which needs a
newer `buf.validate` than this SDK vendors from its attested contract bundle,
and both packages own that module. Install Temporaless **after** medallion so
its complete copy resolves last. The wrong order raises
`MEDALLION_TEMPORALESS_INCOMPATIBLE` with that instruction.

See [`examples/workflows.py`](../examples/workflows.py) for a runnable
quickstart.

## Tables, appends, and queries

A table is one declared tabular collection in the configured workspace: an
ordered schema, a `TIMESTAMP` time column, and an optional sort key. Column
types are `BOOL`, `INT64`, `FLOAT64`, `STRING`, `BYTES`, `TIMESTAMP`, `DATE`,
and `JSON`. Appends take plain dict rows, a `polars.DataFrame`, a
`pyarrow.Table` or `RecordBatch`, or raw Arrow IPC `bytes`. Every write
carries a batch idempotency key — generated automatically, sent as both the
Stripe-style `Idempotency-Key` header and the contract's `request_id` field,
and returned — so the exact batch replays safely; per-row `insert_ids` pass
through as each row's `insert_id` and correlate row errors only.

```python
from medallion import TableColumn

client.tables.create(
    "app_events",
    columns=[
        TableColumn(name="at", type="TIMESTAMP"),
        TableColumn(name="level", type="STRING"),
        TableColumn(name="message", type="STRING", nullable=True),
    ],
    time_column="at",
)

appended = client.tables.append(
    "app_events",
    [
        {"at": "2026-08-29T01:00:00Z", "level": "info", "message": "started"},
        {"at": "2026-08-29T01:00:02Z", "level": "warn", "message": "cold"},
    ],
    insert_ids=["boot:1", "boot:2"],
)
print(appended.accepted_rows, appended.idempotency_key)
for row_error in appended.row_errors:
    print("rejected", row_error.index, row_error.message)

import polars as pl

frame = pl.DataFrame({"level": ["info", "warn"], "count": [1, 2]})
client.tables.append("app_events", frame)  # one Arrow IPC stream
```

Schema evolution is additive only. `client.tables.update()` takes the FULL
desired schema: the existing columns repeated unchanged and in order, then the
new columns, which must be nullable. Resending the current schema is a no-op
success, so retries are safe.

Queries run one statement in the declared ClickHouse SQL dialect, verbatim —
this is not an ORM or a query builder. The call is synchronous first; while
the server reports the query as running the SDK polls transparently, and
iterating the result walks every page without exposing page tokens:

```python
result = client.tables.query(
    "SELECT level, count() AS events FROM app_events GROUP BY level",
    server_timeout_ms=10_000,
)
print([(column.name, column.type) for column in result.columns])
for row in result:
    print(row["level"], row["events"])

frame = client.tables.query(
    "SELECT level, count() AS events FROM app_events GROUP BY level",
).to_polars()
```

`dry_run=True` validates the statement and reports the result schema without
executing it, and without a query resource name to poll. A query that ends in
the `FAILED` state raises the reported cause. Query results are
single-consumption; run the query again to re-read it. Result rows arrive as
dicts keyed by output column name; an `INT64` column comes back as a number
inside the IEEE-754 safe range and as a decimal string outside it.
`client.ingest` exposes the same seven RPCs at the protobuf level
(`ingest_pb2`).

## Deprecated: publish CDC events

Each event needs a stable, source-derived idempotency key of 1–512 UTF-8 bytes.
Any non-empty valid Unicode string is accepted exactly as supplied, including
whitespace and control characters; the key is an event-body field, not an HTTP
header. Do not trim, normalize, or regenerate it during delivery retries.
The SDK does not accept workspace or connector identity at event level,
observed time, source system, or authenticated-ingester fields because
Medallion derives them from trusted request context.

```python
receipt = client.cdc.record(
    stream_name="orders",
    entity_type="order",
    entity_id="order-1842",
    operation="update",
    idempotency_key="postgres:orders:partition-3:lsn-9A/BC",
    source_event_id="debezium-event-1842",
    occurred_at="2026-08-01T17:30:12.123456789Z",
    payload={"after": {"status": "paid", "totalCents": 4200}},
)

for event in receipt.events:
    print(event.event_id, event.duplicate)  # event_id is a lossless string
```

Publish up to 1,000 events without changing their order:

```python
from medallion import CdcEventInput

receipt = client.cdc.publish_batch(
    [
        CdcEventInput(
            stream_name="orders",
            entity_type="order",
            entity_id=row.order_id,
            operation=row.operation,
            idempotency_key=row.outbox_id,
            payload=row.payload,
            occurred_at=row.occurred_at,
        )
        for row in claimed_outbox_rows
    ]
)
```

## Deprecated: publish audit events

`actor` is the source application principal that performed the business action;
it is distinct from the service account authenticating this request. `action`
is a stable operation key and `outcome` is the authoritative result.

```python
receipt = client.audit.record(
    resource_type="invoice",
    resource_id="invoice-842",
    action="approved",
    outcome="succeeded",
    actor={"type": "user", "id": "user-17"},
    idempotency_key="billing-audit:evt-9921",
    payload={"evidenceRef": "s3://audit-evidence/evt-9921.json"},
)
```

Use `AuditEventInput` with `client.audit.publish_batch(...)` for batches. Store
large evidence externally and publish stable references instead of large event
payloads.

## Delivery, duplicates, and retries

Customer delivery can be at least once. Medallion provides durable idempotent
ingestion and duplicate detection; this is not magical transport-level exactly
once delivery. When losing an event is unacceptable, use a transactional outbox
or an equivalent durable queue:

1. Commit the business change and an outbox row in one transaction.
2. Use the durable outbox identity as the event idempotency key.
3. Publish the claimed row or batch.
4. Mark it delivered only after decoding the complete receipt.

Automatic retries are opt-in (`max_attempts` defaults to one). When enabled,
they resend the exact serialized batch, in the same order, with the same keys.
The SDK retries only idempotent ingestion/readback operations and transient
backpressure or availability failures. Validation, authentication,
authorization, workspace conflicts, and idempotency mismatches are terminal.

```python
from medallion import RetryConfig

client = MedallionClient(
    base_url=os.environ["MEDALLION_BASE_URL"],
    api_key=os.environ["MEDALLION_API_KEY"],
    workspace_id=os.environ["MEDALLION_WORKSPACE_ID"],
    default_connector_id=os.environ["MEDALLION_CONNECTOR_ID"],
    timeout=20,
    retry=RetryConfig(max_attempts=3, initial_backoff=0.1, max_backoff=2),
)

if receipt.duplicate_count:
    for event in receipt.events:
        if event.duplicate:
            print("already accepted as", event.event_id)
```

The optional `stable_idempotency_key(namespace, *source_identity)` helper makes
a deterministic UUID-based key. Persisted source IDs remain preferable when
available; never generate a new key inside a retry loop.

## Structured errors

```python
from medallion import KnownErrorReason, MedallionAPIError

try:
    publish_from_outbox()
except MedallionAPIError as error:
    if error.reason == KnownErrorReason.IDEMPOTENCY_MISMATCH:
        quarantine_conflicting_outbox_row(error.metadata)
    elif error.reason == KnownErrorReason.BACKPRESSURE and error.is_retryable(
        idempotent=True
    ):
        reschedule_outbox_delivery()
    else:
        raise
```

Errors retain the Connect code, HTTP status, request ID, decoded
`google.rpc.ErrorInfo`, and unknown additive details. Human message text is not
a stable branching contract. Credentials and raw HTTP bodies are not retained
in errors or tracing.

## Deprecated: read back events

Raw page methods preserve the server's opaque cursor:

```python
page = client.cdc.list(stream_name="orders", page_size=100)
next_page = client.cdc.list(
    stream_name="orders",
    page_size=100,
    cursor=page.next_cursor,
)
```

`limit=0` uses the server default of 100 and the maximum is 500. Cursors are
confidential opaque values, limited to 2048 bytes; do not decode, log, or
modify them. `occurred_at_from` is inclusive and `occurred_at_to` is exclusive.
Audit `resource_type` and `resource_id` filters must be supplied together. The
SDK preserves the server's `observedAt DESC, id DESC` order without sorting or
deduplicating results.

Iterators retain the original workspace and filters, continue through empty
pages with continuation cursors, reject repeated cursors, and stop at a bounded
page count. List requests repeat the configured workspace in both the protected
header and request body. A response containing an event from any other
workspace fails closed.

```python
for event in client.audit.iterate(resource_type="invoice", resource_id="invoice-842"):
    verify(event)
```

Pass a `threading.Event` as `cancellation_event` for cooperative cancellation;
the total `timeout` is propagated as `Connect-Timeout-Ms` and bounds network
I/O and retries.

## Optional tracing

Tracing uses the application's OpenTelemetry provider and exporter. Payloads
and credentials are never added to SDK spans.

```python
client = MedallionClient(
    base_url=os.environ["MEDALLION_BASE_URL"],
    api_key=os.environ["MEDALLION_API_KEY"],
    workspace_id=os.environ["MEDALLION_WORKSPACE_ID"],
    default_connector_id=os.environ["MEDALLION_CONNECTOR_ID"],
    tracing=True,
)
```

## Transport

`base_url` is the Medallion API origin without a path, query, fragment, or
embedded credentials. The SDK uses only canonical Connect paths and rejects
redirects; it has no legacy compatibility prefix, alternate Connect URL,
generic dispatcher, or connector-provisioning API.

API keys are manually provisioned through Medallion's control plane for the
initial release. The SDK never creates, rotates, exchanges, or refreshes them.

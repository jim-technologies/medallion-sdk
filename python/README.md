# Medallion Python SDK

This server-side SDK gets data into and out of Medallion. Its main surface is
`medallion.ingest.v1.MedallionIngestService` — declared tables, idempotent
batch appends (including polars DataFrames), and read-back SQL queries in the
declared ClickHouse dialect with results collected straight into polars.

This branch is the unreleased 0.5.0 candidate. CDC/audit publishing clients
and their generated contract subset are removed. See the
[migration and release blockers](../docs/migration-0.5.md). Provisioning remains
a control-plane operation and is absent here.

Install the SDK directly from Git (add the `polars` extra for the dataframe
conveniences):

```sh
uv add "medallion @ git+https://github.com/jim-technologies/medallion-sdk.git@v0.3.1#subdirectory=python"
uv add "medallion[polars] @ git+https://github.com/jim-technologies/medallion-sdk.git@v0.3.1#subdirectory=python"
```

All language SDKs use the repository-root version and the same `vX.Y.Z` tag.
Use a full commit SHA when a production build requires commit-level pinning.

Before upgrading an environment with older SDK or Temporaless wheels, create
a fresh virtual environment or uninstall **all** distributions that own
`buf/validate/validate_pb2.py`. Ordinary in-place pip upgrades can install the
new shared dependency and then remove its files while uninstalling an old
owner. For an environment using both libraries:

```sh
python -m pip uninstall -y medallion temporaless
python -m pip install "medallion[workflows] @ git+https://github.com/jim-technologies/medallion-sdk.git@COMMIT_SHA#subdirectory=python"
```

Include any other old owner in the uninstall step and install only versions
that use the shared dependency. Rebuild an environment whose imports were
already broken by an ordinary upgrade. Rollback to an older version also
requires a fresh environment; do not mix vendored and shared schema owners.

An administrator first provisions the integration through Medallion's control
plane. Your server application receives a Medallion API base URL, scoped API
key and workspace ID:

Set `MEDALLION_BASE_URL` to that origin, for example
`https://api.example.com`.

```python
import os

from medallion import MedallionClient

client = MedallionClient(
    base_url=os.environ["MEDALLION_BASE_URL"],
    api_key=os.environ["MEDALLION_API_KEY"],
    workspace_id=os.environ["MEDALLION_WORKSPACE_ID"],
)
```

One client represents one immutable, canonical `ws_...` workspace. To target a
different workspace, obtain an appropriately bound credential and construct a
separate client. Per-call workspace overrides are intentionally unavailable.
For an allowed service-account flow, use `access_token` instead of `api_key`;
configuring both is rejected. Never put either credential in browser or mobile
code.

## Durable execution

Medallion can back a Temporaless workflow runtime. This SDK ships no storage
client of its own: `medallion.workflows` returns Temporaless's own clients,
pointed at your Medallion endpoint with this client's credential and
workspace attached as headers. Install the `medallion[workflows]` extra,
which pins Temporaless v0.12.2.

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

The SDK and its pinned Temporaless release use one shared `buf.validate`
distribution from Buf. Its generated Python and typing wheels are pinned by
immutable URL and SHA256; clean pip installs need no extra index or special
installation order. Neither new library bundles a competing copy of that
module; the legacy upgrade procedure above still applies.

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
the server reports the query as running the SDK polls transparently, paced by
the client's retry backoff (0.2 s doubling to 2 s by default, whether or not
retries are enabled; a `Retry-After` on a running answer replaces that wait,
up to 30 s), and iterating the result walks every page without exposing page
tokens:

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

## Delivery and retries

Use one stable batch idempotency key for every retry of a table append. Keep
the exact serialized rows and options unchanged; mark an outbox entry delivered
only after decoding the complete acknowledgement. Rows use the declared ingest
contract rather than CDC/audit envelopes. Retries are opt-in and deadline bounded;
validation, identity, authorization and idempotency conflicts remain terminal.

## Structured errors

```python
from medallion import KnownErrorReason, MedallionAPIError

try:
    append_from_outbox()
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

## Optional tracing

Tracing uses the application's OpenTelemetry provider and exporter. Payloads
and credentials are never added to SDK spans.

```python
client = MedallionClient(
    base_url=os.environ["MEDALLION_BASE_URL"],
    api_key=os.environ["MEDALLION_API_KEY"],
    workspace_id=os.environ["MEDALLION_WORKSPACE_ID"],
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

"""Tables quickstart: declare a table, append rows, replay safely, query back.

Run from a trusted server with the SDK installed from Git (and polars for the
dataframe conveniences: ``pip install "medallion[polars] @ git+https://...``).
Never ship the API key to a browser.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime

import polars as pl

from medallion import MedallionClient, TableColumn


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is required.")
    return value


run_id = f"example_{int(time.time() * 1000)}"
occurred_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
medallion = MedallionClient(
    base_url=required_env("MEDALLION_BASE_URL"),
    api_key=required_env("MEDALLION_API_KEY"),
    workspace_id=required_env("MEDALLION_WORKSPACE_ID"),
    timeout=30.0,
)

# A table is one declared tabular collection in the client's workspace: an
# ordered schema, a TIMESTAMP column carrying event time, and an optional sort
# key. Re-declaring the same table returns the existing one, so retries are
# safe.
columns = [
    TableColumn(name="occurred_at", type="TIMESTAMP"),
    TableColumn(name="run_id", type="STRING"),
    TableColumn(name="seq", type="INT64"),
    TableColumn(name="level", type="STRING"),
    TableColumn(name="message", type="STRING", nullable=True),
]
table = medallion.tables.create(
    "app_events", columns=columns, time_column="occurred_at"
)
print("table", table.table_id, table.create_time)

# Schema evolution is additive only: send the FULL desired schema with the
# existing columns unchanged, then the new nullable ones.
evolved = medallion.tables.update(
    "app_events",
    columns=[*columns, TableColumn(name="trace_id", type="STRING", nullable=True)],
)
print("columns", [column.name for column in evolved.columns])

# Append plain dict rows. The SDK generates a batch idempotency key, sends it
# as the Idempotency-Key header and as the request's request_id, and returns
# it so the exact batch can be replayed safely.
rows = [
    {
        "occurred_at": occurred_at,
        "run_id": run_id,
        "seq": 1,
        "level": "info",
        "message": "service started",
    },
    {
        "occurred_at": occurred_at,
        "run_id": run_id,
        "seq": 2,
        "level": "warn",
        "message": "cache is cold",
    },
]
appended = medallion.tables.append(
    "app_events",
    rows,
    insert_ids=[f"{run_id}:1", f"{run_id}:2"],
)
print("appended", appended.accepted_rows, appended.idempotency_key)
for row_error in appended.row_errors:
    print("rejected row", row_error.index, row_error.message)

# Replaying the exact batch under the same key is absorbed without
# duplication and re-acknowledged with the original counts.
replay = medallion.tables.append(
    "app_events", rows, idempotency_key=appended.idempotency_key
)
print("replayed", replay.accepted_rows)

# A polars DataFrame appends as one Arrow IPC stream, no manual conversion.
frame = pl.DataFrame(
    {
        "occurred_at": [occurred_at, occurred_at],
        "run_id": [run_id, run_id],
        "seq": [3, 4],
        "level": ["info", "info"],
        "message": ["worker online", "queue drained"],
    }
)
print("appended dataframe", medallion.tables.append("app_events", frame).accepted_rows)

# Plan first: dry_run validates the ClickHouse SQL and reports the result
# schema without executing it.
planned = medallion.tables.query(
    "SELECT level, count() AS events FROM app_events GROUP BY level",
    dry_run=True,
)
print("planned columns", [f"{column.name} {column.type}" for column in planned.columns])

# Queries are synchronous first; if the server is still running the statement
# the SDK polls transparently, and iterating crosses every page without
# exposing page tokens.
result = medallion.tables.query(
    f"SELECT run_id, seq, level, message FROM app_events "
    f"WHERE run_id = '{run_id}' ORDER BY seq",
    server_timeout_ms=10_000,
)
print("total rows", result.total_rows)
for row in result:
    print("row", row)

# Or collect a result straight into polars.
collected = medallion.tables.query(
    f"SELECT seq, level FROM app_events WHERE run_id = '{run_id}' ORDER BY seq",
).to_polars()
print(collected)

for item in medallion.tables.iterate():
    print("workspace table", item.table_id)

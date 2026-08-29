"""Datasets quickstart: append rows, replay a batch safely, query them back.

Run from a trusted server with the SDK installed from Git (and polars for the
dataframe conveniences: ``pip install "medallion[polars] @ git+https://...``).
Never ship the API key to a browser.
"""

from __future__ import annotations

import os
import time

import polars as pl

from medallion import MedallionClient


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"{name} is required.")
    return value


run_id = f"example_{int(time.time() * 1000)}"
medallion = MedallionClient(
    base_url=required_env("MEDALLION_BASE_URL"),
    api_key=required_env("MEDALLION_API_KEY"),
    workspace_id=required_env("MEDALLION_WORKSPACE_ID"),
    timeout=30.0,
)

# Datasets are named tabular collections in the client's workspace.
dataset = medallion.datasets.create(
    "app_events",
    description="Application events appended by the quickstart",
)
print("dataset", dataset.dataset_id, dataset.create_time)

# Append plain dict rows. The SDK sends a Stripe-style Idempotency-Key header
# automatically and returns it, so the exact batch can be replayed safely.
rows = [
    {"run_id": run_id, "seq": 1, "level": "info", "message": "service started"},
    {"run_id": run_id, "seq": 2, "level": "warn", "message": "cache is cold"},
]
appended = medallion.datasets.append(
    "app_events",
    rows,
    insert_ids=[f"{run_id}:1", f"{run_id}:2"],
)
print("appended", appended.accepted_rows, appended.idempotency_key)
for row_error in appended.row_errors:
    print("rejected row", row_error.index, row_error.reason)

# Replaying the same batch with the same key is acknowledged as duplicate.
replay = medallion.datasets.append(
    "app_events", rows, idempotency_key=appended.idempotency_key
)
print("replayed", replay.duplicate)

# A polars DataFrame appends as one Arrow IPC stream, no manual conversion.
frame = pl.DataFrame(
    {
        "run_id": [run_id, run_id],
        "seq": [3, 4],
        "level": ["info", "info"],
        "message": ["worker online", "queue drained"],
    }
)
print("appended dataframe", medallion.datasets.append("app_events", frame).accepted_rows)

# Estimate first: dry_run validates the ClickHouse SQL and reports cost
# without executing it.
estimate = medallion.datasets.query(
    "SELECT level, count() AS events FROM app_events GROUP BY level",
    dry_run=True,
)
print("estimated bytes", estimate.total_bytes_processed)

# Queries are synchronous first; if the server is still running the statement
# the SDK polls transparently, and iterating crosses every page without
# exposing page tokens.
result = medallion.datasets.query(
    f"SELECT run_id, seq, level, message FROM app_events "
    f"WHERE run_id = '{run_id}' ORDER BY seq",
    server_timeout_ms=10_000,
)
print("columns", [f"{column.name} {column.type}" for column in result.columns])
for row in result:
    print("row", row)

# Or collect a result straight into polars (arrow format skips JSON entirely).
collected = medallion.datasets.query(
    f"SELECT seq, level FROM app_events WHERE run_id = '{run_id}' ORDER BY seq",
    format="arrow",
).to_polars()
print(collected)

for item in medallion.datasets.iterate():
    print("workspace dataset", item.dataset_id)

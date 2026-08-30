"""Opt-in live tests for the tables surface against a deployed endpoint.

Skipped unless the MEDALLION_SMOKE_* environment names a reachable endpoint,
credential, workspace, and target table; ``make validate`` never sets them.
"""

from __future__ import annotations

import os
import re
import time
import unittest
from datetime import UTC, datetime

from medallion import MedallionClient

REQUIRED_ENV = (
    "MEDALLION_SMOKE_BASE_URL",
    "MEDALLION_SMOKE_API_KEY",
    "MEDALLION_SMOKE_WORKSPACE_ID",
    "MEDALLION_SMOKE_INGEST_TABLE",
)

_live_enabled = all(os.environ.get(name, "").strip() for name in REQUIRED_ENV)


@unittest.skipUnless(_live_enabled, "MEDALLION_SMOKE_* live environment not set")
class LiveTablesTests(unittest.TestCase):
    def test_append_and_read_back_with_sql(self) -> None:
        table = os.environ["MEDALLION_SMOKE_INGEST_TABLE"].strip()
        # The table name is spliced into SQL below, so restrict it to a safe
        # identifier instead of trusting the environment.
        self.assertRegex(table, re.compile(r"^[a-z][a-z0-9_]*$"))
        client = MedallionClient(
            base_url=os.environ["MEDALLION_SMOKE_BASE_URL"].strip(),
            api_key=os.environ["MEDALLION_SMOKE_API_KEY"].strip(),
            workspace_id=os.environ["MEDALLION_SMOKE_WORKSPACE_ID"].strip(),
            timeout=30.0,
        )
        run_id = f"sdk_live_{int(time.time() * 1000)}"
        occurred_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

        found = client.tables.get(table)
        self.assertEqual(found.table_id, table)

        rows = [
            {"occurred_at": occurred_at, "run_id": run_id, "seq": 1, "level": "info"},
            {"occurred_at": occurred_at, "run_id": run_id, "seq": 2, "level": "warn"},
        ]
        appended = client.tables.append(table, rows)
        self.assertEqual(appended.row_errors, [])
        self.assertEqual(appended.accepted_rows, 2)

        # Replaying the exact batch under the same key is absorbed without
        # duplication and re-acknowledged with the original counts.
        replay = client.tables.append(
            table, rows, idempotency_key=appended.idempotency_key
        )
        self.assertEqual(replay.accepted_rows, 2)

        result = client.tables.query(
            f"SELECT run_id, seq, level FROM {table} "
            f"WHERE run_id = '{run_id}' ORDER BY seq",
            server_timeout_ms=20_000,
        )
        read_back = list(result)
        self.assertEqual(len(read_back), 2)
        self.assertEqual(read_back[0]["level"], "info")

        planned = client.tables.query(f"SELECT count() FROM {table}", dry_run=True)
        self.assertTrue(planned.dry_run)
        self.assertTrue(planned.columns)


if __name__ == "__main__":
    unittest.main()

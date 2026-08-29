"""Opt-in live tests for the datasets surface against a deployed endpoint.

Skipped unless the MEDALLION_SMOKE_* environment names a reachable endpoint,
credential, workspace, and target dataset; ``make validate`` never sets them.
"""

from __future__ import annotations

import os
import re
import time
import unittest

from medallion import MedallionClient

REQUIRED_ENV = (
    "MEDALLION_SMOKE_BASE_URL",
    "MEDALLION_SMOKE_API_KEY",
    "MEDALLION_SMOKE_WORKSPACE_ID",
    "MEDALLION_SMOKE_INGEST_DATASET",
)

_live_enabled = all(os.environ.get(name, "").strip() for name in REQUIRED_ENV)


@unittest.skipUnless(_live_enabled, "MEDALLION_SMOKE_* live environment not set")
class LiveDatasetsTests(unittest.TestCase):
    def test_append_and_read_back_with_sql(self) -> None:
        dataset = os.environ["MEDALLION_SMOKE_INGEST_DATASET"].strip()
        # The dataset name is spliced into SQL below, so restrict it to a
        # safe identifier instead of trusting the environment.
        self.assertRegex(dataset, re.compile(r"^[A-Za-z0-9_]+$"))
        client = MedallionClient(
            base_url=os.environ["MEDALLION_SMOKE_BASE_URL"].strip(),
            api_key=os.environ["MEDALLION_SMOKE_API_KEY"].strip(),
            workspace_id=os.environ["MEDALLION_SMOKE_WORKSPACE_ID"].strip(),
            timeout=30.0,
        )
        run_id = f"sdk_live_{int(time.time() * 1000)}"

        found = client.datasets.get(dataset)
        self.assertEqual(found.dataset_id, dataset)

        rows = [
            {"run_id": run_id, "seq": 1, "level": "info"},
            {"run_id": run_id, "seq": 2, "level": "warn"},
        ]
        appended = client.datasets.append(dataset, rows)
        self.assertEqual(appended.row_errors, [])
        self.assertEqual(appended.accepted_rows, 2)

        replay = client.datasets.append(
            dataset, rows, idempotency_key=appended.idempotency_key
        )
        self.assertTrue(replay.duplicate)

        result = client.datasets.query(
            f"SELECT run_id, seq, level FROM {dataset} "
            f"WHERE run_id = '{run_id}' ORDER BY seq",
            server_timeout_ms=20_000,
        )
        read_back = list(result)
        self.assertEqual(len(read_back), 2)
        self.assertEqual(read_back[0]["level"], "info")

        estimate = client.datasets.query(f"SELECT count() FROM {dataset}", dry_run=True)
        self.assertTrue(estimate.dry_run)


if __name__ == "__main__":
    unittest.main()

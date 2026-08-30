"""Opt-in live tests for the durable-execution surface.

Skipped unless MEDALLION_SMOKE_* names a reachable endpoint, credential, and
workspace whose Medallion deployment serves the temporaless.v1 storage
services. `make validate` never sets them.
"""

from __future__ import annotations

import asyncio
import os
import unittest

from medallion import MedallionClient

REQUIRED_ENV = (
    "MEDALLION_SMOKE_BASE_URL",
    "MEDALLION_SMOKE_API_KEY",
    "MEDALLION_SMOKE_WORKSPACE_ID",
    "MEDALLION_SMOKE_WORKFLOWS",
)

_live_enabled = all(os.environ.get(name, "").strip() for name in REQUIRED_ENV)


@unittest.skipUnless(_live_enabled, "MEDALLION_SMOKE_* workflows env not set")
class LiveWorkflowsTests(unittest.TestCase):
    def _client(self) -> MedallionClient:
        return MedallionClient(
            base_url=os.environ["MEDALLION_SMOKE_BASE_URL"].strip(),
            api_key=os.environ["MEDALLION_SMOKE_API_KEY"].strip(),
            workspace_id=os.environ["MEDALLION_SMOKE_WORKSPACE_ID"].strip(),
            timeout=30.0,
        )

    def test_capability_handshake_round_trips(self) -> None:
        client = self._client()
        capabilities = asyncio.run(client.workflows.capabilities())

        # The handshake must answer with a capability this Temporaless
        # release understands, whatever the deployment advertises.
        self.assertIn(
            capabilities.claim_capability_name,
            {
                "CLAIM_CAPABILITY_NO_CLAIMS",
                "CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS",
            },
        )
        self.assertIn(
            capabilities.event_delivery_capability_name,
            {
                "EVENT_DELIVERY_CAPABILITY_NO_ATOMIC_CREATE",
                "EVENT_DELIVERY_CAPABILITY_CREATE_ONLY",
            },
        )
        print(
            "live store capabilities:",
            capabilities.claim_capability_name,
            capabilities.event_delivery_capability_name,
        )

    def test_a_missing_run_reads_back_as_absent(self) -> None:
        from temporaless.storage import WorkflowKey

        client = self._client()
        store = client.workflows.store()
        record = asyncio.run(
            store.get_workflow(
                WorkflowKey(workflow_id="sdk-live-absent", run_id="never-created")
            )
        )
        self.assertIsNone(record)


if __name__ == "__main__":
    unittest.main()

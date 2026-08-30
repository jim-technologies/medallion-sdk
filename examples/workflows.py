"""Workflows quickstart: use Medallion as a Temporaless durable backend.

Install with the extra (``medallion[workflows]``) and run from a trusted
server. This surface can delete runs; never ship these credentials to a
browser or a mobile bundle.
"""

import asyncio
import os

from google.protobuf.wrappers_pb2 import StringValue
from temporaless.v1 import temporaless_pb2
from temporaless.workflow import Options, Workflow, run

from medallion import MedallionClient


async def shout(request: StringValue) -> StringValue:
    return StringValue(value=f"hello {request.value}")


async def greet(workflow: Workflow, request: StringValue) -> StringValue:
    return await workflow.activity(shout, request, activity_id="shout:1")


async def main() -> None:
    medallion = MedallionClient(
        base_url=os.environ["MEDALLION_BASE_URL"],
        api_key=os.environ["MEDALLION_API_KEY"],
        workspace_id=os.environ["MEDALLION_WORKSPACE_ID"],
    )

    # Refuse to start unless the backend can atomically create claims and
    # deliver events exactly once.
    capabilities = await medallion.workflows.require_capabilities()
    print("claims:", capabilities.claim_capability_name)
    print("events:", capabilities.event_delivery_capability_name)

    store = medallion.workflows.store()
    greeting = await run(
        store,
        Options(workflow_id="greet:world", run_id="2026-08-29"),
        StringValue(value="world"),
        StringValue,
        greet,
    )
    print("result:", greeting.value)

    query = medallion.workflows.query_store()
    records, _next_page = await query.list_workflows(
        "", "greet:world", temporaless_pb2.WORKFLOW_STATUS_COMPLETED
    )
    for record in records:
        print("run:", record.key.run_id, record.status)


asyncio.run(main())

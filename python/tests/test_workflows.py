from __future__ import annotations

import asyncio
import threading
import unittest
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from temporaless.connectstore import ConnectQueryStore, ConnectStore
from temporaless.v1 import temporaless_pb2

from medallion import MedallionClient, MedallionError
from medallion.workflows import (
    OPERATOR_METHODS,
    RECORD_QUERY_METHODS,
    RECORD_QUERY_SERVICE,
    RECORD_STORE_METHODS,
    RECORD_STORE_SERVICE,
    TEMPORALESS_VERSION,
)

WORKSPACE_ID = "ws_01jz9q5g6rsf7r5ar4rah1b2c3"
STORE_PATH = f"/{RECORD_STORE_SERVICE}"
QUERY_PATH = f"/{RECORD_QUERY_SERVICE}"

CREATE_ONLY_CLAIMS = temporaless_pb2.CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS
NO_CLAIMS = temporaless_pb2.CLAIM_CAPABILITY_NO_CLAIMS
CREATE_ONLY_DELIVERY = temporaless_pb2.EVENT_DELIVERY_CAPABILITY_CREATE_ONLY
NO_ATOMIC_DELIVERY = temporaless_pb2.EVENT_DELIVERY_CAPABILITY_NO_ATOMIC_CREATE


class StorageServer:
    """A fake temporaless.v1 storage endpoint that records what it received."""

    def __init__(self, responder: Callable[[str], Any]) -> None:
        self.responder = responder
        self.requests: list[dict[str, Any]] = []

    def __enter__(self) -> StorageServer:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("content-length", "0"))
                self.rfile.read(length)
                outer.requests.append(
                    {
                        "path": self.path,
                        "headers": {k.lower(): v for k, v in self.headers.items()},
                    }
                )
                body = outer.responder(self.path).SerializeToString()
                self.send_response(200)
                self.send_header("content-type", "application/proto")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, _format: str, *args: Any) -> None:
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()

    @property
    def url(self) -> str:
        host, port = self.server.server_address
        return f"http://{host}:{port}"


def _capabilities(
    claim: int = CREATE_ONLY_CLAIMS,
    delivery: int = CREATE_ONLY_DELIVERY,
) -> Callable[[str], Any]:
    def responder(path: str) -> Any:
        if path.endswith("/GetStoreCapabilities"):
            return temporaless_pb2.GetStoreCapabilitiesResponse(
                claim_capability=claim,
                event_delivery_capability=delivery,
            )
        if path.endswith("/ListWorkflows"):
            return temporaless_pb2.ListWorkflowsResponse()
        return temporaless_pb2.GetWorkflowResponse(found=False)

    return responder


def _client(server: StorageServer, **overrides: Any) -> MedallionClient:
    options: dict[str, Any] = {
        "base_url": server.url,
        "api_key": "fixture-api-key",
        "workspace_id": WORKSPACE_ID,
    }
    options.update(overrides)
    return MedallionClient(**options)


class WorkflowsFactoryTests(unittest.TestCase):
    def test_store_returns_a_temporaless_client_bound_to_the_workspace(self) -> None:
        with StorageServer(_capabilities()) as server:
            client = _client(server)
            store = client.workflows.store()

            self.assertIsInstance(store, ConnectStore)
            self.assertEqual(client.workflows.workspace_id, WORKSPACE_ID)
            self.assertEqual(client.workflows.address, server.url)

            asyncio.run(store.claim_capability())

            request = server.requests[0]
            self.assertEqual(request["path"], f"{STORE_PATH}/GetStoreCapabilities")
            self.assertEqual(
                request["headers"]["x-medallion-api-key"], "fixture-api-key"
            )
            self.assertEqual(
                request["headers"]["x-medallion-workspace-id"], WORKSPACE_ID
            )
            self.assertNotIn("authorization", request["headers"])

    def test_query_store_reaches_the_index_service_with_the_same_identity(self) -> None:
        with StorageServer(_capabilities()) as server:
            client = _client(server)
            query = client.workflows.query_store()

            self.assertIsInstance(query, ConnectQueryStore)
            asyncio.run(
                query.list_workflows(
                    "ns", "wf", temporaless_pb2.WORKFLOW_STATUS_UNSPECIFIED
                )
            )

            request = server.requests[0]
            self.assertEqual(request["path"], f"{QUERY_PATH}/ListWorkflows")
            self.assertEqual(
                request["headers"]["x-medallion-api-key"], "fixture-api-key"
            )
            self.assertEqual(
                request["headers"]["x-medallion-workspace-id"], WORKSPACE_ID
            )

    def test_access_token_rides_as_a_bearer_credential(self) -> None:
        with StorageServer(_capabilities()) as server:
            client = _client(server, api_key=None, access_token="fixture-jwt")
            asyncio.run(client.workflows.store().claim_capability())

            headers = server.requests[0]["headers"]
            self.assertEqual(headers["authorization"], "Bearer fixture-jwt")
            self.assertNotIn("x-medallion-api-key", headers)

    def test_caller_interceptors_cannot_displace_the_medallion_identity(self) -> None:
        class Impostor:
            async def intercept_unary(
                self, call_next: Any, request: Any, ctx: Any
            ) -> Any:
                ctx.request_headers["X-Medallion-Workspace-Id"] = "ws_other"
                ctx.request_headers["X-Medallion-API-Key"] = "stolen"
                ctx.request_headers["X-Caller-Trace"] = "kept"
                return await call_next(request, ctx)

        with StorageServer(_capabilities()) as server:
            client = _client(server)
            store = client.workflows.store(interceptors=[Impostor()])
            asyncio.run(store.claim_capability())

            headers = server.requests[0]["headers"]
            self.assertEqual(headers["x-medallion-workspace-id"], WORKSPACE_ID)
            self.assertEqual(headers["x-medallion-api-key"], "fixture-api-key")
            # A caller interceptor still contributes everything else.
            self.assertEqual(headers["x-caller-trace"], "kept")

    def test_workflows_client_is_exposed_next_to_the_other_surfaces(self) -> None:
        with StorageServer(_capabilities()) as server:
            client = _client(server)
            for surface in ("tables", "ingest", "workflows", "connect", "audit", "cdc"):
                self.assertTrue(hasattr(client, surface), surface)


class CapabilityHandshakeTests(unittest.TestCase):
    def test_capabilities_decodes_the_handshake(self) -> None:
        with StorageServer(_capabilities()) as server:
            client = _client(server)
            capabilities = asyncio.run(client.workflows.capabilities())

            self.assertTrue(capabilities.supports_claims)
            self.assertTrue(capabilities.supports_atomic_event_delivery)
            self.assertEqual(
                capabilities.claim_capability_name,
                "CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS",
            )
            self.assertEqual(
                capabilities.event_delivery_capability_name,
                "EVENT_DELIVERY_CAPABILITY_CREATE_ONLY",
            )

    def test_capabilities_reuses_a_supplied_store(self) -> None:
        with StorageServer(_capabilities()) as server:
            client = _client(server)
            store = client.workflows.store()
            asyncio.run(client.workflows.capabilities(store=store))
            self.assertEqual(len(server.requests), 2)
            for request in server.requests:
                self.assertEqual(request["path"], f"{STORE_PATH}/GetStoreCapabilities")

    def test_require_capabilities_rejects_a_backend_without_atomic_create(self) -> None:
        responder = _capabilities(claim=NO_CLAIMS, delivery=NO_ATOMIC_DELIVERY)
        with StorageServer(responder) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                asyncio.run(client.workflows.require_capabilities())

            self.assertEqual(
                raised.exception.code, "MEDALLION_STORE_CAPABILITY_UNAVAILABLE"
            )
            message = str(raised.exception)
            self.assertIn("CLAIM_CAPABILITY_NO_CLAIMS", message)
            self.assertIn("EVENT_DELIVERY_CAPABILITY_NO_ATOMIC_CREATE", message)

    def test_require_capabilities_can_demand_only_what_the_runtime_uses(self) -> None:
        responder = _capabilities(claim=NO_CLAIMS, delivery=CREATE_ONLY_DELIVERY)
        with StorageServer(responder) as server:
            client = _client(server)
            capabilities = asyncio.run(
                client.workflows.require_capabilities(claims=False)
            )
            self.assertFalse(capabilities.supports_claims)
            self.assertTrue(capabilities.supports_atomic_event_delivery)

            with self.assertRaises(MedallionError):
                asyncio.run(client.workflows.require_capabilities(claims=True))


class TemporalessCompatTests(unittest.TestCase):
    """Fail here when the wrapped Temporaless contract drifts from the pin."""

    def test_pinned_version_is_the_installed_version(self) -> None:
        from importlib.metadata import version

        self.assertEqual(version("temporaless"), TEMPORALESS_VERSION)

    def test_service_method_sets_match_the_recorded_contract(self) -> None:
        descriptor = temporaless_pb2.DESCRIPTOR
        store = descriptor.services_by_name["RecordStoreService"]
        query = descriptor.services_by_name["RecordQueryService"]

        self.assertEqual(
            {method.name for method in store.methods}, set(RECORD_STORE_METHODS)
        )
        self.assertEqual(
            {method.name for method in query.methods}, set(RECORD_QUERY_METHODS)
        )
        self.assertEqual(store.full_name, RECORD_STORE_SERVICE)
        self.assertEqual(query.full_name, RECORD_QUERY_SERVICE)

    def test_every_rpc_is_unary_so_one_interceptor_covers_the_surface(self) -> None:
        descriptor = temporaless_pb2.DESCRIPTOR
        for name in ("RecordStoreService", "RecordQueryService"):
            for method in descriptor.services_by_name[name].methods:
                with self.subTest(method=f"{name}.{method.name}"):
                    self.assertFalse(method.client_streaming)
                    self.assertFalse(method.server_streaming)

    def test_operator_methods_are_real_rpcs_of_the_wrapped_services(self) -> None:
        every_method = set(RECORD_STORE_METHODS) | set(RECORD_QUERY_METHODS)
        self.assertTrue(set(OPERATOR_METHODS).issubset(every_method))
        # The SDK adds no convenience for any of them.
        for method in OPERATOR_METHODS:
            self.assertFalse(hasattr(MedallionClient, method))

    def test_wrapped_factories_still_accept_the_arguments_the_sdk_passes(self) -> None:
        import inspect

        for factory in (ConnectStore.from_address, ConnectQueryStore.from_address):
            with self.subTest(factory=factory.__qualname__):
                parameters = inspect.signature(factory).parameters
                self.assertIn("address", parameters)
                for name in ("interceptors", "timeout_ms", "read_max_bytes"):
                    self.assertIn(name, parameters)
                    self.assertEqual(
                        parameters[name].kind, inspect.Parameter.KEYWORD_ONLY
                    )

    def test_capability_helpers_the_handshake_depends_on_still_exist(self) -> None:
        for name in ("claim_capability", "event_delivery_capability"):
            self.assertTrue(callable(getattr(ConnectStore, name, None)), name)

    def test_capability_enum_numbers_match_the_recorded_names(self) -> None:
        expected = {
            "CLAIM_CAPABILITY_UNSPECIFIED": 0,
            "CLAIM_CAPABILITY_NO_CLAIMS": 1,
            "CLAIM_CAPABILITY_CREATE_ONLY_CLAIMS": 2,
            "CLAIM_CAPABILITY_CAS_CLAIMS": 3,
            "EVENT_DELIVERY_CAPABILITY_UNSPECIFIED": 0,
            "EVENT_DELIVERY_CAPABILITY_NO_ATOMIC_CREATE": 1,
            "EVENT_DELIVERY_CAPABILITY_CREATE_ONLY": 2,
        }
        for name, number in expected.items():
            with self.subTest(capability=name):
                self.assertEqual(getattr(temporaless_pb2, name), number)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from medallion import (
    MedallionClient,
    MedallionError,
    TracingConfig,
    ingest_pb2,
    stable_idempotency_key,
)

WORKSPACE_ID = "ws_01jz9q5g6rsf7r5ar4rah1b2c3"
LIST_TABLES = "/medallion.ingest.v1.MedallionIngestService/ListTables"


class FakeSpan:
    def __init__(self, name: str, attributes: dict[str, Any]) -> None:
        self.name = name
        self.attributes = dict(attributes)
        self.status: Any = None
        self.ended = False

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def set_status(self, status: Any) -> None:
        self.status = status

    def record_exception(self, exc: BaseException) -> None:
        self.attributes["exception.type"] = type(exc).__name__


class FakeSpanContext:
    def __init__(self, span: FakeSpan) -> None:
        self.span = span

    def __enter__(self) -> FakeSpan:
        return self.span

    def __exit__(self, *_exc: object) -> None:
        self.span.ended = True


class FakeTracer:
    def __init__(self) -> None:
        self.spans: list[FakeSpan] = []

    def start_as_current_span(
        self,
        name: str,
        *,
        kind: Any = None,
        attributes: dict[str, Any] | None = None,
        **_options: Any,
    ) -> FakeSpanContext:
        span = FakeSpan(name, attributes or {})
        self.spans.append(span)
        return FakeSpanContext(span)


class CaptureServer:
    def __init__(self, raw_response: bytes = b'{"tables":[]}') -> None:
        self.raw_response = raw_response

    def __enter__(self):
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("content-length", "0")))
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("x-request-id", "req_123")
                self.end_headers()
                self.wfile.write(outer.raw_response)

            def log_message(self, _format, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *_exc):
        self.server.shutdown()
        self.thread.join(timeout=2)
        self.server.server_close()

    @property
    def url(self):
        host, port = self.server.server_address
        return f"http://{host}:{port}"


class ClientTests(unittest.TestCase):
    def test_retained_surface_and_immutable_workspace(self) -> None:
        client = MedallionClient(
            base_url="https://api.example.com",
            api_key="fixture",
            workspace_id=WORKSPACE_ID,
        )
        self.assertEqual(client.workspace_id, WORKSPACE_ID)
        for name in ("connect", "cdc", "audit"):
            self.assertFalse(hasattr(client, name))
        self.assertTrue(hasattr(client, "ingest"))
        self.assertTrue(hasattr(client, "tables"))
        self.assertTrue(hasattr(client, "workflows"))
        with self.assertRaises(AttributeError):
            client.workspace_id = "ws_01jz9q5g6rsf7r5ar4rah1b2c4"
        with self.assertRaises(MedallionError):
            MedallionClient(
                base_url="https://api.example.com",
                api_key="fixture",
                workspace_id="invalid",
            )

    def test_stable_batch_identity_remains_deterministic(self) -> None:
        self.assertEqual(
            stable_idempotency_key("orders", "42"), stable_idempotency_key("orders", 42)
        )
        self.assertNotEqual(
            stable_idempotency_key("orders", 42), stable_idempotency_key("orders", 43)
        )

    def test_tracing_creates_client_span(self) -> None:
        tracer = FakeTracer()
        with CaptureServer() as server:
            client = MedallionClient(
                base_url=server.url,
                api_key="test-api-key",
                workspace_id=WORKSPACE_ID,
                tracing=TracingConfig(
                    enabled=True,
                    tracer=tracer,
                    span_prefix="test-medallion",
                ),
            )
            client.ingest.list_tables(ingest_pb2.ListTablesRequest())

        self.assertEqual(len(tracer.spans), 1)
        span = tracer.spans[0]
        self.assertEqual(span.name, f"test-medallion POST {LIST_TABLES}")
        self.assertTrue(span.ended)
        self.assertEqual(span.attributes["medallion.sdk.language"], "python")
        self.assertEqual(span.attributes["medallion.request.path"], LIST_TABLES)
        self.assertEqual(span.attributes["http.response.status_code"], 200)
        self.assertEqual(span.attributes["medallion.request_id"], "req_123")
        attributes = repr(span.attributes)
        self.assertNotIn("test-api-key", attributes)
        self.assertNotIn("order_123", attributes)

    def test_transport_errors_and_options_are_classified(self) -> None:
        with CaptureServer(raw_response=b"{") as server:
            client = MedallionClient(
                base_url=server.url, api_key="fallback-key", workspace_id=WORKSPACE_ID
            )
            with self.assertRaises(MedallionError) as malformed:
                client.ingest.list_tables(ingest_pb2.ListTablesRequest())
        self.assertEqual(malformed.exception.code, "MEDALLION_INVALID_JSON_RESPONSE")
        self.assertEqual(malformed.exception.request_id, "req_123")
        self.assertIsNone(malformed.exception.__cause__)
        self.assertNotIn("raw_response", repr(malformed.exception))
        with self.assertRaises(MedallionError) as invalid_url:
            MedallionClient(
                base_url="https://user:secret@example.com?unsafe=true",
                api_key="fixture",
                workspace_id=WORKSPACE_ID,
            )
        self.assertEqual(invalid_url.exception.code, "MEDALLION_INVALID_OPTIONS")

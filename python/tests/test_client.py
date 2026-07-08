from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from medallion import MedallionClient, TracingConfig

CONNECT_SERVICE = "/medallion.connect.v1.MedallionConnectService"
PUBLISH_CDC_EVENTS = f"{CONNECT_SERVICE}/PublishCdcEvents"


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
    ) -> FakeSpanContext:
        span = FakeSpan(name, attributes or {})
        self.spans.append(span)
        return FakeSpanContext(span)


class CaptureServer:
    def __enter__(self) -> "CaptureServer":
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("content-length", "0"))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                outer.requests.append(
                    {
                        "path": self.path,
                        "headers": self.headers,
                        "body": body,
                    }
                )
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("x-request-id", "req_123")
                self.end_headers()
                if self.path.endswith("/PublishCdcEvents"):
                    self.wfile.write(
                        json.dumps(
                            {
                                "accepted_count": 1,
                                "events": [
                                    {
                                        "idempotency_key": "audit_1",
                                        "event_id": "42",
                                    }
                                ],
                            }
                        ).encode("utf-8")
                    )
                elif self.path.endswith("/ListCdcEvents"):
                    self.wfile.write(
                        json.dumps(
                            {
                                "events": [
                                    {
                                        "id": "42",
                                        "organization_id": "org_123",
                                        "connector_id": "conn_123",
                                        "stream_name": "audit_log",
                                        "entity_type": "order",
                                        "entity_id": "order_123",
                                        "idempotency_key": "audit_1",
                                        "actor_principal": "user:user_123",
                                        "ingested_by_principal": "service_account:worker",
                                        "payload_json": json.dumps(
                                            {
                                                "actor": {
                                                    "type": "user",
                                                    "id": "user_123",
                                                },
                                                "after": {"status": "cancelled"},
                                            }
                                        ),
                                        "kind": "EVENT_KIND_AUDIT",
                                        "action": "order.cancelled",
                                    }
                                ],
                                "next_page_cursor": "cursor_2",
                            }
                        ).encode("utf-8")
                    )
                else:
                    self.wfile.write(b"{}")

            def log_message(self, _format: str, *args: Any) -> None:
                return

        self.requests: list[dict[str, Any]] = []
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


class ClientTests(unittest.TestCase):
    def test_audit_record_uses_connect_proto_route_and_headers(self) -> None:
        with CaptureServer() as server:
            client = MedallionClient(
                base_url=server.url,
                api_key="test-api-key",
                default_connector_id="conn_123",
            )
            result = client.audit.record(
                actor={"type": "user", "id": 123},
                action="order.cancelled",
                resource={"type": "order", "id": "order_123"},
                after={"status": "cancelled"},
                idempotency_key="audit_1",
            )

        request = server.requests[0]
        self.assertTrue(request["path"].endswith("/PublishCdcEvents"))
        self.assertEqual(request["headers"].get("authorization"), "Bearer test-api-key")
        self.assertEqual(request["headers"].get("idempotency-key"), "audit_1")
        event = request["body"]["events"][0]
        self.assertEqual(event["actor_principal"], "user:123")
        self.assertEqual(event["kind"], "EVENT_KIND_AUDIT")
        self.assertEqual(result.request_id, "req_123")
        self.assertEqual(result.events[0].event_id, "42")
        self.assertIsNotNone(result.proto)

    def test_audit_trail_filters_source_actor_and_ingester(self) -> None:
        with CaptureServer() as server:
            client = MedallionClient(
                base_url=server.url,
                api_key="test-api-key",
                organization_id="org_123",
                default_connector_id="conn_123",
            )
            result = client.audit.trail(
                resource_type="order",
                resource_id="order_123",
                actor={"type": "user", "id": "user_123"},
                ingester_principal="service_account:worker",
            )

        request = server.requests[0]
        self.assertTrue(request["path"].endswith("/ListCdcEvents"))
        self.assertEqual(request["body"]["actor_principal"], "user:user_123")
        self.assertEqual(
            request["body"]["ingested_by_principal"],
            "service_account:worker",
        )
        self.assertEqual(result.next_cursor, "cursor_2")
        self.assertEqual(result.events[0].ingester_principal, "service_account:worker")
        self.assertEqual(result.events[0].actor_principal, "user:user_123")

    def test_tracing_creates_client_span(self) -> None:
        tracer = FakeTracer()
        with CaptureServer() as server:
            client = MedallionClient(
                base_url=server.url,
                api_key="test-api-key",
                default_connector_id="conn_123",
                tracing=TracingConfig(
                    enabled=True,
                    tracer=tracer,
                    span_prefix="test-medallion",
                ),
            )
            client.audit.record(
                actor={"type": "user", "id": "user_123"},
                action="order.cancelled",
                resource={"type": "order", "id": "order_123"},
                idempotency_key="audit_1",
            )

        self.assertEqual(len(tracer.spans), 1)
        span = tracer.spans[0]
        self.assertEqual(span.name, f"test-medallion POST {PUBLISH_CDC_EVENTS}")
        self.assertTrue(span.ended)
        self.assertEqual(span.attributes["medallion.sdk.language"], "python")
        self.assertEqual(span.attributes["medallion.request.path"], PUBLISH_CDC_EVENTS)
        self.assertEqual(span.attributes["http.response.status_code"], 200)
        self.assertEqual(span.attributes["medallion.request_id"], "req_123")


if __name__ == "__main__":
    unittest.main()

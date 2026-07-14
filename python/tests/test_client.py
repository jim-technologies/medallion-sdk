from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from medallion import MedallionClient, MedallionError, TracingConfig, connect_pb2

CONNECT_SERVICE = "/medallion.connect.v1.MedallionConnectService"
PUBLISH_CDC_EVENTS = f"{CONNECT_SERVICE}/PublishCdcEvents"
PUBLISH_AUDIT_EVENTS = f"{CONNECT_SERVICE}/PublishAuditEvents"


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
                if self.path.endswith(("/PublishAuditEvents", "/PublishCdcEvents")):
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
                elif self.path.endswith("/ListAuditEvents"):
                    self.wfile.write(
                        json.dumps(
                            {
                                "events": [
                                    {
                                        "id": "42",
                                        "organization_id": "org_123",
                                        "connector_id": "conn_123",
                                        "resource_type": "order",
                                        "resource_id": "order_123",
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
                                                "evidenceUrl": "https://evidence.example/orders/order_123",
                                            }
                                        ),
                                        "action": "order.cancelled",
                                        "occurred_at": "1970-01-01T00:00:00Z",
                                        "observed_at": "1970-01-01T00:00:00Z",
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
                evidence_url="https://evidence.example/orders/order_123",
                idempotency_key="audit_1",
            )
            client.audit.record(
                actor={"type": "user", "id": 123},
                action="order.viewed",
                resource={"type": "order", "id": "order_123"},
                idempotency_key="audit_2",
            )

        request = server.requests[0]
        self.assertTrue(request["path"].endswith("/PublishAuditEvents"))
        self.assertEqual(request["headers"].get("authorization"), "Bearer test-api-key")
        self.assertEqual(request["headers"].get("idempotency-key"), "audit_1")
        event = request["body"]["events"][0]
        self.assertEqual(event["actor_principal"], "user:123")
        self.assertEqual(event["resource_type"], "order")
        self.assertEqual(event["resource_id"], "order_123")
        self.assertNotIn("kind", event)
        self.assertNotIn("operation", event)
        self.assertEqual(
            json.loads(event["payload_json"])["evidenceUrl"],
            "https://evidence.example/orders/order_123",
        )
        absent_payload = json.loads(
            server.requests[1]["body"]["events"][0]["payload_json"]
        )
        self.assertIn("evidenceUrl", absent_payload)
        self.assertIsNone(absent_payload["evidenceUrl"])
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
                limit=0,
            )

        request = server.requests[0]
        self.assertTrue(request["path"].endswith("/ListAuditEvents"))
        self.assertEqual(request["body"]["actor_principal"], "user:user_123")
        self.assertEqual(
            request["body"]["ingested_by_principal"],
            "service_account:worker",
        )
        self.assertNotIn("limit", request["body"])
        self.assertEqual(result.next_cursor, "cursor_2")
        self.assertEqual(result.events[0].ingester_principal, "service_account:worker")
        self.assertEqual(result.events[0].actor_principal, "user:user_123")
        self.assertEqual(
            result.events[0].evidence_url,
            "https://evidence.example/orders/order_123",
        )
        self.assertEqual(result.events[0].occurred_at, "1970-01-01T00:00:00Z")
        self.assertEqual(result.events[0].observed_at, "1970-01-01T00:00:00Z")

    def test_audit_trail_requires_resource_type(self) -> None:
        client = MedallionClient(
            base_url="https://connect.example.com",
            api_key="test-api-key",
            organization_id="org_123",
        )
        with self.assertRaises(MedallionError) as raised:
            client.audit.trail(resource_type="", resource_id="order_123")
        self.assertEqual(raised.exception.code, "MEDALLION_MISSING_RESOURCE_TYPE")

    def test_audit_trail_validates_limit(self) -> None:
        client = MedallionClient(
            base_url="https://connect.example.com",
            api_key="test-api-key",
            organization_id="org_123",
        )
        cases = (
            ({"limit": -1}, "MEDALLION_INVALID_AUDIT_TRAIL_LIMIT"),
            ({"limit": 501}, "MEDALLION_AUDIT_TRAIL_LIMIT_TOO_LARGE"),
            ({"page_size": 501}, "MEDALLION_AUDIT_TRAIL_LIMIT_TOO_LARGE"),
        )
        for values, code in cases:
            with self.subTest(values=values):
                with self.assertRaises(MedallionError) as raised:
                    client.audit.trail(
                        resource_type="order",
                        resource_id="order_123",
                        **values,
                    )
                self.assertEqual(raised.exception.code, code)

    def test_audit_trail_accepts_max_limit(self) -> None:
        with CaptureServer() as server:
            client = MedallionClient(
                base_url=server.url,
                api_key="test-api-key",
                organization_id="org_123",
            )
            client.audit.trail(
                resource_type="order",
                resource_id="order_123",
                limit=500,
            )
        self.assertEqual(server.requests[0]["body"]["limit"], 500)

    def test_action_execution_status_includes_indeterminate(self) -> None:
        self.assertEqual(
            connect_pb2.ACTION_EXECUTION_STATUS_INDETERMINATE,
            5,
        )

    def test_canonical_cdc_proto_shapes(self) -> None:
        expected = {
            connect_pb2.CdcEvent: (
                "id",
                "organization_id",
                "connector_id",
                "stream_name",
                "entity_type",
                "entity_id",
                "operation",
                "source_event_id",
                "idempotency_key",
                "actor_principal",
                "payload_json",
                "occurred_at",
                "observed_at",
                "description",
                "source_system",
                "ingested_by_principal",
            ),
            connect_pb2.ListCdcEventsRequest: (
                "organization_id",
                "connector_id",
                "entity_type",
                "entity_id",
                "limit",
                "actor_principal",
                "occurred_at_from",
                "occurred_at_to",
                "source_system",
                "stream_name",
                "page_cursor",
                "ingested_by_principal",
            ),
        }
        for message, field_names in expected.items():
            with self.subTest(message=message.__name__):
                fields = message.DESCRIPTOR.fields
                self.assertEqual(
                    [(field.number, field.name) for field in fields],
                    list(enumerate(field_names, start=1)),
                )

    def test_composite_primary_key_projection_is_complete_and_unambiguous(self) -> None:
        with CaptureServer() as server:
            client = MedallionClient(
                base_url=server.url,
                api_key="test-api-key",
                default_connector_id="conn_123",
            )
            for tenant in ("tenant_a", "tenant_b"):
                client.cdc.record(
                    source="postgres",
                    table="orders",
                    operation="update",
                    primary_key={"tenant_id": tenant, "id": "1"},
                    idempotency_key=f"cdc_{tenant}",
                )
            client.cdc.record(
                source="postgres",
                table="orders",
                operation="update",
                primary_key={"id": "1"},
                idempotency_key="cdc_single",
            )

        first = server.requests[0]["body"]["events"][0]["entity_id"]
        second = server.requests[1]["body"]["events"][0]["entity_id"]
        self.assertEqual(first, '{"id":"1","tenant_id":"tenant_a"}')
        self.assertNotEqual(first, second)
        self.assertEqual(
            server.requests[2]["body"]["events"][0]["entity_id"],
            "1",
        )
        self.assertNotIn("kind", server.requests[0]["body"]["events"][0])
        self.assertNotIn("action", server.requests[0]["body"]["events"][0])

    def test_cdc_rejects_empty_primary_key(self) -> None:
        client = MedallionClient(
            base_url="https://connect.example.com",
            api_key="test-api-key",
            default_connector_id="conn_123",
        )
        with self.assertRaises(MedallionError) as raised:
            client.cdc.record(
                source="postgres",
                table="orders",
                operation="update",
                primary_key={},
            )
        self.assertEqual(raised.exception.code, "MEDALLION_EMPTY_CDC_PRIMARY_KEY")

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
        self.assertEqual(span.name, f"test-medallion POST {PUBLISH_AUDIT_EVENTS}")
        self.assertTrue(span.ended)
        self.assertEqual(span.attributes["medallion.sdk.language"], "python")
        self.assertEqual(span.attributes["medallion.request.path"], PUBLISH_AUDIT_EVENTS)
        self.assertEqual(span.attributes["http.response.status_code"], 200)
        self.assertEqual(span.attributes["medallion.request_id"], "req_123")


if __name__ == "__main__":
    unittest.main()

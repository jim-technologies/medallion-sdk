from __future__ import annotations

import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from buf.validate import validate_pb2
from medallion import MedallionClient, MedallionError, TracingConfig, connect_pb2
from medallion.client import _audit_event_from_connect

CONNECT_SERVICE = "/medallion.connect.v1.MedallionConnectService"
PUBLISH_CDC_EVENTS = f"{CONNECT_SERVICE}/PublishCdcEvents"
PUBLISH_AUDIT_EVENTS = f"{CONNECT_SERVICE}/PublishAuditEvents"
WORKSPACE_ID = "ws_01jz9q5g6rsf7r5ar4rah1b2c3"


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
    def __init__(self, raw_response: bytes | None = None) -> None:
        self.raw_response = raw_response

    def __enter__(self) -> CaptureServer:
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
                if outer.raw_response is not None:
                    self.wfile.write(outer.raw_response)
                    return
                if self.path.endswith(("/PublishAuditEvents", "/PublishCdcEvents")):
                    published = [
                        {
                            "idempotencyKey": event["idempotencyKey"],
                            "eventId": str(42 + index),
                            "duplicate": False,
                        }
                        for index, event in enumerate(body["events"])
                    ]
                    self.wfile.write(
                        json.dumps(
                            {
                                "acceptedCount": len(published),
                                "duplicateCount": 0,
                                "events": published,
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
                                        "workspaceId": WORKSPACE_ID,
                                        "connectorId": "conn_123",
                                        "resourceType": "order",
                                        "resourceId": "order_123",
                                        "idempotencyKey": "audit_1",
                                        "actorPrincipal": "user:user_123",
                                        "ingestedByPrincipal": "service_account:worker",
                                        "payloadJson": (
                                            '{"actor":{"type":"user","id":"payload_spoof"},'
                                            '"after":{"status":"cancelled"},'
                                            '"decimal":1234567890.123456789,'
                                            '"evidenceUrl":"https://evidence.example/orders/order_123"}'
                                        ),
                                        "action": "cancel",
                                        "description": "customer-visible audit",
                                        "sourceEventId": "source:audit:1",
                                        "sourceSystem": "orders",
                                        "origin": "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
                                        "outcome": "AUDIT_EVENT_OUTCOME_SUCCEEDED",
                                        "occurredAt": "2026-08-01T00:00:00Z",
                                        "observedAt": "2026-08-01T00:00:01Z",
                                    }
                                ],
                                "nextPageCursor": "cursor_2",
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
    def test_audit_structured_actor_preserves_matching_colon_id(self) -> None:
        event = _audit_event_from_connect(
            connect_pb2.AuditEvent(
                actor_principal="user:realm:42",
                payload_json=json.dumps({"actor": {"type": "user", "id": "realm:42"}}),
            )
        )
        self.assertEqual(event.actor.type if event.actor else None, "user")
        self.assertEqual(event.actor.provider if event.actor else None, None)
        self.assertEqual(event.actor.id if event.actor else None, "realm:42")

    def test_audit_spoofed_structured_actor_keeps_wire_authoritative(self) -> None:
        event = _audit_event_from_connect(
            connect_pb2.AuditEvent(
                actor_principal="user:realm:42",
                payload_json=json.dumps(
                    {"actor": {"type": "system", "id": "attacker"}}
                ),
            )
        )
        self.assertEqual(event.actor.type if event.actor else None, "user")
        self.assertEqual(event.actor.provider if event.actor else None, "realm")
        self.assertEqual(event.actor.id if event.actor else None, "42")

    def test_audit_record_uses_connect_proto_route_and_headers(self) -> None:
        with CaptureServer() as server:
            client = MedallionClient(
                base_url=server.url,
                api_key="test-api-key",
                workspace_id=WORKSPACE_ID,
                default_connector_id="conn_123",
            )
            result = client.audit.record(
                actor={"type": "user", "id": 123},
                action="cancel",
                outcome="succeeded",
                resource={"type": "order", "id": "order_123"},
                after={"status": "cancelled"},
                evidence_url="https://evidence.example/orders/order_123",
                idempotency_key="audit_1",
            )
            client.audit.record(
                actor={"type": "user", "id": 123},
                action="view",
                outcome="succeeded",
                resource={"type": "order", "id": "order_123"},
                idempotency_key="audit_2",
            )

        request = server.requests[0]
        self.assertTrue(request["path"].endswith("/PublishAuditEvents"))
        self.assertIsNone(request["headers"].get("authorization"))
        self.assertEqual(request["headers"].get("x-medallion-api-key"), "test-api-key")
        self.assertEqual(
            request["headers"].get("x-medallion-workspace-id"), WORKSPACE_ID
        )
        self.assertIsNone(request["headers"].get("idempotency-key"))
        event = request["body"]["events"][0]
        self.assertEqual(event["actorPrincipal"], "user:123")
        self.assertEqual(event["resourceType"], "order")
        self.assertEqual(event["resourceId"], "order_123")
        self.assertEqual(event["outcome"], "AUDIT_EVENT_OUTCOME_SUCCEEDED")
        self.assertNotIn("kind", event)
        self.assertNotIn("operation", event)
        self.assertEqual(
            json.loads(event["payloadJson"])["evidenceUrl"],
            "https://evidence.example/orders/order_123",
        )
        absent_payload = json.loads(
            server.requests[1]["body"]["events"][0]["payloadJson"]
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
                workspace_id=WORKSPACE_ID,
                default_connector_id="conn_123",
            )
            result = client.audit.trail(
                resource_type="order",
                resource_id="order_123",
                actor={"type": "user", "id": "user_123"},
                ingester_principal="service_account:worker",
                action="cancel",
                origin="external_provider",
                outcome="succeeded",
                limit=0,
            )

        request = server.requests[0]
        self.assertTrue(request["path"].endswith("/ListAuditEvents"))
        self.assertEqual(request["body"]["actorPrincipal"], "user:user_123")
        self.assertEqual(
            request["body"]["ingestedByPrincipal"],
            "service_account:worker",
        )
        self.assertEqual(request["body"]["action"], "cancel")
        self.assertEqual(
            request["body"]["origin"],
            "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
        )
        self.assertEqual(
            request["body"]["outcome"],
            "AUDIT_EVENT_OUTCOME_SUCCEEDED",
        )
        self.assertNotIn("limit", request["body"])
        self.assertEqual(result.next_cursor, "cursor_2")
        self.assertEqual(result.events[0].ingester_principal, "service_account:worker")
        self.assertEqual(result.events[0].actor_principal, "user:user_123")
        self.assertIsNotNone(result.events[0].actor)
        self.assertEqual(result.events[0].actor.id, "user_123")
        self.assertEqual(result.events[0].source_system, "orders")
        self.assertEqual(result.events[0].idempotency_key, "audit_1")
        self.assertEqual(result.events[0].description, "customer-visible audit")
        self.assertEqual(result.events[0].source_event_id, "source:audit:1")
        self.assertEqual(
            result.events[0].payload_json,
            '{"actor":{"type":"user","id":"payload_spoof"},'
            '"after":{"status":"cancelled"},'
            '"decimal":1234567890.123456789,'
            '"evidenceUrl":"https://evidence.example/orders/order_123"}',
        )
        self.assertEqual(result.events[0].origin, "external_provider")
        self.assertEqual(result.events[0].outcome, "succeeded")
        self.assertEqual(
            result.events[0].evidence_url,
            "https://evidence.example/orders/order_123",
        )
        self.assertEqual(result.events[0].occurred_at, "2026-08-01T00:00:00Z")
        self.assertEqual(result.events[0].observed_at, "2026-08-01T00:00:01Z")

    def test_audit_trail_requires_resource_type(self) -> None:
        client = MedallionClient(
            base_url="https://api.example.com",
            api_key="test-api-key",
            workspace_id=WORKSPACE_ID,
        )
        with self.assertRaises(MedallionError) as raised:
            client.audit.trail(resource_type="", resource_id="order_123")
        self.assertEqual(raised.exception.code, "MEDALLION_MISSING_RESOURCE_TYPE")

    def test_audit_trail_validates_limit(self) -> None:
        client = MedallionClient(
            base_url="https://api.example.com",
            api_key="test-api-key",
            workspace_id=WORKSPACE_ID,
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
                workspace_id=WORKSPACE_ID,
            )
            client.audit.trail(
                resource_type="order",
                resource_id="order_123",
                limit=500,
            )
        self.assertEqual(server.requests[0]["body"]["limit"], 500)

    def test_canonical_cdc_proto_shapes(self) -> None:
        expected = {
            connect_pb2.CdcEvent: (
                (1, "id"),
                (3, "connector_id"),
                (4, "stream_name"),
                (5, "entity_type"),
                (6, "entity_id"),
                (7, "operation"),
                (8, "source_event_id"),
                (9, "idempotency_key"),
                (10, "actor_principal"),
                (11, "payload_json"),
                (12, "occurred_at"),
                (13, "observed_at"),
                (14, "description"),
                (15, "source_system"),
                (16, "ingested_by_principal"),
                (17, "workspace_id"),
            ),
            connect_pb2.ListCdcEventsRequest: (
                (2, "connector_id"),
                (3, "entity_type"),
                (4, "entity_id"),
                (5, "limit"),
                (6, "actor_principal"),
                (7, "occurred_at_from"),
                (8, "occurred_at_to"),
                (9, "source_system"),
                (10, "stream_name"),
                (11, "page_cursor"),
                (12, "ingested_by_principal"),
                (13, "workspace_id"),
            ),
        }
        for message, fields in expected.items():
            with self.subTest(message=message.__name__):
                self.assertEqual(
                    [(field.number, field.name) for field in message.DESCRIPTOR.fields],
                    list(fields),
                )

    def test_canonical_connect_validation_constraints(self) -> None:
        string_rules = (
            (connect_pb2.CdcEvent, "connector_id", 0, 128),
            (connect_pb2.CdcEvent, "stream_name", 1, 256),
            (connect_pb2.CdcEvent, "entity_type", 1, 256),
            (connect_pb2.CdcEvent, "entity_id", 1, 1024),
            (connect_pb2.CdcEvent, "source_event_id", 0, 1024),
            (connect_pb2.CdcEvent, "idempotency_key", 1, 512),
            (connect_pb2.CdcEvent, "actor_principal", 0, 512),
            (connect_pb2.CdcEvent, "description", 0, 4096),
            (connect_pb2.CdcEvent, "source_system", 0, 256),
            (connect_pb2.CdcEvent, "ingested_by_principal", 0, 512),
            (connect_pb2.AuditEvent, "connector_id", 0, 128),
            (connect_pb2.AuditEvent, "resource_type", 1, 256),
            (connect_pb2.AuditEvent, "resource_id", 1, 1024),
            (connect_pb2.AuditEvent, "action", 1, 256),
            (connect_pb2.AuditEvent, "source_event_id", 0, 1024),
            (connect_pb2.AuditEvent, "idempotency_key", 1, 512),
            (connect_pb2.AuditEvent, "actor_principal", 0, 512),
            (connect_pb2.AuditEvent, "description", 0, 4096),
            (connect_pb2.AuditEvent, "source_system", 0, 256),
            (connect_pb2.AuditEvent, "ingested_by_principal", 0, 512),
            (connect_pb2.PublishCdcEventsRequest, "connector_id", 1, 128),
            (connect_pb2.PublishAuditEventsRequest, "connector_id", 1, 128),
            (connect_pb2.ListCdcEventsRequest, "connector_id", 0, 128),
            (connect_pb2.ListCdcEventsRequest, "entity_type", 0, 256),
            (connect_pb2.ListCdcEventsRequest, "entity_id", 0, 1024),
            (connect_pb2.ListCdcEventsRequest, "actor_principal", 0, 512),
            (connect_pb2.ListCdcEventsRequest, "source_system", 0, 256),
            (connect_pb2.ListCdcEventsRequest, "stream_name", 0, 256),
            (connect_pb2.ListCdcEventsRequest, "page_cursor", 0, 2048),
            (connect_pb2.ListCdcEventsRequest, "ingested_by_principal", 0, 512),
            (connect_pb2.ListAuditEventsRequest, "connector_id", 0, 128),
            (connect_pb2.ListAuditEventsRequest, "resource_type", 0, 256),
            (connect_pb2.ListAuditEventsRequest, "resource_id", 0, 1024),
            (connect_pb2.ListAuditEventsRequest, "actor_principal", 0, 512),
            (connect_pb2.ListAuditEventsRequest, "action", 0, 256),
            (connect_pb2.ListAuditEventsRequest, "source_system", 0, 256),
            (connect_pb2.ListAuditEventsRequest, "page_cursor", 0, 2048),
            (
                connect_pb2.ListAuditEventsRequest,
                "ingested_by_principal",
                0,
                512,
            ),
        )
        for message_type, field_name, min_len, max_bytes in string_rules:
            with self.subTest(message=message_type.__name__, field=field_name):
                field = message_type.DESCRIPTOR.fields_by_name[field_name]
                rules = field.GetOptions().Extensions[validate_pb2.field].string
                self.assertEqual(rules.min_len, min_len)
                self.assertEqual(rules.max_bytes, max_bytes)

        for message_type in (
            connect_pb2.CdcEvent,
            connect_pb2.AuditEvent,
            connect_pb2.ListCdcEventsRequest,
            connect_pb2.ListAuditEventsRequest,
        ):
            with self.subTest(message=message_type.__name__, field="workspace_id"):
                field = message_type.DESCRIPTOR.fields_by_name["workspace_id"]
                rules = field.GetOptions().Extensions[validate_pb2.field].string
                self.assertEqual(rules.pattern, r"^ws_[0-9a-hjkmnp-tv-z]{26}$")

    def test_composite_primary_key_requires_canonical_entity_id(self) -> None:
        with CaptureServer() as server:
            client = MedallionClient(
                base_url=server.url,
                api_key="test-api-key",
                workspace_id=WORKSPACE_ID,
                default_connector_id="conn_123",
            )
            for partition in ("partition_a", "partition_b"):
                client.cdc.record(
                    source="postgres",
                    table="orders",
                    operation="update",
                    primary_key={"partition_id": partition, "id": "1"},
                    entity_id=f"{partition}/order/1",
                    idempotency_key=f"cdc_{partition}",
                )
            client.cdc.record(
                source="postgres",
                table="orders",
                operation="update",
                primary_key={"id": "1"},
                idempotency_key="cdc_single",
            )

        first = server.requests[0]["body"]["events"][0]["entityId"]
        second = server.requests[1]["body"]["events"][0]["entityId"]
        self.assertEqual(first, "partition_a/order/1")
        self.assertNotEqual(first, second)
        self.assertEqual(
            server.requests[2]["body"]["events"][0]["entityId"],
            "1",
        )
        self.assertNotIn("kind", server.requests[0]["body"]["events"][0])
        self.assertNotIn("action", server.requests[0]["body"]["events"][0])

    def test_cdc_rejects_composite_primary_key_without_entity_id(self) -> None:
        client = MedallionClient(
            base_url="https://api.example.com",
            api_key="test-api-key",
            workspace_id=WORKSPACE_ID,
            default_connector_id="conn_123",
        )
        with self.assertRaises(MedallionError) as raised:
            client.cdc.record(
                source="postgres",
                table="orders",
                operation="update",
                primary_key={"partition_id": "partition_a", "id": "1"},
                idempotency_key="cdc_composite_without_entity",
            )
        self.assertEqual(
            raised.exception.code,
            "MEDALLION_MISSING_CDC_ENTITY_ID",
        )

    def test_cdc_rejects_empty_primary_key(self) -> None:
        client = MedallionClient(
            base_url="https://api.example.com",
            api_key="test-api-key",
            workspace_id=WORKSPACE_ID,
            default_connector_id="conn_123",
        )
        with self.assertRaises(MedallionError) as raised:
            client.cdc.record(
                source="postgres",
                table="orders",
                operation="update",
                primary_key={},
                idempotency_key="cdc_empty_key",
            )
        self.assertEqual(raised.exception.code, "MEDALLION_EMPTY_CDC_PRIMARY_KEY")

    def test_event_records_require_stable_idempotency_keys(self) -> None:
        client = MedallionClient(
            base_url="https://api.example.com",
            api_key="test-api-key",
            workspace_id=WORKSPACE_ID,
            default_connector_id="conn_123",
        )
        with self.assertRaises(MedallionError) as audit:
            client.audit.record(
                actor={"type": "user", "id": "user_123"},
                action="cancel",
                outcome="succeeded",
                resource={"type": "order", "id": "order_123"},
                idempotency_key="",
            )
        with self.assertRaises(MedallionError) as cdc:
            client.cdc.record(
                source="postgres",
                table="orders",
                operation="update",
                primary_key={"id": "order_123"},
                idempotency_key="",
            )
        self.assertEqual(audit.exception.code, "MEDALLION_MISSING_IDEMPOTENCY_KEY")
        self.assertEqual(cdc.exception.code, "MEDALLION_MISSING_IDEMPOTENCY_KEY")
        with self.assertRaises(MedallionError) as audit_too_long:
            client.audit.record(
                actor={"type": "user", "id": "user_123"},
                action="cancel",
                outcome="succeeded",
                resource={"type": "order", "id": "order_123"},
                idempotency_key="x" * 513,
            )
        with self.assertRaises(MedallionError) as cdc_too_long:
            client.cdc.record(
                source="postgres",
                table="orders",
                operation="update",
                primary_key={"id": "order_123"},
                idempotency_key="x" * 513,
            )
        self.assertEqual(
            audit_too_long.exception.code, "MEDALLION_INVALID_IDEMPOTENCY_KEY"
        )
        self.assertEqual(
            cdc_too_long.exception.code, "MEDALLION_INVALID_IDEMPOTENCY_KEY"
        )

    def test_tracing_creates_client_span(self) -> None:
        tracer = FakeTracer()
        with CaptureServer() as server:
            client = MedallionClient(
                base_url=server.url,
                api_key="test-api-key",
                workspace_id=WORKSPACE_ID,
                default_connector_id="conn_123",
                tracing=TracingConfig(
                    enabled=True,
                    tracer=tracer,
                    span_prefix="test-medallion",
                ),
            )
            client.audit.record(
                actor={"type": "user", "id": "user_123"},
                action="cancel",
                outcome="succeeded",
                resource={"type": "order", "id": "order_123"},
                idempotency_key="audit_1",
            )

        self.assertEqual(len(tracer.spans), 1)
        span = tracer.spans[0]
        self.assertEqual(span.name, f"test-medallion POST {PUBLISH_AUDIT_EVENTS}")
        self.assertTrue(span.ended)
        self.assertEqual(span.attributes["medallion.sdk.language"], "python")
        self.assertEqual(
            span.attributes["medallion.request.path"], PUBLISH_AUDIT_EVENTS
        )
        self.assertEqual(span.attributes["http.response.status_code"], 200)
        self.assertEqual(span.attributes["medallion.request_id"], "req_123")
        attributes = repr(span.attributes)
        self.assertNotIn("test-api-key", attributes)
        self.assertNotIn("order_123", attributes)

    def test_successful_responses_require_semantic_acknowledgements(self) -> None:
        with CaptureServer(raw_response=b"{}") as server:
            client = MedallionClient(
                base_url=server.url,
                api_key="test-api-key",
                workspace_id=WORKSPACE_ID,
                default_connector_id="conn_123",
            )
            with self.assertRaises(MedallionError) as publish:
                client.audit.record(
                    actor={"type": "user", "id": "user_123"},
                    action="cancel",
                    outcome="succeeded",
                    resource={"type": "order", "id": "order_123"},
                    idempotency_key="audit_empty_ack",
                )
        self.assertEqual(
            publish.exception.code,
            "MEDALLION_INVALID_PUBLISH_RESPONSE",
        )
        self.assertEqual(publish.exception.request_id, "req_123")

    def test_transport_errors_and_options_are_classified(self) -> None:
        with CaptureServer(raw_response=b"{") as server:
            client = MedallionClient(
                base_url=server.url,
                api_key="fallback-key",
                workspace_id=WORKSPACE_ID,
                default_connector_id="conn_123",
            )
            with self.assertRaises(MedallionError) as malformed:
                client.audit.record(
                    actor={"type": "user", "id": "user_123"},
                    action="cancel",
                    outcome="succeeded",
                    resource={"type": "order", "id": "order_123"},
                    idempotency_key="audit_malformed_response",
                )

        self.assertEqual(
            malformed.exception.code,
            "MEDALLION_INVALID_JSON_RESPONSE",
        )
        self.assertEqual(malformed.exception.request_id, "req_123")
        self.assertIsNone(malformed.exception.__cause__)
        self.assertNotIn("raw_response", repr(malformed.exception))

        with self.assertRaises(MedallionError) as invalid_url:
            MedallionClient(
                base_url="https://user:secret@example.com?unsafe=true",
                api_key="test-api-key",
                workspace_id=WORKSPACE_ID,
            )
        self.assertEqual(invalid_url.exception.code, "MEDALLION_INVALID_OPTIONS")


if __name__ == "__main__":
    unittest.main()

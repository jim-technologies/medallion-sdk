from __future__ import annotations

import base64
import json
import threading
import unittest
from dataclasses import dataclass, field
from http.client import IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, build_opener

from google.protobuf import json_format

from medallion import (
    AuditEventInput,
    CdcEventInput,
    KnownErrorReason,
    MedallionAPIError,
    MedallionClient,
    MedallionError,
    RetryConfig,
    TracingConfig,
    connect_pb2,
    stable_idempotency_key,
)
from medallion.request import _RequestClient, _retry_delay

WORKSPACE_ID = "ws_01jz9q5g6rsf7r5ar4rah1b2c3"
OTHER_WORKSPACE_ID = "ws_01jz9q5g6rsf7r5ar4rah1b2c4"
ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Reply:
    status: int
    body: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)


class ScriptedServer:
    def __init__(self, replies: list[Reply]) -> None:
        self.replies = replies
        self.requests: list[dict[str, Any]] = []

    def __enter__(self) -> ScriptedServer:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("content-length", "0"))
                raw = self.rfile.read(length)
                outer.requests.append(
                    {
                        "path": self.path,
                        "headers": self.headers,
                        "raw": raw,
                        "body": json.loads(raw),
                    }
                )
                index = min(len(outer.requests) - 1, len(outer.replies) - 1)
                reply = outer.replies[index]
                self.send_response(reply.status)
                self.send_header("content-type", "application/json")
                if not any(name.lower() == "x-request-id" for name in reply.headers):
                    self.send_header("x-request-id", f"request-{index + 1}")
                for name, value in reply.headers.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(json.dumps(reply.body).encode())

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


class RedirectServer:
    def __enter__(self) -> RedirectServer:
        outer = self
        self.source_requests: list[dict[str, str]] = []
        self.target_requests: list[dict[str, str]] = []

        class TargetHandler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                self._capture()

            def do_GET(self) -> None:  # noqa: N802
                self._capture()

            def _capture(self) -> None:
                outer.target_requests.append(
                    {name.lower(): value for name, value in self.headers.items()}
                )
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, _format: str, *args: Any) -> None:
                return

        self.target = ThreadingHTTPServer(("127.0.0.1", 0), TargetHandler)
        target_host, target_port = self.target.server_address
        target_url = f"http://{target_host}:{target_port}/credential-sink"

        class SourceHandler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("content-length", "0"))
                self.rfile.read(length)
                outer.source_requests.append(
                    {name.lower(): value for name, value in self.headers.items()}
                )
                self.send_response(307)
                self.send_header("location", target_url)
                self.end_headers()

            def log_message(self, _format: str, *args: Any) -> None:
                return

        self.source = ThreadingHTTPServer(("127.0.0.1", 0), SourceHandler)
        self.threads = [
            threading.Thread(target=self.target.serve_forever, daemon=True),
            threading.Thread(target=self.source.serve_forever, daemon=True),
        ]
        for thread in self.threads:
            thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        for server in (self.source, self.target):
            server.shutdown()
        for thread in self.threads:
            thread.join(timeout=2)
        for server in (self.source, self.target):
            server.server_close()

    @property
    def url(self) -> str:
        host, port = self.source.server_address
        return f"http://{host}:{port}"


class OversizedResponseServer:
    def __init__(self, *, status: int, declare_length: bool, body: bytes) -> None:
        self.status = status
        self.declare_length = declare_length
        self.body = body

    def __enter__(self) -> OversizedResponseServer:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("content-length", "0"))
                self.rfile.read(length)
                self.send_response(outer.status)
                self.send_header("content-type", "application/json")
                self.send_header("x-request-id", "request-oversized")
                if outer.declare_length:
                    self.send_header("content-length", str(len(outer.body)))
                self.end_headers()
                try:
                    self.wfile.write(outer.body)
                except BrokenPipeError, ConnectionResetError:
                    pass

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


def _publish_reply(keys: list[str], *, duplicate: bool = False) -> Reply:
    return Reply(
        200,
        {
            "acceptedCount": 0 if duplicate else len(keys),
            "duplicateCount": len(keys) if duplicate else 0,
            "events": [
                {
                    "idempotencyKey": key,
                    "eventId": str(9_223_372_036_854_775_000 + index),
                    "duplicate": duplicate,
                }
                for index, key in enumerate(keys)
            ],
            "futureField": "ignored",
        },
    )


def _client(server: ScriptedServer, **options: Any) -> MedallionClient:
    return MedallionClient(
        base_url=server.url,
        api_key="secret-api-key",
        workspace_id=WORKSPACE_ID,
        default_connector_id="connector-1",
        retry=RetryConfig(
            max_attempts=3,
            initial_backoff=0.001,
            max_backoff=0.002,
            jitter_ratio=0,
        ),
        **options,
    )


def _transport_post_json(
    client: _RequestClient,
    path: str,
    body: dict[str, Any] | None = None,
) -> Any:
    payload = json.dumps(body or {}, separators=(",", ":")).encode()
    return client._post(
        path,
        payload,
        timeout=None,
        cancellation_event=None,
        retry_safe=False,
    )


def _field(number: int, value: str) -> bytes:
    raw = value.encode()
    return bytes([(number << 3) | 2, len(raw)]) + raw


def _error_info(reason: str, metadata: dict[str, str] | None = None) -> str:
    raw = _field(1, reason) + _field(2, "medallion.jimtech.io")
    for key, value in (metadata or {}).items():
        entry = _field(1, key) + _field(2, value)
        raw += bytes([(3 << 3) | 2, len(entry)]) + entry
    return base64.b64encode(raw).decode().rstrip("=")


class IngestionTests(unittest.TestCase):
    def test_official_external_sdk_ingestion_conformance_fixtures(self) -> None:
        document = json.loads(
            (
                ROOT
                / "proto/external-ingestion-contract/v1/conformance/external-sdk-ingestion.json"
            ).read_text()
        )
        self.assertEqual(document["category"], "external_sdk_ingestion")
        self.assertEqual(len(document["fixtures"]), 2)

        for fixture in document["fixtures"]:
            method = fixture["protocol"]["method"]
            with self.subTest(fixture=fixture["id"]):
                with ScriptedServer(
                    [
                        Reply(
                            fixture["expected"]["httpStatus"],
                            fixture["expected"]["body"],
                        )
                    ]
                ) as server:
                    client = MedallionClient(
                        base_url=server.url,
                        api_key="FIXTURE_NON_SECRET_API_KEY",
                        workspace_id=fixture["input"]["headers"][
                            "x-medallion-workspace-id"
                        ],
                    )
                    if method == "PublishCdcEvents":
                        request = json_format.ParseDict(
                            fixture["input"]["body"],
                            connect_pb2.PublishCdcEventsRequest(),
                        )
                        response, _ = client.connect.publish_cdc_events(request)
                    elif method == "PublishAuditEvents":
                        request = json_format.ParseDict(
                            fixture["input"]["body"],
                            connect_pb2.PublishAuditEventsRequest(),
                        )
                        response, _ = client.connect.publish_audit_events(request)
                    else:
                        self.fail(f"unexpected ingestion fixture method {method}")

                self.assertEqual(
                    server.requests[0]["path"], fixture["protocol"]["path"]
                )
                self.assertEqual(server.requests[0]["body"], fixture["input"]["body"])
                self.assertEqual(
                    server.requests[0]["headers"].get("x-medallion-workspace-id"),
                    fixture["input"]["headers"]["x-medallion-workspace-id"],
                )
                self.assertEqual(response.accepted_count, 1)
                self.assertEqual(response.duplicate_count, 1)
                self.assertEqual(
                    [event.idempotency_key for event in response.events],
                    [
                        event["idempotencyKey"]
                        for event in fixture["expected"]["body"]["events"]
                    ],
                )
                self.assertEqual(
                    [str(event.event_id) for event in response.events],
                    [
                        event["eventId"]
                        for event in fixture["expected"]["body"]["events"]
                    ],
                )
                self.assertEqual(
                    [event.duplicate for event in response.events],
                    [False, True],
                )

    def test_platform_audit_and_arbitrary_rpc_invocation_are_not_public(self) -> None:
        client = MedallionClient(
            base_url="https://api.example.com",
            api_key="test-key",
            workspace_id=WORKSPACE_ID,
        )
        self.assertFalse(hasattr(client, "invoke"))
        self.assertFalse(hasattr(client, "".join(("onto", "logy"))))
        self.assertFalse(hasattr(client.connect, "invoke"))
        self.assertFalse(hasattr(client.connect, "publish_platform_audit_events"))
        self.assertFalse(hasattr(connect_pb2, "PublishPlatformAuditEventsRequest"))
        self.assertEqual(
            {
                name
                for name, value in vars(type(client.connect)).items()
                if not name.startswith("_") and callable(value)
            },
            {
                "publish_cdc_events",
                "publish_audit_events",
                "list_cdc_events",
                "list_audit_events",
            },
        )

    def test_api_key_workspace_and_canonical_headers(self) -> None:
        with ScriptedServer([_publish_reply(["cdc:1"])]) as server:
            _client(server).cdc.record(
                stream_name="orders",
                entity_type="order",
                entity_id="1",
                operation="insert",
                idempotency_key="cdc:1",
                payload={"amount": 42},
            )
        request = server.requests[0]
        self.assertEqual(
            request["path"],
            "/medallion.connect.v1.MedallionConnectService/PublishCdcEvents",
        )
        self.assertEqual(
            request["headers"].get("x-medallion-api-key"), "secret-api-key"
        )
        self.assertEqual(
            request["headers"].get("x-medallion-workspace-id"), WORKSPACE_ID
        )
        self.assertEqual(request["headers"].get("connect-protocol-version"), "1")
        self.assertEqual(request["headers"].get_content_type(), "application/json")
        self.assertTrue(request["headers"].get("connect-timeout-ms").isdigit())
        self.assertIsNone(request["headers"].get("authorization"))
        self.assertNotIn("idempotency-key", request["headers"])

    def test_jwt_auth_and_ambiguous_configuration(self) -> None:
        with ScriptedServer([_publish_reply(["audit:1"])]) as server:
            client = MedallionClient(
                base_url=server.url,
                access_token="jwt-value",
                workspace_id=WORKSPACE_ID,
                default_connector_id="connector-1",
            )
            client.audit.record(
                resource_type="order",
                resource_id="1",
                action="created",
                outcome="succeeded",
                idempotency_key="audit:1",
                payload={"reference": "evidence-1"},
            )
        headers = server.requests[0]["headers"]
        self.assertEqual(headers.get("authorization"), "Bearer jwt-value")
        self.assertIsNone(headers.get("x-medallion-api-key"))

        for options in (
            {"api_key": "key", "access_token": "jwt"},
            {"api_key": None, "access_token": None},
            {"api_key": "   ", "access_token": None},
            {"api_key": "key", "access_token": "   "},
        ):
            with self.subTest(options=options):
                with self.assertRaises(MedallionError) as raised:
                    MedallionClient(
                        base_url="https://api.example.com",
                        workspace_id=WORKSPACE_ID,
                        **options,
                    )
                self.assertEqual(raised.exception.code, "MEDALLION_INVALID_OPTIONS")

    def test_workspace_is_immutable_and_list_conflicts_fail_before_network(
        self,
    ) -> None:
        with ScriptedServer([Reply(200, {"events": []})]) as server:
            client = _client(server)
            self.assertEqual(client.workspace_id, WORKSPACE_ID)
            calls = (
                lambda: client.connect.list_cdc_events(
                    connect_pb2.ListCdcEventsRequest(workspace_id=OTHER_WORKSPACE_ID)
                ),
                lambda: client.connect.list_audit_events(
                    connect_pb2.ListAuditEventsRequest(workspace_id=OTHER_WORKSPACE_ID)
                ),
            )
            for call in calls:
                with self.subTest(call=call):
                    with self.assertRaises(MedallionError) as raised:
                        call()
                    self.assertEqual(
                        raised.exception.code,
                        "MEDALLION_WORKSPACE_SELECTOR_CONFLICT",
                    )
        self.assertEqual(server.requests, [])

    def test_same_credential_cross_workspace_denial_is_preserved(self) -> None:
        with ScriptedServer(
            [
                Reply(200, {"events": []}),
                Reply(403, {"code": "permission_denied", "message": "denied"}),
            ]
        ) as server:
            first = _client(server)
            second = MedallionClient(
                base_url=server.url,
                api_key="secret-api-key",
                workspace_id=OTHER_WORKSPACE_ID,
                default_connector_id="connector-1",
                retry=RetryConfig(max_attempts=3),
            )
            first.cdc.list()
            with self.assertRaises(MedallionAPIError) as raised:
                second.cdc.list()
        self.assertEqual(raised.exception.connect_code, "permission_denied")
        self.assertEqual(len(server.requests), 2)
        self.assertEqual(server.requests[1]["body"]["workspaceId"], OTHER_WORKSPACE_ID)
        self.assertEqual(
            server.requests[1]["headers"].get("x-medallion-workspace-id"),
            OTHER_WORKSPACE_ID,
        )

    def test_list_request_preflight_is_complete_and_never_uses_network(self) -> None:
        with ScriptedServer([Reply(200, {"events": []})]) as server:
            client = _client(server)
            invalid_timestamp = connect_pb2.ListCdcEventsRequest()
            invalid_timestamp.occurred_at_from.nanos = -1
            reversed_range = connect_pb2.ListAuditEventsRequest()
            reversed_range.occurred_at_from.seconds = 2
            reversed_range.occurred_at_to.seconds = 1

            calls = (
                lambda: client.connect.list_cdc_events(
                    connect_pb2.ListCdcEventsRequest(page_cursor="é" * 1025)
                ),
                lambda: client.connect.list_cdc_events(
                    connect_pb2.ListCdcEventsRequest(limit=501)
                ),
                lambda: client.connect.list_cdc_events(invalid_timestamp),
                lambda: client.connect.list_audit_events(
                    connect_pb2.ListAuditEventsRequest(resource_type="order")
                ),
                lambda: client.connect.list_audit_events(
                    connect_pb2.ListAuditEventsRequest(origin=99)
                ),
                lambda: client.connect.list_audit_events(reversed_range),
                lambda: client.cdc.list(cursor="é" * 1025),
                lambda: client.cdc.list(limit=501),
                lambda: client.cdc.list(
                    occurred_at_from="2026-08-02T00:00:00Z",
                    occurred_at_to="2026-08-01T00:00:00Z",
                ),
                lambda: client.audit.list(resource_type="order"),
                lambda: client.audit.list(action="é" * 129),
                lambda: client.audit.list(origin="future"),
            )
            for call in calls:
                with self.subTest(call=call):
                    with self.assertRaises(MedallionError):
                        call()
        self.assertEqual(server.requests, [])

    def test_list_rejects_equal_timestamp_bounds(self) -> None:
        with ScriptedServer([Reply(200, {"events": []})]) as server:
            client = _client(server)
            raw = connect_pb2.ListCdcEventsRequest()
            raw.occurred_at_from.seconds = 1
            raw.occurred_at_to.seconds = 1
            for call in (
                lambda: client.connect.list_cdc_events(raw),
                lambda: client.audit.list(
                    occurred_at_from="2026-08-01T00:00:00Z",
                    occurred_at_to="2026-08-01T00:00:00Z",
                ),
            ):
                with self.subTest(call=call):
                    with self.assertRaises(MedallionError) as raised:
                        call()
                    self.assertEqual(
                        raised.exception.code,
                        "MEDALLION_INVALID_TIMESTAMP_RANGE",
                    )
        self.assertEqual(server.requests, [])
        self.assertEqual(raw.workspace_id, "")

    def test_idempotency_keys_use_body_bytes_without_legacy_header(self) -> None:
        key = "é" * 256
        with ScriptedServer([_publish_reply([key])]) as server:
            client = _client(server)
            client.cdc.record(
                stream_name="orders",
                entity_type="order",
                entity_id="1",
                operation="insert",
                idempotency_key=key,
            )
        event = server.requests[0]["body"]["events"][0]
        self.assertEqual(event["idempotencyKey"], key)
        self.assertNotIn("idempotency-key", server.requests[0]["headers"])

        with self.assertRaises(MedallionError) as raised:
            client.cdc.record(
                stream_name="orders",
                entity_type="order",
                entity_id="2",
                operation="insert",
                idempotency_key="é" * 257,
            )
        self.assertEqual(raised.exception.code, "MEDALLION_INVALID_IDEMPOTENCY_KEY")

    def test_idempotency_key_preserves_any_nonempty_valid_unicode(self) -> None:
        key = " \t\nsource:雪:01 "
        with ScriptedServer([_publish_reply([key])]) as server:
            client = _client(server)
            receipt = client.audit.record(
                resource_type="order",
                resource_id="1",
                action="import",
                outcome="succeeded",
                idempotency_key=key,
            )
        self.assertEqual(receipt.events[0].idempotency_key, key)
        self.assertEqual(server.requests[0]["body"]["events"][0]["idempotencyKey"], key)

        for invalid in ("", "\ud800"):
            with self.subTest(invalid=repr(invalid)):
                with self.assertRaises(MedallionError) as raised:
                    client.audit.record(
                        resource_type="order",
                        resource_id="2",
                        action="import",
                        outcome="succeeded",
                        idempotency_key=invalid,
                    )
                self.assertIn(
                    raised.exception.code,
                    {
                        "MEDALLION_MISSING_IDEMPOTENCY_KEY",
                        "MEDALLION_INVALID_IDEMPOTENCY_KEY",
                    },
                )

    def test_redirects_never_forward_api_key_or_jwt_headers(self) -> None:
        authentication = (
            (
                "api-key",
                {"api_key": "fixture-api-key"},
                "x-medallion-api-key",
                "fixture-api-key",
            ),
            (
                "jwt",
                {"access_token": "fixture-jwt"},
                "authorization",
                "Bearer fixture-jwt",
            ),
        )
        for auth_name, auth, header, expected in authentication:
            for custom_opener in (False, True):
                with self.subTest(auth=auth_name, custom_opener=custom_opener):
                    with RedirectServer() as server:
                        client = _RequestClient(
                            base_url=server.url,
                            workspace_id=WORKSPACE_ID,
                            opener=(
                                build_opener(HTTPRedirectHandler())
                                if custom_opener
                                else None
                            ),
                            **auth,
                        )
                        with self.assertRaises(MedallionAPIError) as raised:
                            _transport_post_json(
                                client,
                                "/medallion.connect.v1."
                                "MedallionConnectService/ListCdcEvents",
                                {},
                            )

                    self.assertEqual(raised.exception.status, 307)
                    self.assertEqual(len(server.source_requests), 1)
                    self.assertEqual(server.source_requests[0].get(header), expected)
                    self.assertEqual(server.target_requests, [])

    def test_success_and_error_responses_are_bounded(self) -> None:
        for status in (200, 503):
            for declare_length in (True, False):
                with self.subTest(status=status, declare_length=declare_length):
                    with OversizedResponseServer(
                        status=status,
                        declare_length=declare_length,
                        body=b"x" * 65,
                    ) as server:
                        client = _RequestClient(
                            base_url=server.url,
                            workspace_id=WORKSPACE_ID,
                            api_key="fixture-api-key",
                        )
                        with patch("medallion.request.MAX_RESPONSE_BYTES", 64):
                            with self.assertRaises(MedallionError) as raised:
                                _transport_post_json(
                                    client,
                                    "/medallion.connect.v1."
                                    "MedallionConnectService/ListCdcEvents",
                                )

                    self.assertNotIsInstance(raised.exception, MedallionAPIError)
                    self.assertEqual(
                        raised.exception.code,
                        "MEDALLION_RESPONSE_TOO_LARGE",
                    )
                    self.assertEqual(
                        raised.exception.request_id,
                        "request-oversized",
                    )

    def test_response_read_failures_retry_only_retry_safe_calls(self) -> None:
        def response(*, raw: bytes | None = None, failure: Exception | None = None):
            value = MagicMock()
            value.headers = {"x-request-id": "request-read-failure"}
            value.status = 200
            value.read.side_effect = failure
            if failure is None:
                value.read.return_value = raw
            value.__enter__.return_value = value
            value.__exit__.return_value = False
            return value

        retrying_opener = MagicMock()
        retrying_opener.open.side_effect = [
            response(failure=IncompleteRead(b"{", 1)),
            response(raw=b"{}"),
        ]
        retrying_client = _RequestClient(
            base_url="https://api.example.com",
            workspace_id=WORKSPACE_ID,
            api_key="fixture-api-key",
            opener=retrying_opener,
            retry=RetryConfig(
                max_attempts=2,
                initial_backoff=0,
                max_backoff=0,
                jitter_ratio=0,
            ),
        )
        envelope = retrying_client._post(
            "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
            b"{}",
            timeout=None,
            cancellation_event=None,
            retry_safe=True,
        )
        self.assertEqual(envelope.attempts, 2)
        self.assertEqual(retrying_opener.open.call_count, 2)

        unsafe_opener = MagicMock()
        payload_secret = b"customer-payload-secret"
        unsafe_opener.open.return_value = response(
            failure=IncompleteRead(payload_secret, 1)
        )
        unsafe_client = _RequestClient(
            base_url="https://api.example.com",
            workspace_id=WORKSPACE_ID,
            api_key="fixture-api-key",
            opener=unsafe_opener,
            retry=RetryConfig(max_attempts=2),
        )
        with self.assertRaises(MedallionError) as raised:
            unsafe_client._post(
                "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
                b"{}",
                timeout=None,
                cancellation_event=None,
                retry_safe=False,
            )
        self.assertEqual(raised.exception.code, "MEDALLION_NETWORK_ERROR")
        self.assertEqual(raised.exception.request_id, "request-read-failure")
        self.assertIsInstance(raised.exception.__cause__, OSError)
        self.assertEqual(
            str(raised.exception.__cause__),
            "Medallion response body read failed.",
        )
        self.assertNotIn(
            payload_secret.decode(),
            repr(raised.exception.__cause__),
        )
        self.assertEqual(unsafe_opener.open.call_count, 1)

        error_body = MagicMock()
        error_body.read.side_effect = IncompleteRead(b"{", 1)
        terminal_opener = MagicMock()
        terminal_opener.open.side_effect = HTTPError(
            "https://api.example.com",
            401,
            "Unauthorized",
            {"x-request-id": "request-auth-read-failure"},
            error_body,
        )
        terminal_client = _RequestClient(
            base_url="https://api.example.com",
            workspace_id=WORKSPACE_ID,
            api_key="fixture-api-key",
            opener=terminal_opener,
            retry=RetryConfig(
                max_attempts=2,
                initial_backoff=0,
                max_backoff=0,
                jitter_ratio=0,
            ),
        )
        with self.assertRaises(MedallionError) as terminal:
            terminal_client._post(
                "/medallion.connect.v1.MedallionConnectService/ListCdcEvents",
                b"{}",
                timeout=None,
                cancellation_event=None,
                retry_safe=True,
            )
        self.assertEqual(terminal.exception.code, "MEDALLION_NETWORK_ERROR")
        self.assertEqual(
            terminal.exception.request_id,
            "request-auth-read-failure",
        )
        self.assertEqual(terminal_opener.open.call_count, 1)

    def test_retry_jitter_never_exceeds_configured_backoff_cap(self) -> None:
        config = RetryConfig(
            max_attempts=5,
            initial_backoff=0.5,
            max_backoff=1.0,
            jitter_ratio=1.0,
        )
        with patch("medallion.request.random.uniform", return_value=1.0):
            self.assertEqual(_retry_delay(config, 5, None), 1.0)

    def test_all_connect_rpcs_reject_empty_success_bodies(self) -> None:
        rpc_cases = (
            (
                "PublishCdcEvents",
                connect_pb2.PublishCdcEventsRequest(),
                connect_pb2.PublishCdcEventsResponse(),
            ),
            (
                "ListCdcEvents",
                connect_pb2.ListCdcEventsRequest(),
                connect_pb2.ListCdcEventsResponse(),
            ),
            (
                "PublishAuditEvents",
                connect_pb2.PublishAuditEventsRequest(),
                connect_pb2.PublishAuditEventsResponse(),
            ),
            (
                "ListAuditEvents",
                connect_pb2.ListAuditEventsRequest(),
                connect_pb2.ListAuditEventsResponse(),
            ),
        )
        for status in (200, 204):
            for method, request, response in rpc_cases:
                with self.subTest(status=status, method=method):
                    with OversizedResponseServer(
                        status=status,
                        declare_length=True,
                        body=b"",
                    ) as server:
                        client = _RequestClient(
                            base_url=server.url,
                            workspace_id=WORKSPACE_ID,
                            api_key="fixture-api-key",
                        )
                        with self.assertRaises(MedallionError) as raised:
                            client._post_proto(
                                "/medallion.connect.v1."
                                f"MedallionConnectService/{method}",
                                request,
                                response,
                            )
                    self.assertEqual(
                        raised.exception.code,
                        "MEDALLION_INVALID_JSON_RESPONSE",
                    )
                    self.assertEqual(
                        raised.exception.request_id,
                        "request-oversized",
                    )

    def test_base_url_must_be_an_origin(self) -> None:
        for base_url in (
            "https://api.example.com",
            "https://api.example.com/",
            "http://localhost:8080",
            "http://127.99.1.2:8080",
            "http://[::1]:8080",
        ):
            with self.subTest(valid=base_url):
                client = _RequestClient(
                    base_url=base_url,
                    workspace_id=WORKSPACE_ID,
                    api_key="fixture-api-key",
                )
                self.assertFalse(client._base_url.endswith("/"))

        canonical = _RequestClient(
            base_url="HTTPS://EXAMPLE.COM:443/",
            workspace_id=WORKSPACE_ID,
            api_key="fixture-api-key",
        )
        self.assertEqual(canonical._base_url, "https://example.com:443")

        for base_url in (
            "https://api.example.com/not-an-origin",
            "https://api.example.com/nested/path/",
            "https://api.example.com//",
            "https://api.example.com/%2F",
            "http://api.example.com",
            "http://localhost.example.com",
            "http://128.0.0.1",
            "http://[::2]",
            "https://api.example.com?",
            "https://api.example.com#",
            "https://:443",
            "https://api.example.com:not-a-port",
            "https://exam\nple.com",
            "https://exam\tple.com",
        ):
            with self.subTest(invalid=base_url):
                with self.assertRaises(MedallionError) as raised:
                    _RequestClient(
                        base_url=base_url,
                        workspace_id=WORKSPACE_ID,
                        api_key="fixture-api-key",
                    )
                self.assertEqual(raised.exception.code, "MEDALLION_INVALID_OPTIONS")

    def test_internal_transport_rejects_arbitrary_rpc_paths_before_network(
        self,
    ) -> None:
        opener = MagicMock()
        client = _RequestClient(
            base_url="https://api.example.com",
            workspace_id=WORKSPACE_ID,
            api_key="fixture-api-key",
            opener=opener,
        )
        with self.assertRaises(MedallionError) as raised:
            _transport_post_json(
                client,
                "/medallion.connect.v1.MedallionConnectService/RegisterConnector",
            )
        self.assertEqual(raised.exception.code, "MEDALLION_UNSUPPORTED_RPC")
        opener.open.assert_not_called()

    def test_tracing_cannot_override_protected_headers(self) -> None:
        with ScriptedServer([Reply(200, {"events": []})]) as server:
            client = _client(server, tracing=True)

            def inject_protected(_config: Any, headers: dict[str, str]) -> None:
                headers["X-Medallion-Workspace-Id"] = OTHER_WORKSPACE_ID

            with patch(
                "medallion.request.inject_trace_context",
                side_effect=inject_protected,
            ):
                with self.assertRaises(MedallionError) as raised:
                    client.cdc.list()
        self.assertEqual(
            raised.exception.code,
            "MEDALLION_PROTECTED_HEADER_OVERRIDE",
        )
        self.assertEqual(server.requests, [])

    def test_custom_trace_propagator_can_only_add_standard_trace_headers(self) -> None:
        def malicious_inject(headers: dict[str, str]) -> None:
            headers.update(
                {
                    "Authorization": "Bearer attacker",
                    "Host": "attacker.example",
                    "X-Medallion-Workspace-Id": OTHER_WORKSPACE_ID,
                    "X-Medallion-API-Key": "attacker-key",
                    "X-Custom-Exfiltration": "forbidden",
                    "traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
                }
            )

        with ScriptedServer([Reply(200, {"events": []})]) as server:
            client = _client(server, tracing=True)
            with patch(
                "medallion.tracing.propagate.inject",
                side_effect=malicious_inject,
            ):
                client.cdc.list()

        headers = server.requests[0]["headers"]
        self.assertIsNone(headers.get("authorization"))
        self.assertEqual(headers.get("x-medallion-api-key"), "secret-api-key")
        self.assertEqual(headers.get("x-medallion-workspace-id"), WORKSPACE_ID)
        self.assertNotEqual(headers.get("host"), "attacker.example")
        self.assertIsNone(headers.get("x-custom-exfiltration"))
        self.assertEqual(
            headers.get("traceparent"),
            "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
        )

    def test_missing_workspace_and_cancelled_call_fail_before_network(self) -> None:
        with ScriptedServer([_publish_reply(["cdc:1"])]) as server:
            with self.assertRaises(MedallionError) as workspace:
                MedallionClient(
                    base_url=server.url,
                    api_key="key",
                    default_connector_id="connector-1",
                    workspace_id=None,  # type: ignore[arg-type]
                )
            cancelled = threading.Event()
            cancelled.set()
            with self.assertRaises(MedallionError) as cancellation:
                _client(server).audit.record(
                    resource_type="order",
                    resource_id="1",
                    action="created",
                    outcome="succeeded",
                    idempotency_key="audit:1",
                    cancellation_event=cancelled,
                )
        self.assertEqual(workspace.exception.code, "MEDALLION_MISSING_WORKSPACE_ID")
        self.assertEqual(cancellation.exception.code, "MEDALLION_CANCELLED")
        self.assertEqual(server.requests, [])

    def test_workspace_id_requires_the_canonical_shape(self) -> None:
        for value in (
            "",
            "workspace-1",
            "ws_01jz9q5g6rsf7r5ar4rah1b2ci",
            f" {WORKSPACE_ID}",
            WORKSPACE_ID.upper(),
        ):
            with self.subTest(value=value):
                with self.assertRaises(MedallionError) as raised:
                    MedallionClient(
                        base_url="https://api.example.com",
                        api_key="key",
                        workspace_id=value,
                    )
                self.assertIn(
                    raised.exception.code,
                    {
                        "MEDALLION_MISSING_WORKSPACE_ID",
                        "MEDALLION_INVALID_WORKSPACE_ID",
                    },
                )

        for retired_option in ("tenant" + "_id", "organization" + "_id"):
            with self.subTest(retired_option=retired_option):
                with self.assertRaises(TypeError):
                    MedallionClient(
                        base_url="https://api.example.com",
                        api_key="key",
                        workspace_id=WORKSPACE_ID,
                        **{retired_option: "retired"},
                    )

    def test_cdc_batch_is_typed_deterministic_and_lossless(self) -> None:
        keys = ["cdc:1", "cdc:2"]
        payload = {"z": 1, "a": [True, None]}
        with ScriptedServer([_publish_reply(keys)]) as server:
            result = _client(server).cdc.publish_batch(
                [
                    CdcEventInput(
                        stream_name="orders",
                        entity_type="order",
                        entity_id="1",
                        operation="insert",
                        idempotency_key=keys[0],
                        payload=payload,
                        occurred_at="2026-08-01T01:02:03.123456789Z",
                    ),
                    {
                        "stream_name": "orders",
                        "entity_type": "order",
                        "entity_id": " 2 ",
                        "operation": "delete",
                        "idempotency_key": keys[1],
                        "source_event_id": " source:2 ",
                        "actor": {"id": " actor:2 "},
                        "payload_json": (
                            '{"decimal":1234567890.123456789,'
                            '"duplicate":1,"duplicate":2}'
                        ),
                    },
                ]
            )
        events = server.requests[0]["body"]["events"]
        self.assertEqual([item["idempotencyKey"] for item in events], keys)
        self.assertEqual(events[0]["occurredAt"], "2026-08-01T01:02:03.123456789Z")
        self.assertEqual(events[0]["payloadJson"], '{"a":[true,null],"z":1}')
        self.assertEqual(payload, {"z": 1, "a": [True, None]})
        self.assertEqual(events[1]["entityId"], " 2 ")
        self.assertEqual(events[1]["sourceEventId"], " source:2 ")
        self.assertEqual(events[1]["actorPrincipal"], " actor:2 ")
        self.assertEqual(
            events[1]["payloadJson"],
            '{"decimal":1234567890.123456789,"duplicate":1,"duplicate":2}',
        )
        for forbidden in (
            "id",
            "workspaceId",
            "connectorId",
            "observedAt",
            "sourceSystem",
            "ingestedByPrincipal",
        ):
            self.assertNotIn(forbidden, events[0])
        self.assertIsNone(server.requests[0]["headers"].get("idempotency-key"))
        self.assertEqual(result.events[0].event_id, "9223372036854775000")
        self.assertIsInstance(result.events[0].event_id, str)
        self.assertIsNone(result.idempotency_key)

    def test_omitted_payload_defaults_to_object_and_explicit_null_is_preserved(
        self,
    ) -> None:
        with ScriptedServer(
            [_publish_reply(["cdc:omitted"]), _publish_reply(["cdc:null"])]
        ) as server:
            client = _client(server)
            client.cdc.record(
                stream_name="orders",
                entity_type="order",
                entity_id="1",
                operation="insert",
                idempotency_key="cdc:omitted",
            )
            client.cdc.record(
                stream_name="orders",
                entity_type="order",
                entity_id="2",
                operation="insert",
                idempotency_key="cdc:null",
                payload=None,
            )

        self.assertEqual(server.requests[0]["body"]["events"][0]["payloadJson"], "{}")
        self.assertEqual(server.requests[1]["body"]["events"][0]["payloadJson"], "null")

    def test_audit_batch_requires_outcome_and_omits_server_fields(self) -> None:
        with ScriptedServer([_publish_reply(["audit:1"], duplicate=True)]) as server:
            result = _client(server).audit.publish_batch(
                [
                    AuditEventInput(
                        resource_type="invoice",
                        resource_id=9_223_372_036_854_775_000,
                        action="approved",
                        outcome="succeeded",
                        idempotency_key="audit:1",
                        actor={"type": "user", "id": "user-1"},
                        payload={"evidenceRef": "object://evidence/1"},
                    )
                ]
            )
        event = server.requests[0]["body"]["events"][0]
        self.assertEqual(event["resourceId"], "9223372036854775000")
        self.assertEqual(event["action"], "approved")
        self.assertEqual(event["outcome"], "AUDIT_EVENT_OUTCOME_SUCCEEDED")
        self.assertNotIn("origin", event)
        self.assertTrue(result.duplicate)
        self.assertEqual(result.duplicate_count, 1)

        with self.assertRaises(MedallionError) as missing:
            _client(server).audit.publish_batch(
                [
                    {
                        "resource_type": "invoice",
                        "resource_id": "1",
                        "action": "approved",
                        "idempotency_key": "audit:missing-outcome",
                    }
                ]
            )
        self.assertEqual(missing.exception.code, "MEDALLION_INVALID_AUDIT_EVENT")

    def test_invalid_payloads_and_server_fields_fail_locally(self) -> None:
        cycle: list[Any] = []
        cycle.append(cycle)
        invalid = (
            cycle,
            {"not-json": object()},
            {1: "non-string-key"},
            {"not-unicode": "\ud800"},
            float("nan"),
        )
        with ScriptedServer([_publish_reply(["cdc:1"])]) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as empty_batch:
                client.cdc.publish_batch([])
            self.assertEqual(empty_batch.exception.code, "MEDALLION_INVALID_BATCH_SIZE")
            for payload in invalid:
                with self.subTest(payload=type(payload).__name__):
                    with self.assertRaises(MedallionError) as raised:
                        client.cdc.record(
                            stream_name="orders",
                            entity_type="order",
                            entity_id="1",
                            operation="insert",
                            idempotency_key="cdc:1",
                            payload=payload,
                        )
                    self.assertEqual(
                        raised.exception.code, "MEDALLION_INVALID_JSON_BODY"
                    )
            with self.assertRaises(MedallionError) as invalid_raw:
                client.cdc.record(
                    stream_name="orders",
                    entity_type="order",
                    entity_id="1",
                    operation="insert",
                    idempotency_key="cdc:raw-json",
                    payload_json='"\\ud800"',
                )
            self.assertEqual(
                invalid_raw.exception.code,
                "MEDALLION_INVALID_JSON_BODY",
            )
            with self.assertRaises(MedallionError) as owned:
                client.cdc.publish_batch(
                    [
                        {
                            "stream_name": "orders",
                            "entity_type": "order",
                            "entity_id": "1",
                            "operation": "insert",
                            "idempotency_key": "cdc:1",
                            "workspace_id": OTHER_WORKSPACE_ID,
                        }
                    ]
                )
            self.assertEqual(owned.exception.code, "MEDALLION_INVALID_CDC_EVENT")
        self.assertEqual(server.requests, [])

    def test_publish_rejects_out_of_range_analytical_timestamps_locally(self) -> None:
        with ScriptedServer([_publish_reply(["unused"])]) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as high_level:
                client.cdc.record(
                    stream_name="orders",
                    entity_type="order",
                    entity_id="1",
                    operation="insert",
                    idempotency_key="cdc:historical",
                    occurred_at="1800-01-01T00:00:00Z",
                )

            event = connect_pb2.AuditEvent(
                resource_type="order",
                resource_id="1",
                action="read",
                outcome=connect_pb2.AUDIT_EVENT_OUTCOME_SUCCEEDED,
                idempotency_key="audit:future",
                payload_json="{}",
            )
            event.occurred_at.FromJsonString("2262-04-11T23:47:16.854775808Z")
            with self.assertRaises(MedallionError) as low_level:
                client.connect.publish_audit_events(
                    connect_pb2.PublishAuditEventsRequest(
                        connector_id="connector-1",
                        events=[event],
                    )
                )

        self.assertEqual(high_level.exception.code, "MEDALLION_TIMESTAMP_OUT_OF_RANGE")
        self.assertEqual(low_level.exception.code, "MEDALLION_TIMESTAMP_OUT_OF_RANGE")
        self.assertEqual(server.requests, [])

    def test_mixed_publish_batch_has_explicit_mixed_result(self) -> None:
        reply = Reply(
            200,
            {
                "acceptedCount": 1,
                "duplicateCount": 1,
                "events": [
                    {
                        "idempotencyKey": "cdc:mixed:1",
                        "eventId": "1",
                        "duplicate": False,
                    },
                    {
                        "idempotencyKey": "cdc:mixed:2",
                        "eventId": "2",
                        "duplicate": True,
                    },
                ],
            },
        )
        with ScriptedServer([reply]) as server:
            result = _client(server).cdc.publish_batch(
                [
                    CdcEventInput("orders", "order", "1", "insert", "cdc:mixed:1"),
                    CdcEventInput("orders", "order", "2", "update", "cdc:mixed:2"),
                ]
            )

        self.assertEqual(result.result, "mixed")
        self.assertFalse(result.duplicate)

    def test_duplicate_batch_idempotency_keys_fail_before_network(self) -> None:
        with ScriptedServer([_publish_reply(["unused"])]) as server:
            client = _client(server)
            calls = (
                lambda: client.cdc.publish_batch(
                    [
                        CdcEventInput("orders", "order", "1", "insert", "same"),
                        CdcEventInput("customers", "order", "2", "update", "same"),
                    ]
                ),
                lambda: client.audit.publish_batch(
                    [
                        AuditEventInput("order", "1", "read", "succeeded", "same"),
                        AuditEventInput("order", "2", "read", "succeeded", "same"),
                    ]
                ),
                lambda: client.connect.publish_cdc_events(
                    connect_pb2.PublishCdcEventsRequest(
                        connector_id="connector-1",
                        events=[
                            connect_pb2.CdcEvent(
                                stream_name="customers",
                                entity_type="order",
                                entity_id="1",
                                operation=connect_pb2.CDC_OPERATION_INSERT,
                                idempotency_key="same",
                                payload_json="{}",
                            ),
                            connect_pb2.CdcEvent(
                                stream_name="orders",
                                entity_type="order",
                                entity_id="2",
                                operation=connect_pb2.CDC_OPERATION_UPDATE,
                                idempotency_key="same",
                                payload_json="{}",
                            ),
                        ],
                    )
                ),
                lambda: client.connect.publish_audit_events(
                    connect_pb2.PublishAuditEventsRequest(
                        connector_id="connector-1",
                        events=[
                            connect_pb2.AuditEvent(
                                resource_type="order",
                                resource_id="1",
                                action="read",
                                outcome=connect_pb2.AUDIT_EVENT_OUTCOME_SUCCEEDED,
                                idempotency_key="same",
                                payload_json="{}",
                            ),
                            connect_pb2.AuditEvent(
                                resource_type="order",
                                resource_id="2",
                                action="read",
                                outcome=connect_pb2.AUDIT_EVENT_OUTCOME_SUCCEEDED,
                                idempotency_key="same",
                                payload_json="{}",
                            ),
                        ],
                    )
                ),
            )
            for call in calls:
                with self.subTest(call=call):
                    with self.assertRaises(MedallionError) as raised:
                        call()
                    self.assertEqual(
                        raised.exception.code,
                        "MEDALLION_DUPLICATE_IDEMPOTENCY_KEY",
                    )
        self.assertEqual(server.requests, [])

    def test_publish_receipt_event_ids_must_be_strictly_positive(self) -> None:
        for family in ("cdc", "audit"):
            for event_id in (0, -1):
                key = f"{family}:{event_id}"
                reply = Reply(
                    200,
                    {
                        "acceptedCount": 1,
                        "duplicateCount": 0,
                        "events": [
                            {
                                "idempotencyKey": key,
                                "eventId": str(event_id),
                                "duplicate": False,
                            }
                        ],
                    },
                )
                with self.subTest(family=family, event_id=event_id):
                    with ScriptedServer([reply]) as server:
                        client = _client(server)
                        with self.assertRaises(MedallionError) as raised:
                            if family == "cdc":
                                client.cdc.record(
                                    stream_name="orders",
                                    entity_type="order",
                                    entity_id="1",
                                    operation="insert",
                                    idempotency_key=key,
                                )
                            else:
                                client.audit.record(
                                    resource_type="order",
                                    resource_id="1",
                                    action="read",
                                    outcome="succeeded",
                                    idempotency_key=key,
                                )
                    self.assertEqual(
                        raised.exception.code,
                        "MEDALLION_INVALID_PUBLISH_RESPONSE",
                    )
                    self.assertEqual(raised.exception.request_id, "request-1")

    def test_retry_reuses_exact_batch_and_idempotency_keys(self) -> None:
        details = [
            {
                "type": "google.rpc.ErrorInfo",
                "value": _error_info("BACKPRESSURE"),
            }
        ]
        with ScriptedServer(
            [
                Reply(
                    429,
                    {
                        "code": "resource_exhausted",
                        "message": "busy",
                        "details": details,
                    },
                    {"Retry-After": "0"},
                ),
                _publish_reply(["cdc:1", "cdc:2"]),
            ]
        ) as server:
            result = _client(server).cdc.publish_batch(
                [
                    CdcEventInput("orders", "order", "1", "insert", "cdc:1"),
                    CdcEventInput("orders", "order", "2", "update", "cdc:2"),
                ]
            )
        self.assertEqual(len(server.requests), 2)
        self.assertEqual(server.requests[0]["raw"], server.requests[1]["raw"])
        self.assertEqual(result.accepted_count, 2)

    def test_retry_after_is_not_capped_and_must_fit_total_deadline(self) -> None:
        with ScriptedServer(
            [
                Reply(
                    503,
                    {"code": "unavailable", "message": "retry later"},
                    {"Retry-After": "2"},
                ),
                _publish_reply(["cdc:retry-after"]),
            ]
        ) as server:
            with patch("medallion.request._wait") as wait:
                result = _client(server).cdc.record(
                    stream_name="orders",
                    entity_type="order",
                    entity_id="1",
                    operation="insert",
                    idempotency_key="cdc:retry-after",
                )
        self.assertEqual(result.accepted_count, 1)
        self.assertEqual(len(server.requests), 2)
        wait.assert_called_once()
        self.assertEqual(wait.call_args.args[0], 2.0)

        with ScriptedServer(
            [
                Reply(
                    503,
                    {"code": "unavailable", "message": "retry later"},
                    {"Retry-After": "60"},
                ),
                _publish_reply(["cdc:deadline"]),
            ]
        ) as server:
            with self.assertRaises(MedallionAPIError):
                _client(server, timeout=0.5).cdc.record(
                    stream_name="orders",
                    entity_type="order",
                    entity_id="1",
                    operation="insert",
                    idempotency_key="cdc:deadline",
                )
        self.assertEqual(len(server.requests), 1)

    def test_terminal_connect_codes_override_transient_http_status(self) -> None:
        for code in ("unauthenticated", "permission_denied", "invalid_argument"):
            with self.subTest(code=code):
                with ScriptedServer(
                    [
                        Reply(503, {"code": code, "message": "terminal"}),
                        _publish_reply([f"cdc:{code}"]),
                    ]
                ) as server:
                    with self.assertRaises(MedallionAPIError) as raised:
                        _client(server).cdc.record(
                            stream_name="orders",
                            entity_type="order",
                            entity_id="1",
                            operation="insert",
                            idempotency_key=f"cdc:{code}",
                        )
                self.assertEqual(raised.exception.connect_code, code)
                self.assertEqual(len(server.requests), 1)

    def test_api_error_retry_guidance_matches_transport_fallbacks(self) -> None:
        cases = (
            (
                "unknown future reason",
                MedallionAPIError(
                    "provider unavailable",
                    status=400,
                    connect_code="unavailable",
                    error_info_domain="medallion.jimtech.io",
                    reason="PROVIDER_UNAVAILABLE",
                ),
                False,
            ),
            (
                "transient Connect code",
                MedallionAPIError(
                    "unavailable",
                    status=400,
                    connect_code="unavailable",
                ),
                True,
            ),
            (
                "transient HTTP fallback",
                MedallionAPIError("unavailable", status=503),
                True,
            ),
            (
                "terminal Connect code overrides HTTP",
                MedallionAPIError(
                    "denied",
                    status=503,
                    connect_code="permission_denied",
                ),
                False,
            ),
            (
                "terminal HTTP fallback",
                MedallionAPIError("bad request", status=400),
                False,
            ),
        )
        for name, error, expected in cases:
            with self.subTest(name=name):
                self.assertEqual(
                    error.is_retryable(idempotent=True),
                    expected,
                )
                self.assertFalse(error.is_retryable(idempotent=False))

        unknown = cases[0][1]
        self.assertIsNone(unknown.known_reason)
        self.assertEqual(unknown.reason, "PROVIDER_UNAVAILABLE")

    def test_server_error_message_is_never_recorded_in_tracing(self) -> None:
        sentinel = "server-secret-trace-message"
        tracer = MagicMock()
        span = MagicMock()
        span_context = MagicMock()
        span_context.__enter__.return_value = span
        span_context.__exit__.return_value = False
        tracer.start_as_current_span.return_value = span_context

        with ScriptedServer(
            [Reply(400, {"code": "invalid_argument", "message": sentinel})]
        ) as server:
            client = _RequestClient(
                base_url=server.url,
                workspace_id=WORKSPACE_ID,
                api_key="fixture-api-key",
                tracing=TracingConfig(enabled=True, tracer=tracer),
            )
            with self.assertRaises(MedallionAPIError) as raised:
                _transport_post_json(
                    client,
                    "/medallion.connect.v1.MedallionConnectService/ListAuditEvents",
                )

        self.assertNotIn(sentinel, str(raised.exception))
        self.assertNotIn(sentinel, repr(span.method_calls))
        recorded = span.record_exception.call_args.args[0]
        self.assertEqual(str(recorded), "MEDALLION_API_ERROR")
        span.set_attribute.assert_any_call(
            "medallion.error.code",
            "MEDALLION_API_ERROR",
        )
        span.set_attribute.assert_any_call("medallion.error.type", "api")
        span.set_attribute.assert_any_call("medallion.request_id", "request-1")
        tracing_options = tracer.start_as_current_span.call_args.kwargs
        self.assertFalse(tracing_options["record_exception"])
        self.assertFalse(tracing_options["set_status_on_exception"])

    def test_idempotency_mismatch_is_terminal_and_error_is_structured(self) -> None:
        details = [
            {
                "type": "type.googleapis.com/google.rpc.ErrorInfo",
                "value": _error_info(
                    "IDEMPOTENCY_MISMATCH", {"scope": "connector/stream"}
                ),
            },
            {"type": "future.example.Detail", "value": "opaque"},
        ]
        with ScriptedServer(
            [
                Reply(
                    409,
                    {
                        "code": "already_exists",
                        "message": "key secret-api-key mismatch\n",
                        "details": details,
                    },
                ),
                _publish_reply(["cdc:1"]),
            ]
        ) as server:
            with self.assertRaises(MedallionAPIError) as raised:
                _client(server).cdc.record(
                    stream_name="orders",
                    entity_type="order",
                    entity_id="1",
                    operation="insert",
                    idempotency_key="cdc:1",
                )
        error = raised.exception
        self.assertEqual(len(server.requests), 1)
        self.assertEqual(error.status, 409)
        self.assertEqual(error.http_status, 409)
        self.assertEqual(error.connect_code, "already_exists")
        self.assertEqual(error.grpc_code, "ALREADY_EXISTS")
        self.assertEqual(error.error_info_domain, "medallion.jimtech.io")
        self.assertEqual(error.domain, "medallion.jimtech.io")
        self.assertEqual(error.reason, "IDEMPOTENCY_MISMATCH")
        self.assertEqual(error.known_reason, KnownErrorReason.IDEMPOTENCY_MISMATCH)
        self.assertEqual(error.metadata, {"scope": "connector/stream"})
        self.assertEqual(error.details[1]["type"], "future.example.Detail")
        self.assertEqual(error.request_id, "request-1")
        self.assertNotIn("secret-api-key", str(error))
        self.assertIsInstance(error.cause, OSError)
        self.assertFalse(error.is_retryable(idempotent=True))

    def test_error_fields_and_request_id_redact_echoed_credentials(self) -> None:
        secret = "secret-api-key"
        payload_secret = "customer-payload-secret"
        numeric_payload_secret = 98_765_432_123_456_789
        encoded_secret = base64.b64encode(f"echo:{secret}".encode()).decode()
        with ScriptedServer(
            [
                Reply(
                    400,
                    {
                        "code": f"invalid_argument:{secret}",
                        "message": f"credential {secret} rejected",
                        "details": [
                            {
                                "type": "future.example.Detail",
                                "value": encoded_secret,
                            },
                            {
                                "type": "google.rpc.ErrorInfo",
                                "value": _error_info(
                                    f"FUTURE_{secret}",
                                    {
                                        f"key-{secret}": f"value-{secret}",
                                        "echo": payload_secret,
                                        "numericEcho": str(numeric_payload_secret),
                                    },
                                ),
                            },
                        ],
                    },
                    {"x-request-id": f"request-{secret}"},
                )
            ]
        ) as server:
            with self.assertRaises(MedallionAPIError) as raised:
                _client(server).cdc.record(
                    stream_name="orders",
                    entity_type="order",
                    entity_id="1",
                    operation="insert",
                    idempotency_key="cdc:1",
                    payload={
                        "private": payload_secret,
                        "numericPrivate": numeric_payload_secret,
                    },
                )

        error = raised.exception
        public_fields = (
            str(error),
            error.connect_code,
            error.request_id,
            error.error_info_domain,
            error.reason,
            repr(error.metadata),
            repr(error.details),
            repr(error.response_body),
        )
        for value in public_fields:
            self.assertNotIn(secret, value or "")
            self.assertNotIn(payload_secret, value or "")
            self.assertNotIn(str(numeric_payload_secret), value or "")

    def test_every_known_and_unknown_error_reason_decodes(self) -> None:
        from medallion.errors import error_info_from_details

        reasons = [reason.value for reason in KnownErrorReason] + ["FUTURE_REASON"]
        for reason in reasons:
            with self.subTest(reason=reason):
                domain, decoded, metadata = error_info_from_details(
                    [
                        {
                            "type": "google.rpc.ErrorInfo",
                            "value": _error_info(reason, {"safe": "value"}),
                        }
                    ]
                )
                self.assertEqual(domain, "medallion.jimtech.io")
                self.assertEqual(decoded, reason)
                self.assertEqual(metadata, {"safe": "value"})

    def test_cdc_list_rejects_invalid_server_events_with_request_id(self) -> None:
        valid = {
            "id": "1",
            "workspaceId": WORKSPACE_ID,
            "streamName": "orders",
            "entityType": "order",
            "entityId": "1",
            "operation": "CDC_OPERATION_INSERT",
            "idempotencyKey": "cdc:1",
            "payloadJson": "{}",
        }
        invalid_events = (
            {**valid, "id": "0"},
            {**valid, "id": "-1"},
            {**valid, "workspaceId": OTHER_WORKSPACE_ID},
            {key: value for key, value in valid.items() if key != "operation"},
            {**valid, "operation": 99},
            {key: value for key, value in valid.items() if key != "streamName"},
            {key: value for key, value in valid.items() if key != "entityType"},
            {key: value for key, value in valid.items() if key != "entityId"},
            {key: value for key, value in valid.items() if key != "idempotencyKey"},
            {**valid, "idempotencyKey": "é" * 257},
            {**valid, "streamName": "é" * 129},
            {**valid, "payloadJson": "{"},
        )
        for event in invalid_events:
            with self.subTest(event=event):
                with ScriptedServer([Reply(200, {"events": [event]})]) as server:
                    with self.assertRaises(MedallionError) as raised:
                        _client(server).cdc.list()
                self.assertEqual(
                    raised.exception.code,
                    "MEDALLION_INVALID_LIST_RESPONSE",
                )
                self.assertEqual(raised.exception.request_id, "request-1")

    def test_audit_list_rejects_invalid_server_events_with_request_id(self) -> None:
        valid = {
            "id": "1",
            "workspaceId": WORKSPACE_ID,
            "resourceType": "order",
            "resourceId": "1",
            "action": "read",
            "origin": "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
            "outcome": "AUDIT_EVENT_OUTCOME_SUCCEEDED",
            "idempotencyKey": "audit:1",
            "payloadJson": "{}",
        }
        invalid_events = (
            {**valid, "id": "0"},
            {**valid, "id": "-1"},
            {**valid, "workspaceId": OTHER_WORKSPACE_ID},
            {key: value for key, value in valid.items() if key != "origin"},
            {**valid, "origin": 99},
            {key: value for key, value in valid.items() if key != "outcome"},
            {**valid, "outcome": 99},
            {key: value for key, value in valid.items() if key != "resourceType"},
            {key: value for key, value in valid.items() if key != "resourceId"},
            {key: value for key, value in valid.items() if key != "action"},
            {key: value for key, value in valid.items() if key != "idempotencyKey"},
            {**valid, "idempotencyKey": "é" * 257},
            {**valid, "action": "é" * 129},
            {**valid, "payloadJson": "{"},
        )
        for event in invalid_events:
            with self.subTest(event=event):
                with ScriptedServer([Reply(200, {"events": [event]})]) as server:
                    with self.assertRaises(MedallionError) as raised:
                        _client(server).audit.list()
                self.assertEqual(
                    raised.exception.code,
                    "MEDALLION_INVALID_LIST_RESPONSE",
                )
                self.assertEqual(raised.exception.request_id, "request-1")

    def test_readback_preserves_opaque_whitespace_idempotency_keys(self) -> None:
        key = " \t\nsource:雪:01 "
        cdc_event = {
            "id": "1",
            "workspaceId": WORKSPACE_ID,
            "streamName": "orders",
            "entityType": "order",
            "entityId": "1",
            "operation": "CDC_OPERATION_INSERT",
            "idempotencyKey": key,
            "payloadJson": "{}",
        }
        audit_event = {
            "id": "2",
            "workspaceId": WORKSPACE_ID,
            "resourceType": "order",
            "resourceId": "1",
            "action": "read",
            "origin": "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
            "outcome": "AUDIT_EVENT_OUTCOME_SUCCEEDED",
            "idempotencyKey": key,
            "payloadJson": "{}",
        }
        with ScriptedServer(
            [
                Reply(200, {"events": [cdc_event]}),
                Reply(200, {"events": [audit_event]}),
            ]
        ) as server:
            client = _client(server)
            cdc_page = client.cdc.list()
            audit_page = client.audit.list()

        self.assertEqual(cdc_page.events[0].idempotency_key, key)
        self.assertEqual(audit_page.events[0].idempotency_key, key)

    def test_pagination_handles_empty_pages_and_rejects_repeated_cursor(self) -> None:
        page = {
            "events": [
                {
                    "id": "9223372036854775000",
                    "streamName": "orders",
                    "entityType": "order",
                    "entityId": "1",
                    "operation": "CDC_OPERATION_INSERT",
                    "idempotencyKey": "cdc:1",
                    "payloadJson": "{}",
                    "occurredAt": "2026-08-01T01:02:03.123456789Z",
                    "workspaceId": WORKSPACE_ID,
                    "ingestedByPrincipal": "service_account:connect",
                    "description": "lossless CDC projection",
                }
            ],
            "nextPageCursor": "",
            "additiveField": True,
        }
        with ScriptedServer(
            [
                Reply(200, {"events": [], "nextPageCursor": "opaque-A"}),
                Reply(200, page),
            ]
        ) as server:
            events = list(_client(server).cdc.iterate(stream_name="orders"))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].event_id, "9223372036854775000")
        self.assertEqual(events[0].occurred_at, "2026-08-01T01:02:03.123456789Z")
        self.assertEqual(events[0].workspace_id, WORKSPACE_ID)
        self.assertEqual(
            events[0].ingester_principal,
            "service_account:connect",
        )
        self.assertEqual(events[0].description, "lossless CDC projection")
        self.assertEqual(events[0].payload_json, "{}")
        self.assertEqual(server.requests[1]["body"]["pageCursor"], "opaque-A")
        self.assertEqual(
            [request["body"]["workspaceId"] for request in server.requests],
            [WORKSPACE_ID, WORKSPACE_ID],
        )
        self.assertEqual(
            [
                request["headers"].get("x-medallion-workspace-id")
                for request in server.requests
            ],
            [WORKSPACE_ID, WORKSPACE_ID],
        )
        self.assertEqual(
            [request["body"]["streamName"] for request in server.requests],
            ["orders", "orders"],
        )

        repeated = Reply(200, {"events": [], "nextPageCursor": "same-cursor"})
        with ScriptedServer([repeated, repeated]) as server:
            with self.assertRaises(MedallionError) as raised:
                list(_client(server).audit.iterate())
        self.assertEqual(raised.exception.code, "MEDALLION_REPEATED_CURSOR")
        self.assertEqual(len(server.requests), 2)

    def test_iterators_stop_at_the_page_safety_limit(self) -> None:
        replies = [
            Reply(200, {"events": [], "nextPageCursor": "cursor-a"}),
            Reply(200, {"events": [], "nextPageCursor": "cursor-b"}),
            Reply(200, {"events": [], "nextPageCursor": "cursor-c"}),
        ]
        for family in ("cdc", "audit"):
            with self.subTest(family=family):
                with ScriptedServer(replies) as server:
                    client = _client(server)
                    with patch("medallion.client.MAX_ITERATOR_PAGES", 2):
                        with self.assertRaises(MedallionError) as raised:
                            if family == "cdc":
                                list(client.cdc.iterate())
                            else:
                                list(client.audit.iterate())
                self.assertEqual(
                    raised.exception.code,
                    "MEDALLION_PAGINATION_LIMIT",
                )
                self.assertEqual(raised.exception.request_id, "request-2")
                self.assertEqual(len(server.requests), 2)

    def test_stable_idempotency_helper_is_deterministic(self) -> None:
        first = stable_idempotency_key("orders", "partition-1", 42)
        self.assertEqual(first, "orders:103ec0f8-cc69-5f19-81d9-08f2d641a5e4")
        self.assertEqual(first, stable_idempotency_key("orders", "partition-1", 42))
        self.assertNotEqual(first, stable_idempotency_key("orders", "partition-1", 43))
        for values in (("x" * 512, "1"), ("orders", "\ud800")):
            with self.subTest(values=repr(values)):
                with self.assertRaises(MedallionError) as raised:
                    stable_idempotency_key(*values)
                self.assertEqual(
                    raised.exception.code,
                    "MEDALLION_INVALID_IDEMPOTENCY_KEY",
                )


if __name__ == "__main__":
    unittest.main()

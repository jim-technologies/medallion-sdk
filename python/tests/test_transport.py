from __future__ import annotations

import base64
import json
import threading
import unittest
from dataclasses import dataclass, field
from http.client import IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, build_opener

from medallion import (
    KnownErrorReason,
    MedallionAPIError,
    MedallionClient,
    MedallionError,
    RetryConfig,
    TracingConfig,
    ingest_pb2,
)
from medallion.request import _RequestClient, _retry_delay

WORKSPACE_ID = "ws_01jz9q5g6rsf7r5ar4rah1b2c3"
OTHER_WORKSPACE_ID = "ws_01jz9q5g6rsf7r5ar4rah1b2c4"
INGEST_SERVICE = "/medallion.ingest.v1.MedallionIngestService"
LIST_TABLES = f"{INGEST_SERVICE}/ListTables"
APPEND_ROWS = f"{INGEST_SERVICE}/AppendRows"


@dataclass(frozen=True)
class Reply:
    status: int
    body: dict[str, Any]
    headers: dict[str, str] = field(default_factory=dict)


class ScriptedServer:
    """Answers each POST with the next scripted reply, repeating the last."""

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
    """A source that redirects every POST to a target recording what it gets."""

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


class RawResponseServer:
    """Answers every POST with one fixed status and raw body."""

    def __init__(self, *, status: int, declare_length: bool, body: bytes) -> None:
        self.status = status
        self.declare_length = declare_length
        self.body = body

    def __enter__(self) -> RawResponseServer:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("content-length", "0"))
                self.rfile.read(length)
                self.send_response(outer.status)
                self.send_header("content-type", "application/json")
                self.send_header("x-request-id", "request-raw")
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


def _client(server: ScriptedServer, **options: Any) -> MedallionClient:
    return MedallionClient(
        base_url=server.url,
        api_key="secret-api-key",
        workspace_id=WORKSPACE_ID,
        retry=RetryConfig(
            max_attempts=3,
            initial_backoff=0.001,
            max_backoff=0.002,
            jitter_ratio=0,
        ),
        **options,
    )


def _list_tables(client: MedallionClient, **options: Any) -> Any:
    return client.ingest.list_tables(ingest_pb2.ListTablesRequest(), **options)


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


class TransportTests(unittest.TestCase):
    """Shared transport behavior, exercised through the retained ingest RPCs."""

    def test_api_key_workspace_and_canonical_headers(self) -> None:
        with ScriptedServer([Reply(200, {"tables": []})]) as server:
            _list_tables(_client(server))
        request = server.requests[0]
        self.assertEqual(request["path"], LIST_TABLES)
        headers = request["headers"]
        self.assertEqual(headers.get("x-medallion-api-key"), "secret-api-key")
        self.assertEqual(headers.get("x-medallion-workspace-id"), WORKSPACE_ID)
        self.assertEqual(headers.get("connect-protocol-version"), "1")
        self.assertEqual(headers.get_content_type(), "application/json")
        self.assertTrue(headers.get("connect-timeout-ms").isdigit())
        self.assertIsNone(headers.get("authorization"))
        self.assertNotIn("idempotency-key", headers)
        # Workspace identity rides only on the verified transport.
        self.assertNotIn("workspaceId", request["body"])

    def test_jwt_auth_and_ambiguous_configuration(self) -> None:
        with ScriptedServer([Reply(200, {"tables": []})]) as server:
            client = MedallionClient(
                base_url=server.url,
                access_token="jwt-value",
                workspace_id=WORKSPACE_ID,
            )
            _list_tables(client)
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

    def test_same_credential_cross_workspace_denial_is_preserved(self) -> None:
        with ScriptedServer(
            [
                Reply(200, {"tables": []}),
                Reply(403, {"code": "permission_denied", "message": "denied"}),
            ]
        ) as server:
            first = _client(server)
            second = MedallionClient(
                base_url=server.url,
                api_key="secret-api-key",
                workspace_id=OTHER_WORKSPACE_ID,
                retry=RetryConfig(max_attempts=3),
            )
            _list_tables(first)
            with self.assertRaises(MedallionAPIError) as raised:
                _list_tables(second)
        self.assertEqual(raised.exception.connect_code, "permission_denied")
        self.assertEqual(len(server.requests), 2)
        self.assertEqual(
            server.requests[1]["headers"].get("x-medallion-workspace-id"),
            OTHER_WORKSPACE_ID,
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
                            _transport_post_json(client, LIST_TABLES)

                    self.assertEqual(raised.exception.status, 307)
                    self.assertEqual(len(server.source_requests), 1)
                    self.assertEqual(server.source_requests[0].get(header), expected)
                    self.assertEqual(server.target_requests, [])

    def test_success_and_error_responses_are_bounded(self) -> None:
        for status in (200, 503):
            for declare_length in (True, False):
                with self.subTest(status=status, declare_length=declare_length):
                    with RawResponseServer(
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
                                _transport_post_json(client, LIST_TABLES)

                    self.assertNotIsInstance(raised.exception, MedallionAPIError)
                    self.assertEqual(
                        raised.exception.code, "MEDALLION_RESPONSE_TOO_LARGE"
                    )
                    self.assertEqual(raised.exception.request_id, "request-raw")

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

        no_backoff = RetryConfig(
            max_attempts=2, initial_backoff=0, max_backoff=0, jitter_ratio=0
        )
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
            retry=no_backoff,
        )
        envelope = retrying_client._post(
            LIST_TABLES,
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
                LIST_TABLES,
                b"{}",
                timeout=None,
                cancellation_event=None,
                retry_safe=False,
            )
        self.assertEqual(raised.exception.code, "MEDALLION_NETWORK_ERROR")
        self.assertEqual(raised.exception.request_id, "request-read-failure")
        self.assertIsInstance(raised.exception.__cause__, OSError)
        self.assertEqual(
            str(raised.exception.__cause__), "Medallion response body read failed."
        )
        self.assertNotIn(payload_secret.decode(), repr(raised.exception.__cause__))
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
            retry=no_backoff,
        )
        with self.assertRaises(MedallionError) as terminal:
            terminal_client._post(
                LIST_TABLES,
                b"{}",
                timeout=None,
                cancellation_event=None,
                retry_safe=True,
            )
        self.assertEqual(terminal.exception.code, "MEDALLION_NETWORK_ERROR")
        self.assertEqual(terminal.exception.request_id, "request-auth-read-failure")
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

    def test_every_ingest_rpc_rejects_empty_success_bodies(self) -> None:
        rpc_cases = (
            (
                "CreateTable",
                ingest_pb2.CreateTableRequest(),
                ingest_pb2.CreateTableResponse(),
            ),
            ("GetTable", ingest_pb2.GetTableRequest(), ingest_pb2.GetTableResponse()),
            (
                "ListTables",
                ingest_pb2.ListTablesRequest(),
                ingest_pb2.ListTablesResponse(),
            ),
            (
                "UpdateTable",
                ingest_pb2.UpdateTableRequest(),
                ingest_pb2.UpdateTableResponse(),
            ),
            (
                "AppendRows",
                ingest_pb2.AppendRowsRequest(),
                ingest_pb2.AppendRowsResponse(),
            ),
            ("RunQuery", ingest_pb2.RunQueryRequest(), ingest_pb2.RunQueryResponse()),
            (
                "GetQueryResults",
                ingest_pb2.GetQueryResultsRequest(),
                ingest_pb2.GetQueryResultsResponse(),
            ),
        )
        for status in (200, 204):
            for method, request, response in rpc_cases:
                with self.subTest(status=status, method=method):
                    with RawResponseServer(
                        status=status, declare_length=True, body=b""
                    ) as server:
                        client = _RequestClient(
                            base_url=server.url,
                            workspace_id=WORKSPACE_ID,
                            api_key="fixture-api-key",
                        )
                        with self.assertRaises(MedallionError) as raised:
                            client._post_proto(
                                f"{INGEST_SERVICE}/{method}", request, response
                            )
                    self.assertEqual(
                        raised.exception.code, "MEDALLION_INVALID_JSON_RESPONSE"
                    )
                    self.assertEqual(raised.exception.request_id, "request-raw")

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

    def test_removed_and_arbitrary_rpc_paths_fail_before_network(self) -> None:
        opener = MagicMock()
        client = _RequestClient(
            base_url="https://api.example.com",
            workspace_id=WORKSPACE_ID,
            api_key="fixture-api-key",
            opener=opener,
        )
        for path in (
            "/medallion.connect.v1.MedallionConnectService/PublishCdcEvents",
            "/medallion.connect.v1.MedallionConnectService/ListAuditEvents",
            "/medallion.connect.v1.MedallionConnectService/RegisterConnector",
            f"{INGEST_SERVICE}/DeleteTable",
        ):
            with self.subTest(path=path):
                with self.assertRaises(MedallionError) as raised:
                    _transport_post_json(client, path)
                self.assertEqual(raised.exception.code, "MEDALLION_UNSUPPORTED_RPC")
        opener.open.assert_not_called()

    def test_tracing_cannot_override_protected_headers(self) -> None:
        with ScriptedServer([Reply(200, {"tables": []})]) as server:
            client = _client(server, tracing=True)

            def inject_protected(_config: Any, headers: dict[str, str]) -> None:
                headers["X-Medallion-Workspace-Id"] = OTHER_WORKSPACE_ID

            with patch(
                "medallion.request.inject_trace_context",
                side_effect=inject_protected,
            ):
                with self.assertRaises(MedallionError) as raised:
                    _list_tables(client)
        self.assertEqual(raised.exception.code, "MEDALLION_PROTECTED_HEADER_OVERRIDE")
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

        with ScriptedServer([Reply(200, {"tables": []})]) as server:
            client = _client(server, tracing=True)
            with patch(
                "medallion.tracing.propagate.inject",
                side_effect=malicious_inject,
            ):
                _list_tables(client)

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
        with ScriptedServer([Reply(200, {"tables": []})]) as server:
            with self.assertRaises(MedallionError) as workspace:
                MedallionClient(
                    base_url=server.url,
                    api_key="key",
                    workspace_id=None,  # type: ignore[arg-type]
                )
            cancelled = threading.Event()
            cancelled.set()
            with self.assertRaises(MedallionError) as cancellation:
                _list_tables(_client(server), cancellation_event=cancelled)
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

    def test_append_retry_reuses_exact_batch_and_idempotency_key(self) -> None:
        details = [
            {"type": "google.rpc.ErrorInfo", "value": _error_info("BACKPRESSURE")}
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
                Reply(200, {"acceptedRows": "2"}),
            ]
        ) as server:
            result = _client(server).tables.append(
                "events",
                [{"level": "info"}, {"level": "warn"}],
                idempotency_key="outbox:batch:7",
            )
        self.assertEqual(len(server.requests), 2)
        first, second = server.requests
        self.assertEqual(first["path"], APPEND_ROWS)
        self.assertEqual(first["raw"], second["raw"])
        self.assertEqual(first["headers"].get("idempotency-key"), "outbox:batch:7")
        self.assertEqual(second["headers"].get("idempotency-key"), "outbox:batch:7")
        self.assertEqual(second["body"]["requestId"], "outbox:batch:7")
        self.assertEqual(result.accepted_rows, 2)

    def test_retry_after_is_not_capped_and_must_fit_total_deadline(self) -> None:
        with ScriptedServer(
            [
                Reply(
                    503,
                    {"code": "unavailable", "message": "retry later"},
                    {"Retry-After": "2"},
                ),
                Reply(200, {"tables": []}),
            ]
        ) as server:
            with patch("medallion.request._wait") as wait:
                _list_tables(_client(server))
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
                Reply(200, {"tables": []}),
            ]
        ) as server:
            with self.assertRaises(MedallionAPIError):
                _list_tables(_client(server, timeout=0.5))
        self.assertEqual(len(server.requests), 1)

    def test_terminal_connect_codes_override_transient_http_status(self) -> None:
        for code in ("unauthenticated", "permission_denied", "invalid_argument"):
            with self.subTest(code=code):
                with ScriptedServer(
                    [
                        Reply(503, {"code": code, "message": "terminal"}),
                        Reply(200, {"tables": []}),
                    ]
                ) as server:
                    with self.assertRaises(MedallionAPIError) as raised:
                        _list_tables(_client(server))
                self.assertEqual(raised.exception.connect_code, code)
                self.assertEqual(len(server.requests), 1)

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
                _transport_post_json(client, LIST_TABLES)

        self.assertNotIn(sentinel, str(raised.exception))
        self.assertNotIn(sentinel, repr(span.method_calls))
        recorded = span.record_exception.call_args.args[0]
        self.assertEqual(str(recorded), "MEDALLION_API_ERROR")
        span.set_attribute.assert_any_call(
            "medallion.error.code", "MEDALLION_API_ERROR"
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
                "value": _error_info("IDEMPOTENCY_MISMATCH", {"scope": "table/batch"}),
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
                Reply(200, {"acceptedRows": "1"}),
            ]
        ) as server:
            with self.assertRaises(MedallionAPIError) as raised:
                _client(server).tables.append(
                    "events", [{"level": "info"}], idempotency_key="outbox:batch:8"
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
        self.assertEqual(error.metadata, {"scope": "table/batch"})
        self.assertEqual(error.details[1]["type"], "future.example.Detail")
        self.assertEqual(error.request_id, "request-1")
        self.assertNotIn("secret-api-key", str(error))
        self.assertIsInstance(error.cause, OSError)
        self.assertFalse(error.is_retryable(idempotent=True))

    def test_error_fields_and_request_id_redact_echoed_credentials(self) -> None:
        for auth_name, auth, secret in (
            ("api-key", {"api_key": "secret-api-key"}, "secret-api-key"),
            ("jwt", {"access_token": "secret-jwt-value"}, "secret-jwt-value"),
        ):
            encoded_secret = base64.b64encode(f"echo:{secret}".encode()).decode()
            with self.subTest(auth=auth_name):
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
                                            {f"key-{secret}": f"value-{secret}"},
                                        ),
                                    },
                                ],
                            },
                            {"x-request-id": f"request-{secret}"},
                        )
                    ]
                ) as server:
                    client = MedallionClient(
                        base_url=server.url, workspace_id=WORKSPACE_ID, **auth
                    )
                    with self.assertRaises(MedallionAPIError) as raised:
                        _list_tables(client)

                error = raised.exception
                for value in (
                    str(error),
                    repr(error),
                    error.connect_code,
                    error.request_id,
                    error.error_info_domain,
                    error.reason,
                    repr(error.metadata),
                    repr(error.details),
                    repr(error.response_body),
                ):
                    self.assertNotIn(secret, value or "")
                self.assertEqual(error.request_id, "request-[REDACTED]")
                self.assertEqual(error.details[0]["value"], "[REDACTED]")

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


if __name__ == "__main__":
    unittest.main()

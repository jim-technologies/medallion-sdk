from __future__ import annotations

import base64
import io
import json
import math
import re
import threading
import unittest
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import polars as pl
import pyarrow as pa

from medallion import MedallionClient, MedallionError, TableColumn

WORKSPACE_ID = "ws_01jz9q5g6rsf7r5ar4rah1b2c3"
INGEST_SERVICE = "/medallion.ingest.v1.MedallionIngestService"
QUERY_NAME = "queries/01jz9q5g6rsf7r5ar4rah1b2c3"
UUID_TEXT = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
SCHEMA = [
    TableColumn(name="occurred_at", type="TIMESTAMP"),
    TableColumn(name="level", type="STRING"),
]

Responder = Callable[[str, dict[str, Any]], dict[str, Any]]


class IngestServer:
    """Captures ingest requests and answers from a scripted responder."""

    def __init__(self, responder: Responder) -> None:
        self.responder = responder
        self.requests: list[dict[str, Any]] = []

    def __enter__(self) -> IngestServer:
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("content-length", "0"))
                body = json.loads(self.rfile.read(length).decode("utf-8"))
                outer.requests.append(
                    {"path": self.path, "headers": self.headers, "body": body}
                )
                payload = outer.responder(self.path, body)
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("x-request-id", "req_ingest")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode("utf-8"))

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


def _client(server: IngestServer) -> MedallionClient:
    return MedallionClient(
        base_url=server.url,
        api_key="fixture-api-key",
        workspace_id=WORKSPACE_ID,
    )


def _accept_rows(count: int) -> Responder:
    def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
        return {"acceptedRows": str(count)}

    return responder


class TableAppendTests(unittest.TestCase):
    def test_dict_rows_use_canonical_route_and_generated_key(self) -> None:
        with IngestServer(_accept_rows(2)) as server:
            client = _client(server)
            result = client.tables.append(
                "events",
                [{"level": "info", "count": 1}, {"level": "warn", "count": 2}],
                insert_ids=["evt-1", None],
            )

            request = server.requests[0]
            self.assertEqual(request["path"], f"{INGEST_SERVICE}/AppendRows")
            self.assertRegex(request["headers"]["Idempotency-Key"], UUID_TEXT)
            self.assertEqual(
                request["headers"]["X-Medallion-Workspace-Id"], WORKSPACE_ID
            )
            self.assertEqual(request["body"]["table"], "tables/events")
            # request_id is the field the contract deduplicates on.
            self.assertEqual(request["body"]["requestId"], result.idempotency_key)
            rows = request["body"]["rows"]
            self.assertEqual(
                rows[0],
                {"insertId": "evt-1", "json": {"level": "info", "count": 1}},
            )
            self.assertEqual(rows[1], {"json": {"level": "warn", "count": 2}})
            self.assertEqual(result.accepted_rows, 2)
            self.assertEqual(result.row_errors, [])
            self.assertRegex(result.idempotency_key, UUID_TEXT)
            self.assertEqual(result.request_id, "req_ingest")

    def test_caller_key_reaches_both_the_header_and_request_id(self) -> None:
        with IngestServer(_accept_rows(1)) as server:
            client = _client(server)
            result = client.tables.append(
                "events",
                [{"ok": True}],
                idempotency_key="outbox:batch:42",
            )
            self.assertEqual(
                server.requests[0]["headers"]["Idempotency-Key"],
                "outbox:batch:42",
            )
            self.assertEqual(server.requests[0]["body"]["requestId"], "outbox:batch:42")
            self.assertEqual(result.idempotency_key, "outbox:batch:42")

    def test_per_row_errors_surface_with_skip_invalid_rows(self) -> None:
        def responder(_path: str, body: dict[str, Any]) -> dict[str, Any]:
            assert body["skipInvalidRows"] is True
            return {
                "acceptedRows": "1",
                "rowErrors": [
                    {
                        "index": "1",
                        "error": {"code": 3, "message": "count expects an integer"},
                    }
                ],
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.tables.append(
                "events",
                [{"count": 1}, {"count": "oops"}],
                skip_invalid_rows=True,
            )
            self.assertEqual(result.accepted_rows, 1)
            self.assertEqual(result.row_errors[0].index, 1)
            self.assertEqual(result.row_errors[0].code, 3)
            self.assertEqual(result.row_errors[0].message, "count expects an integer")

    def test_polars_dataframe_rides_as_one_arrow_ipc_stream(self) -> None:
        frame = pl.DataFrame({"level": ["info", "warn"], "count": [1, 2]})
        with IngestServer(_accept_rows(2)) as server:
            client = _client(server)
            result = client.tables.append("events", frame)

            body = server.requests[0]["body"]
            self.assertNotIn("rows", body)
            payload = base64.b64decode(body["arrowRows"]["serializedRecordBatch"])
            table = pa.ipc.open_stream(io.BytesIO(payload)).read_all()
            self.assertEqual(table.num_rows, 2)
            self.assertEqual(table.column("level").to_pylist(), ["info", "warn"])
            self.assertEqual(result.accepted_rows, 2)

    def test_pyarrow_and_raw_ipc_bytes_pass_through(self) -> None:
        table = pa.table({"n": [1, 2, 3]})
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        raw = sink.getvalue()

        with IngestServer(_accept_rows(3)) as server:
            client = _client(server)
            client.tables.append("events", table)
            client.tables.append("events", raw)

            for request in server.requests:
                payload = base64.b64decode(
                    request["body"]["arrowRows"]["serializedRecordBatch"]
                )
                decoded = pa.ipc.open_stream(io.BytesIO(payload)).read_all()
                self.assertEqual(decoded.num_rows, 3)

    def test_local_validation_rejects_bad_batches_before_network(self) -> None:
        def rejecting(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            raise AssertionError("no request may reach the network")

        with IngestServer(rejecting) as server:
            client = _client(server)
            cases: list[tuple[dict[str, Any], str]] = [
                ({"rows": []}, "MEDALLION_INVALID_BATCH_SIZE"),
                ({"rows": [["not", "a", "mapping"]]}, "MEDALLION_INVALID_ROW"),
                (
                    {"rows": [{"ok": True}], "insert_ids": ["a", "b"]},
                    "MEDALLION_INVALID_ROW",
                ),
                (
                    {"rows": [{"bad": math.inf}]},
                    "MEDALLION_INVALID_ROW",
                ),
                ({"rows": b""}, "MEDALLION_INVALID_ROW"),
                (
                    {"rows": pa.table({"n": [1]}), "insert_ids": ["a"]},
                    "MEDALLION_INVALID_ROW",
                ),
                (
                    {"rows": [{"ok": True}], "idempotency_key": "bad key"},
                    "MEDALLION_INVALID_IDEMPOTENCY_KEY",
                ),
            ]
            for kwargs, code in cases:
                with self.subTest(code=code, kwargs=kwargs):
                    with self.assertRaises(MedallionError) as raised:
                        client.tables.append(
                            "events",
                            kwargs["rows"],
                            insert_ids=kwargs.get("insert_ids"),
                            idempotency_key=kwargs.get("idempotency_key"),
                        )
                    self.assertEqual(raised.exception.code, code)
            with self.assertRaises(MedallionError) as raised:
                client.tables.append("Events", [{"ok": True}])
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_TABLE_ID")
            self.assertEqual(server.requests, [])

    def test_acknowledgements_must_match_the_submitted_batch(self) -> None:
        def over_counted(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {"acceptedRows": "5"}

        with IngestServer(over_counted) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.tables.append("events", [{"ok": True}])
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_INGEST_RESPONSE")

        def out_of_range(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {"acceptedRows": "0", "rowErrors": [{"index": "7"}]}

        with IngestServer(out_of_range) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.tables.append("events", [{"ok": True}])
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_INGEST_RESPONSE")


class TableQueryTests(unittest.TestCase):
    def test_succeeded_query_iterates_rows(self) -> None:
        def responder(_path: str, body: dict[str, Any]) -> dict[str, Any]:
            assert body["query"] == "SELECT level, count FROM events"
            return {
                "name": QUERY_NAME,
                "state": "SUCCEEDED",
                "schema": {
                    "columns": [
                        {"name": "level", "type": "STRING"},
                        {"name": "count", "type": "INT64", "nullable": True},
                    ]
                },
                "rows": [{"level": "info", "count": 1}],
                "totalRows": "1",
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.tables.query(
                "SELECT level, count FROM events", server_timeout_ms=2_000
            )
            self.assertEqual(server.requests[0]["path"], f"{INGEST_SERVICE}/RunQuery")
            self.assertEqual(server.requests[0]["body"]["timeoutMs"], "2000")
            self.assertNotIn("Idempotency-Key", server.requests[0]["headers"])
            self.assertEqual(result.query_name, QUERY_NAME)
            self.assertEqual(
                [(column.name, column.nullable) for column in result.columns],
                [("level", False), ("count", True)],
            )
            self.assertEqual(result.total_rows, 1)
            self.assertEqual(list(result), [{"level": "info", "count": 1}])

    def test_query_polls_while_running_then_returns_rows(self) -> None:
        calls = {"count": 0}

        def responder(path: str, _body: dict[str, Any]) -> dict[str, Any]:
            calls["count"] += 1
            if calls["count"] <= 2:
                return {"name": QUERY_NAME, "state": "RUNNING"}
            assert path == f"{INGEST_SERVICE}/GetQueryResults"
            return {
                "name": QUERY_NAME,
                "state": "SUCCEEDED",
                "rows": [{"n": 1}],
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.tables.query("SELECT n FROM events")
            self.assertEqual(list(result.rows()), [{"n": 1}])
            self.assertEqual(len(server.requests), 3)
            self.assertEqual(
                server.requests[1]["path"], f"{INGEST_SERVICE}/GetQueryResults"
            )
            self.assertEqual(server.requests[1]["body"]["name"], QUERY_NAME)

    def test_rows_paginate_without_exposing_page_tokens(self) -> None:
        def responder(path: str, body: dict[str, Any]) -> dict[str, Any]:
            if path.endswith("/RunQuery"):
                return {
                    "name": QUERY_NAME,
                    "state": "SUCCEEDED",
                    "rows": [{"n": 1}, {"n": 2}],
                    "nextPageToken": "page-2",
                }
            assert body["pageToken"] == "page-2"
            return {
                "name": QUERY_NAME,
                "state": "SUCCEEDED",
                "rows": [{"n": 3}],
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.tables.query("SELECT n FROM events")
            self.assertEqual(list(result), [{"n": 1}, {"n": 2}, {"n": 3}])
            self.assertEqual(len(server.requests), 2)

    def test_repeated_continuation_token_stops_iteration(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {
                "name": QUERY_NAME,
                "state": "SUCCEEDED",
                "rows": [{"n": 1}],
                "nextPageToken": "loop",
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.tables.query("SELECT n FROM events")
            with self.assertRaises(MedallionError) as raised:
                list(result)
            self.assertEqual(raised.exception.code, "MEDALLION_REPEATED_CURSOR")

    def test_running_acknowledgement_requires_a_resource_name(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {"state": "RUNNING"}

        with IngestServer(responder) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.tables.query("SELECT 1")
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_INGEST_RESPONSE")

    def test_failed_query_raises_the_reported_cause(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {
                "name": QUERY_NAME,
                "state": "FAILED",
                "error": {"code": 13, "message": "query was abandoned by its executor"},
            }

        with IngestServer(responder) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.tables.query("SELECT sleep(3600)")
            self.assertEqual(raised.exception.code, "MEDALLION_QUERY_FAILED")
            self.assertIn("abandoned", str(raised.exception))

    def test_dry_run_reports_a_schema_without_rows_or_a_name(self) -> None:
        def responder(_path: str, body: dict[str, Any]) -> dict[str, Any]:
            assert body["dryRun"] is True
            return {
                "state": "SUCCEEDED",
                "schema": {"columns": [{"name": "n", "type": "INT64"}]},
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.tables.query("SELECT n FROM events", dry_run=True)
            self.assertTrue(result.dry_run)
            self.assertIsNone(result.query_name)
            self.assertEqual([column.name for column in result.columns], ["n"])
            self.assertEqual(list(result), [])

    def test_to_polars_collects_rows(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {
                "name": QUERY_NAME,
                "state": "SUCCEEDED",
                "rows": [
                    {"level": "info", "count": 1},
                    {"level": "warn", "count": 2},
                ],
            }

        with IngestServer(responder) as server:
            client = _client(server)
            frame = client.tables.query("SELECT * FROM events").to_polars()
            self.assertEqual(frame.shape, (2, 2))
            self.assertEqual(frame["level"].to_list(), ["info", "warn"])

    def test_result_is_single_consumption(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {"name": QUERY_NAME, "state": "SUCCEEDED", "rows": [{"n": 1}]}

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.tables.query("SELECT n FROM events")
            self.assertEqual(list(result), [{"n": 1}])
            with self.assertRaises(MedallionError) as raised:
                list(result)
            self.assertEqual(raised.exception.code, "MEDALLION_RESULT_CONSUMED")

    def test_rejects_empty_statement_and_out_of_range_options_locally(self) -> None:
        def rejecting(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            raise AssertionError("no request may reach the network")

        with IngestServer(rejecting) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.tables.query("   ")
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_QUERY")
            with self.assertRaises(MedallionError) as raised:
                client.tables.query("SELECT 1", server_timeout_ms=900_000)
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_OPTIONS")
            with self.assertRaises(MedallionError) as raised:
                client.tables.query("SELECT 1", page_size=200_000)
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_PAGE_SIZE")
            self.assertEqual(server.requests, [])


class TableManagementTests(unittest.TestCase):
    def test_create_update_get_and_iterate_tables(self) -> None:
        def responder(path: str, body: dict[str, Any]) -> dict[str, Any]:
            if path.endswith("/CreateTable"):
                return {
                    "table": {
                        "name": f"tables/{body['tableId']}",
                        "schema": body["table"]["schema"],
                        "timeColumn": body["table"]["timeColumn"],
                        "sortColumns": ["occurred_at"],
                        "createTime": "2026-08-29T00:00:00Z",
                    }
                }
            if path.endswith("/UpdateTable"):
                return {"table": {**body["table"], "timeColumn": "occurred_at"}}
            if path.endswith("/GetTable"):
                return {"table": {"name": body["name"]}}
            if body.get("pageToken") == "p2":
                return {"tables": [{"name": "tables/b"}]}
            return {
                "tables": [{"name": "tables/a"}],
                "nextPageToken": "p2",
            }

        with IngestServer(responder) as server:
            client = _client(server)
            created = client.tables.create(
                "events", columns=SCHEMA, time_column="occurred_at"
            )
            self.assertEqual(created.table_id, "events")
            self.assertEqual(created.name, "tables/events")
            self.assertEqual(created.time_column, "occurred_at")
            self.assertEqual(created.sort_columns, ["occurred_at"])
            self.assertEqual(created.create_time, "2026-08-29T00:00:00Z")
            self.assertRegex(
                server.requests[0]["headers"]["Idempotency-Key"], UUID_TEXT
            )
            self.assertRegex(server.requests[0]["body"]["requestId"], UUID_TEXT)

            evolved = client.tables.update(
                "events",
                columns=[
                    *SCHEMA,
                    TableColumn(name="trace_id", type="STRING", nullable=True),
                ],
            )
            self.assertEqual(evolved.columns[-1].name, "trace_id")
            self.assertTrue(evolved.columns[-1].nullable)
            self.assertEqual(
                server.requests[1]["body"]["table"]["name"], "tables/events"
            )

            found = client.tables.get("events")
            self.assertEqual(found.table_id, "events")

            names = [item.table_id for item in client.tables.iterate()]
            self.assertEqual(names, ["a", "b"])

    def test_local_schema_validation_rejects_bad_declarations(self) -> None:
        def rejecting(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            raise AssertionError("no request may reach the network")

        with IngestServer(rejecting) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.tables.create("events", columns=[], time_column="occurred_at")
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_SCHEMA")
            with self.assertRaises(MedallionError) as raised:
                client.tables.create(
                    "events",
                    columns=[{"name": "occurred_at", "type": "DATETIME"}],
                    time_column="occurred_at",
                )
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_SCHEMA")
            with self.assertRaises(MedallionError) as raised:
                client.tables.create("events", columns=SCHEMA, time_column="missing")
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_SCHEMA")
            with self.assertRaises(MedallionError) as raised:
                client.tables.create(
                    "Events", columns=SCHEMA, time_column="occurred_at"
                )
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_TABLE_ID")
            self.assertEqual(server.requests, [])

    def test_table_acknowledgement_requires_a_resource_name(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {"table": {}}

        with IngestServer(responder) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.tables.get("events")
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_INGEST_RESPONSE")


if __name__ == "__main__":
    unittest.main()

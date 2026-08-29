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

from medallion import MedallionClient, MedallionError

WORKSPACE_ID = "ws_01jz9q5g6rsf7r5ar4rah1b2c3"
INGEST_SERVICE = "/medallion.ingest.v1.MedallionIngestService"
UUID_TEXT = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)

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


class DatasetAppendTests(unittest.TestCase):
    def test_dict_rows_use_canonical_route_and_generated_key(self) -> None:
        with IngestServer(_accept_rows(2)) as server:
            client = _client(server)
            result = client.datasets.append(
                "events",
                [{"level": "info", "count": 1}, {"level": "warn", "count": 2}],
                insert_ids=["evt-1", None],
            )

            request = server.requests[0]
            self.assertEqual(request["path"], f"{INGEST_SERVICE}/Append")
            self.assertRegex(request["headers"]["Idempotency-Key"], UUID_TEXT)
            self.assertEqual(
                request["headers"]["X-Medallion-Workspace-Id"], WORKSPACE_ID
            )
            rows = request["body"]["jsonRows"]["rows"]
            self.assertEqual(
                rows[0],
                {"insertId": "evt-1", "json": '{"count":1,"level":"info"}'},
            )
            self.assertEqual(rows[1], {"json": '{"count":2,"level":"warn"}'})
            self.assertEqual(result.accepted_rows, 2)
            self.assertFalse(result.duplicate)
            self.assertEqual(result.row_errors, [])
            self.assertRegex(result.idempotency_key, UUID_TEXT)
            self.assertEqual(result.request_id, "req_ingest")

    def test_caller_key_passes_through_and_replay_is_reported(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {"acceptedRows": "0", "duplicate": True}

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.datasets.append(
                "events",
                [{"ok": True}],
                idempotency_key="outbox:batch:42",
            )
            self.assertEqual(
                server.requests[0]["headers"]["Idempotency-Key"],
                "outbox:batch:42",
            )
            self.assertEqual(result.idempotency_key, "outbox:batch:42")
            self.assertTrue(result.duplicate)

    def test_per_row_errors_surface_from_the_acknowledgement(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {
                "acceptedRows": "1",
                "insertErrors": [
                    {"index": 1, "reason": "TYPE_MISMATCH", "message": "count"}
                ],
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.datasets.append("events", [{"count": 1}, {"count": "oops"}])
            self.assertEqual(result.accepted_rows, 1)
            self.assertEqual(result.row_errors[0].index, 1)
            self.assertEqual(result.row_errors[0].reason, "TYPE_MISMATCH")

    def test_polars_dataframe_rides_as_one_arrow_ipc_stream(self) -> None:
        frame = pl.DataFrame({"level": ["info", "warn"], "count": [1, 2]})
        with IngestServer(_accept_rows(2)) as server:
            client = _client(server)
            result = client.datasets.append("events", frame)

            body = server.requests[0]["body"]
            self.assertNotIn("jsonRows", body)
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
            client.datasets.append("events", table)
            client.datasets.append("events", raw)

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
                        client.datasets.append(
                            "events",
                            kwargs["rows"],
                            insert_ids=kwargs.get("insert_ids"),
                            idempotency_key=kwargs.get("idempotency_key"),
                        )
                    self.assertEqual(raised.exception.code, code)
            with self.assertRaises(MedallionError) as raised:
                client.datasets.append("", [{"ok": True}])
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_DATASET_ID")
            self.assertEqual(server.requests, [])

    def test_acknowledgements_must_match_the_submitted_batch(self) -> None:
        def over_counted(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {"acceptedRows": "5"}

        with IngestServer(over_counted) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.datasets.append("events", [{"ok": True}])
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_INGEST_RESPONSE")

        def out_of_range(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {"acceptedRows": "0", "insertErrors": [{"index": 7}]}

        with IngestServer(out_of_range) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.datasets.append("events", [{"ok": True}])
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_INGEST_RESPONSE")


class DatasetQueryTests(unittest.TestCase):
    def test_completed_query_iterates_parsed_rows(self) -> None:
        def responder(_path: str, body: dict[str, Any]) -> dict[str, Any]:
            assert body["query"] == "SELECT level, count FROM events"
            return {
                "results": {
                    "completed": True,
                    "schema": {
                        "columns": [
                            {"name": "level", "type": "String"},
                            {"name": "count", "type": "UInt64"},
                        ]
                    },
                    "rowsJson": ['{"level":"info","count":1}'],
                    "totalRows": "1",
                    "totalBytesProcessed": "128",
                }
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.datasets.query(
                "SELECT level, count FROM events", server_timeout_ms=2_000
            )
            self.assertEqual(server.requests[0]["body"]["timeoutMs"], "2000")
            self.assertNotIn("Idempotency-Key", server.requests[0]["headers"])
            self.assertEqual(
                [column.name for column in result.columns], ["level", "count"]
            )
            self.assertEqual(result.total_rows, 1)
            self.assertEqual(result.total_bytes_processed, 128)
            self.assertEqual(list(result), [{"level": "info", "count": 1}])

    def test_query_polls_while_running_then_returns_rows(self) -> None:
        calls = {"count": 0}

        def responder(path: str, _body: dict[str, Any]) -> dict[str, Any]:
            calls["count"] += 1
            if calls["count"] <= 2:
                return {"results": {"completed": False, "queryId": "q_1"}}
            assert path == f"{INGEST_SERVICE}/GetQueryResults"
            return {
                "results": {
                    "completed": True,
                    "queryId": "q_1",
                    "rowsJson": ['{"n":1}'],
                }
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.datasets.query("SELECT n FROM events")
            self.assertEqual(list(result.rows()), [{"n": 1}])
            self.assertEqual(len(server.requests), 3)
            self.assertEqual(
                server.requests[1]["path"], f"{INGEST_SERVICE}/GetQueryResults"
            )
            self.assertEqual(server.requests[1]["body"]["queryId"], "q_1")

    def test_rows_paginate_without_exposing_page_tokens(self) -> None:
        def responder(path: str, body: dict[str, Any]) -> dict[str, Any]:
            if path.endswith("/Query"):
                return {
                    "results": {
                        "completed": True,
                        "queryId": "q_2",
                        "rowsJson": ['{"n":1}', '{"n":2}'],
                        "nextPageToken": "page-2",
                    }
                }
            assert body["pageToken"] == "page-2"
            return {
                "results": {
                    "completed": True,
                    "queryId": "q_2",
                    "rowsJson": ['{"n":3}'],
                }
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.datasets.query("SELECT n FROM events")
            self.assertEqual(list(result), [{"n": 1}, {"n": 2}, {"n": 3}])
            self.assertEqual(len(server.requests), 2)

    def test_repeated_continuation_token_stops_iteration(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {
                "results": {
                    "completed": True,
                    "queryId": "q_3",
                    "rowsJson": ['{"n":1}'],
                    "nextPageToken": "loop",
                }
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.datasets.query("SELECT n FROM events")
            with self.assertRaises(MedallionError) as raised:
                list(result)
            self.assertEqual(raised.exception.code, "MEDALLION_REPEATED_CURSOR")

    def test_running_acknowledgement_requires_query_id(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {"results": {"completed": False}}

        with IngestServer(responder) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.datasets.query("SELECT 1")
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_INGEST_RESPONSE")

    def test_dry_run_reports_estimate_without_rows(self) -> None:
        def responder(_path: str, body: dict[str, Any]) -> dict[str, Any]:
            assert body["dryRun"] is True
            return {
                "results": {
                    "completed": True,
                    "schema": {"columns": [{"name": "n", "type": "UInt8"}]},
                    "totalBytesProcessed": "4096",
                }
            }

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.datasets.query("SELECT n FROM events", dry_run=True)
            self.assertTrue(result.dry_run)
            self.assertEqual(result.total_bytes_processed, 4_096)
            self.assertEqual(list(result), [])

    def test_to_polars_collects_json_rows(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {
                "results": {
                    "completed": True,
                    "rowsJson": [
                        '{"level":"info","count":1}',
                        '{"level":"warn","count":2}',
                    ],
                }
            }

        with IngestServer(responder) as server:
            client = _client(server)
            frame = client.datasets.query("SELECT * FROM events").to_polars()
            self.assertEqual(frame.shape, (2, 2))
            self.assertEqual(frame["level"].to_list(), ["info", "warn"])

    def test_to_polars_reads_arrow_pages(self) -> None:
        table = pa.table({"n": [1, 2, 3]})
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        encoded = base64.b64encode(sink.getvalue()).decode("ascii")

        def responder(_path: str, body: dict[str, Any]) -> dict[str, Any]:
            assert body["format"] == "RESULT_FORMAT_ARROW_IPC"
            return {
                "results": {
                    "completed": True,
                    "arrowRows": {"serializedRecordBatch": encoded},
                }
            }

        with IngestServer(responder) as server:
            client = _client(server)
            frame = client.datasets.query(
                "SELECT n FROM events", format="arrow"
            ).to_polars()
            self.assertEqual(frame["n"].to_list(), [1, 2, 3])

    def test_result_is_single_consumption(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {"results": {"completed": True, "rowsJson": ['{"n":1}']}}

        with IngestServer(responder) as server:
            client = _client(server)
            result = client.datasets.query("SELECT n FROM events")
            self.assertEqual(list(result), [{"n": 1}])
            with self.assertRaises(MedallionError) as raised:
                list(result)
            self.assertEqual(raised.exception.code, "MEDALLION_RESULT_CONSUMED")

    def test_rejects_empty_statement_and_unknown_format_locally(self) -> None:
        def rejecting(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            raise AssertionError("no request may reach the network")

        with IngestServer(rejecting) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.datasets.query("   ")
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_QUERY")
            with self.assertRaises(MedallionError) as raised:
                client.datasets.query("SELECT 1", format="csv")
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_QUERY")
            self.assertEqual(server.requests, [])


class DatasetManagementTests(unittest.TestCase):
    def test_create_get_and_iterate_datasets(self) -> None:
        def responder(path: str, body: dict[str, Any]) -> dict[str, Any]:
            if path.endswith("/CreateDataset"):
                return {
                    "dataset": {
                        "datasetId": body["datasetId"],
                        "description": body.get("description", ""),
                        "createTime": "2026-08-29T00:00:00Z",
                    }
                }
            if path.endswith("/GetDataset"):
                return {"dataset": {"datasetId": body["datasetId"]}}
            if body.get("pageToken") == "p2":
                return {"datasets": [{"datasetId": "b"}]}
            return {
                "datasets": [{"datasetId": "a"}],
                "nextPageToken": "p2",
            }

        with IngestServer(responder) as server:
            client = _client(server)
            created = client.datasets.create("events", description="app events")
            self.assertEqual(created.dataset_id, "events")
            self.assertEqual(created.description, "app events")
            self.assertEqual(created.create_time, "2026-08-29T00:00:00Z")
            self.assertRegex(
                server.requests[0]["headers"]["Idempotency-Key"], UUID_TEXT
            )

            found = client.datasets.get("events")
            self.assertEqual(found.dataset_id, "events")

            names = [item.dataset_id for item in client.datasets.iterate()]
            self.assertEqual(names, ["a", "b"])

    def test_dataset_acknowledgement_requires_an_identifier(self) -> None:
        def responder(_path: str, _body: dict[str, Any]) -> dict[str, Any]:
            return {"dataset": {}}

        with IngestServer(responder) as server:
            client = _client(server)
            with self.assertRaises(MedallionError) as raised:
                client.datasets.get("events")
            self.assertEqual(raised.exception.code, "MEDALLION_INVALID_INGEST_RESPONSE")


if __name__ == "__main__":
    unittest.main()

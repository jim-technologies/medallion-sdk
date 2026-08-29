"""Tabular ingestion and query over medallion.ingest.v1.

``IngestClient`` is the thin protobuf client for the six ingest RPCs.
``DatasetsClient`` is the ergonomic, dataframe-friendly layer: it appends
plain dict rows, polars DataFrames, or pyarrow tables, and returns query
results that iterate across pages and collect into polars. SQL passes
through verbatim in the declared ClickHouse dialect; this is not an ORM or
a query builder, and it never runs in a browser.
"""

from __future__ import annotations

import io
import json
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from threading import Event
from typing import Any

from medallion.ingest.v1 import ingest_pb2

from .errors import MedallionError
from .request import _RequestClient

INGEST_SERVICE = "/medallion.ingest.v1.MedallionIngestService"
APPEND = f"{INGEST_SERVICE}/Append"
QUERY = f"{INGEST_SERVICE}/Query"
GET_QUERY_RESULTS = f"{INGEST_SERVICE}/GetQueryResults"
CREATE_DATASET = f"{INGEST_SERVICE}/CreateDataset"
GET_DATASET = f"{INGEST_SERVICE}/GetDataset"
LIST_DATASETS = f"{INGEST_SERVICE}/ListDatasets"

MAX_INGEST_BATCH_ROWS = 50_000
MAX_DATASET_ID_BYTES = 256
MAX_DATASET_DESCRIPTION_BYTES = 4_096
MAX_INSERT_ID_BYTES = 128
MAX_QUERY_BYTES = 1_048_576
MAX_QUERY_ID_BYTES = 1_024
MAX_INGEST_PAGE_TOKEN_BYTES = 2_048
MAX_QUERY_MAX_RESULTS = 100_000
MAX_INGEST_PAGE_SIZE = 500
MAX_QUERY_POLLS = 1_000
MAX_RESULT_PAGES = 10_000
# Stripe-convention bound for the Idempotency-Key header value.
MAX_IDEMPOTENCY_KEY_BYTES = 255

_FORMATS = {
    None: ingest_pb2.RESULT_FORMAT_UNSPECIFIED,
    "json": ingest_pb2.RESULT_FORMAT_JSON,
    "arrow": ingest_pb2.RESULT_FORMAT_ARROW_IPC,
}


@dataclass(frozen=True)
class DatasetRowError:
    """One rejected row of an append batch."""

    index: int
    reason: str | None = None
    message: str | None = None


@dataclass(frozen=True)
class DatasetAppendResult:
    """Acknowledgement of one append batch."""

    idempotency_key: str
    accepted_rows: int
    duplicate: bool
    row_errors: list[DatasetRowError] = field(default_factory=list)
    request_id: str | None = None
    proto: ingest_pb2.AppendResponse | None = None


@dataclass(frozen=True)
class DatasetColumn:
    """One result column with its declared ClickHouse type."""

    name: str
    type: str


@dataclass(frozen=True)
class Dataset:
    """One named tabular collection in the selected workspace."""

    dataset_id: str
    description: str | None = None
    create_time: str | None = None
    proto: ingest_pb2.Dataset | None = None


@dataclass(frozen=True)
class DatasetPage:
    """One page of datasets."""

    datasets: list[Dataset]
    next_page_token: str | None = None
    request_id: str | None = None


class IngestClient:
    """Thin protobuf client for the six medallion.ingest.v1 RPCs."""

    def __init__(self, requests: _RequestClient) -> None:
        self._requests = requests

    @property
    def workspace_id(self) -> str:
        return self._requests.workspace_id

    def append(
        self,
        request: ingest_pb2.AppendRequest,
        *,
        idempotency_key: str,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.AppendResponse, str | None]:
        _required_dataset_id(request.dataset_id, "append.dataset_id")
        payload = request.WhichOneof("rows")
        if payload is None:
            raise MedallionError(
                "An append batch requires exactly one of JSON rows or one Arrow record batch.",
                code="MEDALLION_AMBIGUOUS_ROW_PAYLOAD",
            )
        submitted: int | None = None
        if payload == "json_rows":
            submitted = len(request.json_rows.rows)
            if not 1 <= submitted <= MAX_INGEST_BATCH_ROWS:
                raise MedallionError(
                    f"An append batch must contain between 1 and {MAX_INGEST_BATCH_ROWS} rows.",
                    code="MEDALLION_INVALID_BATCH_SIZE",
                )
            for index, row in enumerate(request.json_rows.rows):
                _validate_wire_row(row, index)
        elif not request.arrow_rows.serialized_record_batch:
            raise MedallionError(
                "An Arrow append payload must not be empty.",
                code="MEDALLION_INVALID_ROW",
            )
        response = ingest_pb2.AppendResponse()
        envelope = self._requests._post_proto(
            APPEND,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
            idempotency_key=_valid_idempotency_key(idempotency_key),
        )
        _validate_append_response(response, submitted, envelope.request_id)
        return response, envelope.request_id

    def query(
        self,
        request: ingest_pb2.QueryRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.QueryResponse, str | None]:
        if not request.query.strip():
            raise MedallionError(
                "query requires one SQL statement.",
                code="MEDALLION_INVALID_QUERY",
            )
        if len(request.query.encode("utf-8")) > MAX_QUERY_BYTES:
            raise MedallionError(
                f"query must not exceed {MAX_QUERY_BYTES} bytes.",
                code="MEDALLION_INVALID_QUERY",
            )
        _validate_result_page_inputs(request.max_results)
        response = ingest_pb2.QueryResponse()
        envelope = self._requests._post_proto(
            QUERY,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        _validate_query_results(response.results, envelope.request_id)
        return response, envelope.request_id

    def get_query_results(
        self,
        request: ingest_pb2.GetQueryResultsRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.GetQueryResultsResponse, str | None]:
        if not request.query_id:
            raise MedallionError(
                "query results require the query_id returned by query().",
                code="MEDALLION_INVALID_QUERY",
            )
        _validate_token(request.page_token, "query.page_token")
        _validate_result_page_inputs(request.max_results)
        response = ingest_pb2.GetQueryResultsResponse()
        envelope = self._requests._post_proto(
            GET_QUERY_RESULTS,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        _validate_query_results(response.results, envelope.request_id)
        return response, envelope.request_id

    def create_dataset(
        self,
        request: ingest_pb2.CreateDatasetRequest,
        *,
        idempotency_key: str,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.CreateDatasetResponse, str | None]:
        _required_dataset_id(request.dataset_id, "datasets.create.dataset_id")
        if len(request.description.encode("utf-8")) > MAX_DATASET_DESCRIPTION_BYTES:
            raise MedallionError(
                f"datasets.create.description must not exceed {MAX_DATASET_DESCRIPTION_BYTES} bytes.",
                code="MEDALLION_INVALID_DATASET_ID",
            )
        response = ingest_pb2.CreateDatasetResponse()
        envelope = self._requests._post_proto(
            CREATE_DATASET,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
            idempotency_key=_valid_idempotency_key(idempotency_key),
        )
        _validate_dataset(response.dataset, envelope.request_id)
        return response, envelope.request_id

    def get_dataset(
        self,
        request: ingest_pb2.GetDatasetRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.GetDatasetResponse, str | None]:
        _required_dataset_id(request.dataset_id, "datasets.get.dataset_id")
        response = ingest_pb2.GetDatasetResponse()
        envelope = self._requests._post_proto(
            GET_DATASET,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        _validate_dataset(response.dataset, envelope.request_id)
        return response, envelope.request_id

    def list_datasets(
        self,
        request: ingest_pb2.ListDatasetsRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.ListDatasetsResponse, str | None]:
        if request.page_size and not 1 <= request.page_size <= MAX_INGEST_PAGE_SIZE:
            raise MedallionError(
                f"page_size must be an integer from 1 through {MAX_INGEST_PAGE_SIZE}.",
                code="MEDALLION_INVALID_PAGE_SIZE",
            )
        _validate_token(request.page_token, "datasets.list.page_token")
        response = ingest_pb2.ListDatasetsResponse()
        envelope = self._requests._post_proto(
            LIST_DATASETS,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        for dataset in response.datasets:
            _validate_dataset(dataset, envelope.request_id)
        _validate_token(response.next_page_token, "datasets.list.next_page_token")
        return response, envelope.request_id


class DatasetsClient:
    """Ergonomic datasets surface: append rows, run SQL, manage datasets."""

    def __init__(self, ingest: IngestClient) -> None:
        self._ingest = ingest

    def append(
        self,
        dataset_id: str,
        rows: Any,
        *,
        insert_ids: Sequence[str | None] | None = None,
        idempotency_key: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> DatasetAppendResult:
        """Append one batch of rows to a dataset.

        ``rows`` may be a sequence of plain dict rows, a ``polars.DataFrame``,
        a ``pyarrow.Table`` or ``pyarrow.RecordBatch``, or ``bytes`` holding a
        pre-encoded Arrow IPC stream. A Stripe-style Idempotency-Key header is
        generated automatically and returned so the exact batch can be
        replayed safely; per-row ``insert_ids`` pass through only with dict
        rows.
        """

        key = (
            str(uuid.uuid4())
            if idempotency_key is None
            else _valid_idempotency_key(idempotency_key)
        )
        request = ingest_pb2.AppendRequest(dataset_id=dataset_id)
        arrow_payload = _arrow_ipc_payload(rows)
        if arrow_payload is not None:
            if insert_ids is not None:
                raise MedallionError(
                    "insert_ids apply only to dict rows; Arrow payloads deduplicate by Idempotency-Key.",
                    code="MEDALLION_INVALID_ROW",
                )
            request.arrow_rows.serialized_record_batch = arrow_payload
        else:
            request.json_rows.rows.extend(_json_wire_rows(rows, insert_ids))
        response, request_id = self._ingest.append(
            request,
            idempotency_key=key,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return DatasetAppendResult(
            idempotency_key=key,
            accepted_rows=response.accepted_rows,
            duplicate=response.duplicate,
            row_errors=[
                DatasetRowError(
                    index=error.index,
                    reason=error.reason or None,
                    message=error.message or None,
                )
                for error in response.insert_errors
            ],
            request_id=request_id,
            proto=response,
        )

    def query(
        self,
        sql: str,
        *,
        server_timeout_ms: int | None = None,
        dry_run: bool = False,
        max_results: int | None = None,
        format: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> DatasetQueryResult:
        """Run one SQL statement in the declared ClickHouse dialect.

        The call is synchronous first; while the server reports the query as
        still running, the SDK polls transparently, then returns a result
        whose iteration crosses every page without exposing page tokens.
        """

        if format not in _FORMATS:
            raise MedallionError(
                "query format must be json or arrow.",
                code="MEDALLION_INVALID_QUERY",
            )
        request = ingest_pb2.QueryRequest(
            query=sql,
            timeout_ms=_server_timeout(server_timeout_ms),
            dry_run=dry_run,
            max_results=max_results or 0,
            format=_FORMATS[format],
        )
        response, request_id = self._ingest.query(
            request,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        results = response.results
        polls = 0
        while not results.completed:
            polls += 1
            if polls > MAX_QUERY_POLLS:
                raise MedallionError(
                    f"Medallion query polling exceeded {MAX_QUERY_POLLS} attempts without completing.",
                    code="MEDALLION_QUERY_POLL_LIMIT",
                    request_id=request_id,
                )
            poll, request_id = self._ingest.get_query_results(
                ingest_pb2.GetQueryResultsRequest(
                    query_id=results.query_id,
                    timeout_ms=_server_timeout(server_timeout_ms),
                    max_results=max_results or 0,
                    format=_FORMATS[format],
                ),
                timeout=timeout,
                cancellation_event=cancellation_event,
            )
            results = poll.results
        return DatasetQueryResult(
            self._ingest,
            results,
            request_id=request_id,
            server_timeout_ms=server_timeout_ms,
            max_results=max_results,
            format=format,
            dry_run=dry_run,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )

    def create(
        self,
        dataset_id: str,
        *,
        description: str | None = None,
        idempotency_key: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> Dataset:
        """Create one dataset; replay-safe through the Idempotency-Key header."""

        key = (
            str(uuid.uuid4())
            if idempotency_key is None
            else _valid_idempotency_key(idempotency_key)
        )
        response, request_id = self._ingest.create_dataset(
            ingest_pb2.CreateDatasetRequest(
                dataset_id=dataset_id,
                description=description or "",
            ),
            idempotency_key=key,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return _dataset_from_proto(response.dataset)

    def get(
        self,
        dataset_id: str,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> Dataset:
        """Read one dataset by identifier."""

        response, _request_id = self._ingest.get_dataset(
            ingest_pb2.GetDatasetRequest(dataset_id=dataset_id),
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return _dataset_from_proto(response.dataset)

    def list(
        self,
        *,
        page_size: int | None = None,
        page_token: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> DatasetPage:
        """List one page of datasets."""

        response, request_id = self._ingest.list_datasets(
            ingest_pb2.ListDatasetsRequest(
                page_size=page_size or 0,
                page_token=page_token or "",
            ),
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return DatasetPage(
            datasets=[_dataset_from_proto(dataset) for dataset in response.datasets],
            next_page_token=response.next_page_token or None,
            request_id=request_id,
        )

    def iterate(
        self,
        *,
        page_size: int | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> Iterator[Dataset]:
        """Iterate every dataset across pages without touching page tokens."""

        page_token: str | None = None
        seen: set[str] = set()
        pages = 0
        while True:
            pages += 1
            if pages > MAX_RESULT_PAGES:
                raise MedallionError(
                    f"Medallion iteration exceeded {MAX_RESULT_PAGES} pages.",
                    code="MEDALLION_PAGINATION_LIMIT",
                )
            page = self.list(
                page_size=page_size,
                page_token=page_token,
                timeout=timeout,
                cancellation_event=cancellation_event,
            )
            yield from page.datasets
            if not page.next_page_token:
                return
            if page.next_page_token in seen:
                raise _repeated_token()
            seen.add(page.next_page_token)
            page_token = page.next_page_token


class DatasetQueryResult:
    """A completed query.

    Iterating yields each row as a dict and paginates transparently;
    ``to_polars()`` collects every page into one ``polars.DataFrame``. Each
    result is single-consumption so one read never silently replays another.
    """

    def __init__(
        self,
        ingest: IngestClient,
        results: ingest_pb2.QueryResults,
        *,
        request_id: str | None,
        server_timeout_ms: int | None,
        max_results: int | None,
        format: str | None,
        dry_run: bool,
        timeout: float | None,
        cancellation_event: Event | None,
    ) -> None:
        self._ingest = ingest
        self._first_page: ingest_pb2.QueryResults | None = results
        self._server_timeout_ms = server_timeout_ms
        self._max_results = max_results
        self._format = format
        self._timeout = timeout
        self._cancellation_event = cancellation_event
        self.request_id = request_id
        self.query_id = results.query_id or None
        self.dry_run = dry_run
        self.columns = [
            DatasetColumn(name=column.name, type=column.type)
            for column in results.schema.columns
        ]
        self.total_rows = results.total_rows
        self.total_bytes_processed = results.total_bytes_processed

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self.rows()

    def rows(self) -> Iterator[dict[str, Any]]:
        """Yield each result row as a dict, across every page."""

        for page in self._pages():
            for index, text in enumerate(page.rows_json):
                yield _parse_row_object(text, index, self.request_id)

    def arrow_batches(self) -> Iterator[bytes]:
        """Yield each page's raw Arrow IPC stream (arrow format only)."""

        for page in self._pages():
            payload = page.arrow_rows.serialized_record_batch
            if payload:
                yield payload

    def to_polars(self) -> Any:
        """Collect every page into one ``polars.DataFrame``."""

        polars = _import_polars()
        if self._format == "arrow":
            pyarrow = _import_pyarrow()
            tables = [
                pyarrow.ipc.open_stream(payload).read_all()
                for payload in self.arrow_batches()
            ]
            if not tables:
                return polars.DataFrame()
            return polars.from_arrow(pyarrow.concat_tables(tables))
        collected = list(self.rows())
        if not collected:
            return polars.DataFrame()
        return polars.DataFrame(collected)

    def _pages(self) -> Iterator[ingest_pb2.QueryResults]:
        page = self._first_page
        if page is None:
            raise MedallionError(
                "This query result was already consumed; run the query again to re-read it.",
                code="MEDALLION_RESULT_CONSUMED",
            )
        self._first_page = None
        seen: set[str] = set()
        pages = 0
        while True:
            pages += 1
            if pages > MAX_RESULT_PAGES:
                raise MedallionError(
                    f"Medallion iteration exceeded {MAX_RESULT_PAGES} pages.",
                    code="MEDALLION_PAGINATION_LIMIT",
                )
            yield page
            token = page.next_page_token
            if not token:
                return
            if self.query_id is None:
                raise MedallionError(
                    "Medallion returned a continuation token without a query identifier.",
                    code="MEDALLION_INVALID_INGEST_RESPONSE",
                    request_id=self.request_id,
                )
            if token in seen:
                raise _repeated_token()
            seen.add(token)
            response, request_id = self._ingest.get_query_results(
                ingest_pb2.GetQueryResultsRequest(
                    query_id=self.query_id,
                    page_token=token,
                    timeout_ms=_server_timeout(self._server_timeout_ms),
                    max_results=self._max_results or 0,
                    format=_FORMATS[self._format],
                ),
                timeout=self._timeout,
                cancellation_event=self._cancellation_event,
            )
            page = response.results
            if not page.completed:
                raise MedallionError(
                    "Medallion reported a completed query as running during pagination.",
                    code="MEDALLION_INVALID_INGEST_RESPONSE",
                    request_id=request_id,
                )


def _json_wire_rows(
    rows: Any,
    insert_ids: Sequence[str | None] | None,
) -> list[ingest_pb2.Row]:
    if isinstance(rows, str | bytes | Mapping) or not isinstance(rows, Sequence):
        raise MedallionError(
            "append rows must be a sequence of dict rows, a polars DataFrame, "
            "a pyarrow table, or Arrow IPC bytes.",
            code="MEDALLION_INVALID_ROW",
        )
    if not 1 <= len(rows) <= MAX_INGEST_BATCH_ROWS:
        raise MedallionError(
            f"An append batch must contain between 1 and {MAX_INGEST_BATCH_ROWS} rows.",
            code="MEDALLION_INVALID_BATCH_SIZE",
        )
    if insert_ids is not None and len(insert_ids) != len(rows):
        raise MedallionError(
            "insert_ids must align one-to-one with the submitted rows.",
            code="MEDALLION_INVALID_ROW",
        )
    wire_rows: list[ingest_pb2.Row] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise MedallionError(
                f"append rows[{index}] must be a mapping of column values.",
                code="MEDALLION_INVALID_ROW",
            )
        try:
            text = json.dumps(
                dict(row),
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
        except TypeError, ValueError:
            raise MedallionError(
                f"append rows[{index}] must be JSON serializable with finite numbers.",
                code="MEDALLION_INVALID_ROW",
            ) from None
        wire = ingest_pb2.Row(json=text)
        insert_id = insert_ids[index] if insert_ids is not None else None
        if insert_id is not None:
            wire.insert_id = insert_id
        wire_rows.append(wire)
    return wire_rows


def _arrow_ipc_payload(rows: Any) -> bytes | None:
    """Return Arrow IPC bytes for arrow-shaped inputs, or None for dict rows."""

    if isinstance(rows, bytes | bytearray | memoryview):
        payload = bytes(rows)
        if not payload:
            raise MedallionError(
                "An Arrow append payload must not be empty.",
                code="MEDALLION_INVALID_ROW",
            )
        return payload
    module = type(rows).__module__
    if module.split(".", 1)[0] == "polars":
        table = rows.to_arrow()
        return _table_to_ipc(table)
    if module.split(".", 1)[0] == "pyarrow":
        pyarrow = _import_pyarrow()
        if isinstance(rows, pyarrow.RecordBatch):
            return _table_to_ipc(pyarrow.Table.from_batches([rows]))
        if isinstance(rows, pyarrow.Table):
            return _table_to_ipc(rows)
        raise MedallionError(
            "pyarrow append payloads must be a Table or RecordBatch.",
            code="MEDALLION_INVALID_ROW",
        )
    return None


def _table_to_ipc(table: Any) -> bytes:
    pyarrow = _import_pyarrow()
    sink = io.BytesIO()
    with pyarrow.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


def _import_polars() -> Any:
    try:
        import polars
    except ImportError:
        raise MedallionError(
            'polars is required for dataframe conveniences; install "medallion[polars]".',
            code="MEDALLION_POLARS_REQUIRED",
        ) from None
    return polars


def _import_pyarrow() -> Any:
    try:
        import pyarrow
        import pyarrow.ipc  # noqa: F401  (registers the ipc submodule)
    except ImportError:
        raise MedallionError(
            'pyarrow is required for Arrow payloads; install "medallion[polars]".',
            code="MEDALLION_POLARS_REQUIRED",
        ) from None
    return pyarrow


def _valid_idempotency_key(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > MAX_IDEMPOTENCY_KEY_BYTES
        or any(not (0x20 < ord(char) < 0x7F) for char in value)
    ):
        raise MedallionError(
            f"idempotency_key must be a printable ASCII string of at most {MAX_IDEMPOTENCY_KEY_BYTES} bytes.",
            code="MEDALLION_INVALID_IDEMPOTENCY_KEY",
        )
    return value


def _required_dataset_id(value: str, path: str) -> None:
    if not value.strip():
        raise MedallionError(
            f"{path} is required.",
            code="MEDALLION_INVALID_DATASET_ID",
        )
    if len(value.encode("utf-8")) > MAX_DATASET_ID_BYTES:
        raise MedallionError(
            f"{path} must not exceed {MAX_DATASET_ID_BYTES} bytes.",
            code="MEDALLION_INVALID_DATASET_ID",
        )


def _validate_wire_row(row: ingest_pb2.Row, index: int) -> None:
    if len(row.insert_id.encode("utf-8")) > MAX_INSERT_ID_BYTES:
        raise MedallionError(
            f"append rows[{index}].insert_id must not exceed {MAX_INSERT_ID_BYTES} bytes.",
            code="MEDALLION_INVALID_ROW",
        )
    try:
        parsed = json.loads(row.json)
    except json.JSONDecodeError, ValueError:
        parsed = None
    if not isinstance(parsed, dict):
        raise MedallionError(
            f"append rows[{index}].json must contain exactly one JSON object.",
            code="MEDALLION_INVALID_ROW",
        )


def _validate_result_page_inputs(max_results: int) -> None:
    if max_results and not 0 <= max_results <= MAX_QUERY_MAX_RESULTS:
        raise MedallionError(
            f"max_results must be an integer from 0 through {MAX_QUERY_MAX_RESULTS}.",
            code="MEDALLION_INVALID_PAGE_SIZE",
        )


def _validate_token(value: str, path: str) -> None:
    if value and len(value.encode("utf-8")) > MAX_INGEST_PAGE_TOKEN_BYTES:
        raise MedallionError(
            f"{path} must not exceed {MAX_INGEST_PAGE_TOKEN_BYTES} bytes.",
            code="MEDALLION_INVALID_PAGE_TOKEN",
        )


def _server_timeout(server_timeout_ms: int | None) -> int:
    if server_timeout_ms is None:
        return 0
    if (
        isinstance(server_timeout_ms, bool)
        or not isinstance(server_timeout_ms, int)
        or not 0 <= server_timeout_ms <= 2_147_483_647
    ):
        raise MedallionError(
            "server_timeout_ms must be an integer from 0 through 2147483647.",
            code="MEDALLION_INVALID_OPTIONS",
        )
    return server_timeout_ms


def _validate_append_response(
    response: ingest_pb2.AppendResponse,
    submitted: int | None,
    request_id: str | None,
) -> None:
    if submitted is not None:
        if response.accepted_rows > submitted:
            raise _invalid_ingest_response(
                "Medallion acknowledged more rows than this batch submitted.",
                request_id,
            )
        for error in response.insert_errors:
            if error.index >= submitted:
                raise _invalid_ingest_response(
                    "Medallion returned an append row error outside the submitted batch.",
                    request_id,
                )


def _validate_query_results(
    results: ingest_pb2.QueryResults,
    request_id: str | None,
) -> None:
    if len(results.query_id.encode("utf-8")) > MAX_QUERY_ID_BYTES:
        raise _invalid_ingest_response(
            "Medallion returned a malformed query identifier.",
            request_id,
        )
    if not results.completed and not results.query_id:
        raise _invalid_ingest_response(
            "Medallion returned an incomplete query without a query identifier to poll.",
            request_id,
        )
    if len(results.next_page_token.encode("utf-8")) > MAX_INGEST_PAGE_TOKEN_BYTES:
        raise _invalid_ingest_response(
            "Medallion returned a malformed query continuation token.",
            request_id,
        )
    if not results.completed and (results.next_page_token or results.rows_json):
        raise _invalid_ingest_response(
            "Medallion returned result rows for an incomplete query.",
            request_id,
        )
    for text in results.rows_json:
        if not text:
            raise _invalid_ingest_response(
                "Medallion returned malformed query result rows.",
                request_id,
            )


def _validate_dataset(
    dataset: ingest_pb2.Dataset,
    request_id: str | None,
) -> None:
    if (
        not dataset.dataset_id
        or len(dataset.dataset_id.encode("utf-8")) > MAX_DATASET_ID_BYTES
    ):
        raise _invalid_ingest_response(
            "Medallion returned a dataset without a valid identifier.",
            request_id,
        )


def _dataset_from_proto(dataset: ingest_pb2.Dataset) -> Dataset:
    create_time = (
        dataset.create_time.ToJsonString() if dataset.HasField("create_time") else None
    )
    return Dataset(
        dataset_id=dataset.dataset_id,
        description=dataset.description or None,
        create_time=create_time,
        proto=dataset,
    )


def _parse_row_object(
    text: str,
    index: int,
    request_id: str | None,
) -> dict[str, Any]:
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError, ValueError:
        parsed = None
    if not isinstance(parsed, dict):
        raise _invalid_ingest_response(
            f"Medallion returned a malformed JSON result row at index {index}.",
            request_id,
        )
    return parsed


def _repeated_token() -> MedallionError:
    return MedallionError(
        "Medallion returned a repeated non-empty continuation token; iteration stopped.",
        code="MEDALLION_REPEATED_CURSOR",
    )


def _invalid_ingest_response(
    message: str,
    request_id: str | None,
) -> MedallionError:
    return MedallionError(
        message,
        code="MEDALLION_INVALID_INGEST_RESPONSE",
        request_id=request_id,
    )

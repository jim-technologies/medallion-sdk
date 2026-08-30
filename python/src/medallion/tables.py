"""Tabular ingestion and query over medallion.ingest.v1.

``IngestClient`` is the thin protobuf client for the seven ingest RPCs.
``TablesClient`` is the ergonomic, dataframe-friendly layer: it declares
tables, evolves their schema additively, appends plain dict rows, polars
DataFrames, or pyarrow tables, and returns query results that iterate across
pages and collect into polars. SQL passes through verbatim in the declared
ClickHouse dialect; this is not an ORM or a query builder, and it never runs
in a browser.
"""

from __future__ import annotations

import io
import json
import re
import uuid
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from threading import Event
from typing import Any

from google.protobuf.json_format import MessageToDict

from medallion.ingest.v1 import ingest_pb2

from .errors import MedallionError
from .request import _RequestClient

INGEST_SERVICE = "/medallion.ingest.v1.MedallionIngestService"
CREATE_TABLE = f"{INGEST_SERVICE}/CreateTable"
GET_TABLE = f"{INGEST_SERVICE}/GetTable"
LIST_TABLES = f"{INGEST_SERVICE}/ListTables"
UPDATE_TABLE = f"{INGEST_SERVICE}/UpdateTable"
APPEND_ROWS = f"{INGEST_SERVICE}/AppendRows"
RUN_QUERY = f"{INGEST_SERVICE}/RunQuery"
GET_QUERY_RESULTS = f"{INGEST_SERVICE}/GetQueryResults"

MAX_INGEST_BATCH_ROWS = 50_000
MAX_INSERT_ID_BYTES = 128
MAX_REQUEST_ID_BYTES = 512
MAX_QUERY_BYTES = 262_144
MAX_QUERY_TIMEOUT_MS = 600_000
MAX_QUERY_PAGE_SIZE = 100_000
MAX_INGEST_PAGE_TOKEN_BYTES = 4_096
MAX_TABLE_PAGE_SIZE = 1_000
MAX_ARROW_PAYLOAD_BYTES = 16_777_216
MAX_TABLE_COLUMNS = 512
MAX_SORT_COLUMNS = 8
MAX_QUERY_POLLS = 1_000
MAX_RESULT_PAGES = 10_000
# Stripe-convention bound for the Idempotency-Key header value.
MAX_IDEMPOTENCY_KEY_BYTES = 255

TABLE_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,62}$", re.ASCII)
COLUMN_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$", re.ASCII)
_TABLE_NAME_PATTERN = re.compile(r"^tables/[a-z][a-z0-9_]{0,62}$", re.ASCII)
_QUERY_NAME_PATTERN = re.compile(r"^queries/[0-9a-hjkmnp-tv-z]{26}$", re.ASCII)

#: The BigQuery-style column types the tabular schema accepts.
COLUMN_TYPES = frozenset(
    {
        "BOOL",
        "INT64",
        "FLOAT64",
        "STRING",
        "BYTES",
        "TIMESTAMP",
        "DATE",
        "JSON",
    }
)
_QUERY_STATES = frozenset({"RUNNING", "SUCCEEDED", "FAILED"})


@dataclass(frozen=True)
class TableColumn:
    """One declared column of a table schema, or of a query result schema."""

    name: str
    type: str
    nullable: bool = False


@dataclass(frozen=True)
class TableRowError:
    """One rejected row of an append batch."""

    index: int
    code: int | None = None
    message: str | None = None


@dataclass(frozen=True)
class TableAppendResult:
    """Acknowledgement of one append batch."""

    idempotency_key: str
    accepted_rows: int
    row_errors: list[TableRowError] = field(default_factory=list)
    request_id: str | None = None
    proto: ingest_pb2.AppendRowsResponse | None = None


@dataclass(frozen=True)
class Table:
    """One declared table in the selected workspace."""

    table_id: str
    name: str
    columns: list[TableColumn] = field(default_factory=list)
    time_column: str = ""
    sort_columns: list[str] = field(default_factory=list)
    create_time: str | None = None
    proto: ingest_pb2.Table | None = None


@dataclass(frozen=True)
class TablePage:
    """One page of tables."""

    tables: list[Table]
    next_page_token: str | None = None
    request_id: str | None = None


class IngestClient:
    """Thin protobuf client for the seven medallion.ingest.v1 RPCs."""

    def __init__(self, requests: _RequestClient) -> None:
        self._requests = requests

    @property
    def workspace_id(self) -> str:
        return self._requests.workspace_id

    def create_table(
        self,
        request: ingest_pb2.CreateTableRequest,
        *,
        idempotency_key: str,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.CreateTableResponse, str | None]:
        if not TABLE_ID_PATTERN.match(request.table_id):
            raise _invalid_table_id(
                "tables.create.table_id must be a lowercase identifier of at "
                "most 63 characters."
            )
        _validate_table_spec(request.table, "tables.create")
        key = _stamp_request_id(request, idempotency_key)
        response = ingest_pb2.CreateTableResponse()
        envelope = self._requests._post_proto(
            CREATE_TABLE,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
            idempotency_key=key,
        )
        _validate_table(response.table, envelope.request_id)
        return response, envelope.request_id

    def update_table(
        self,
        request: ingest_pb2.UpdateTableRequest,
        *,
        idempotency_key: str,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.UpdateTableResponse, str | None]:
        _required_table_name(request.table.name, "tables.update.name")
        _validate_table_spec(request.table, "tables.update")
        key = _stamp_request_id(request, idempotency_key)
        response = ingest_pb2.UpdateTableResponse()
        envelope = self._requests._post_proto(
            UPDATE_TABLE,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
            idempotency_key=key,
        )
        _validate_table(response.table, envelope.request_id)
        return response, envelope.request_id

    def get_table(
        self,
        request: ingest_pb2.GetTableRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.GetTableResponse, str | None]:
        _required_table_name(request.name, "tables.get.name")
        response = ingest_pb2.GetTableResponse()
        envelope = self._requests._post_proto(
            GET_TABLE,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        _validate_table(response.table, envelope.request_id)
        return response, envelope.request_id

    def list_tables(
        self,
        request: ingest_pb2.ListTablesRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.ListTablesResponse, str | None]:
        if request.page_size and not 1 <= request.page_size <= MAX_TABLE_PAGE_SIZE:
            raise MedallionError(
                f"page_size must be an integer from 1 through {MAX_TABLE_PAGE_SIZE}.",
                code="MEDALLION_INVALID_PAGE_SIZE",
            )
        _validate_token(request.page_token, "tables.list.page_token")
        response = ingest_pb2.ListTablesResponse()
        envelope = self._requests._post_proto(
            LIST_TABLES,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        for table in response.tables:
            _validate_table(table, envelope.request_id)
        _validate_token(response.next_page_token, "tables.list.next_page_token")
        return response, envelope.request_id

    def append_rows(
        self,
        request: ingest_pb2.AppendRowsRequest,
        *,
        idempotency_key: str,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.AppendRowsResponse, str | None]:
        _required_table_name(request.table, "tables.append.table")
        has_json = len(request.rows) > 0
        has_arrow = request.HasField("arrow_rows")
        if has_json == has_arrow:
            raise MedallionError(
                "An append batch requires exactly one of JSON rows or one Arrow record batch.",
                code="MEDALLION_AMBIGUOUS_ROW_PAYLOAD",
            )
        submitted: int | None = None
        if has_json:
            submitted = len(request.rows)
            if submitted > MAX_INGEST_BATCH_ROWS:
                raise MedallionError(
                    f"An append batch must contain between 1 and {MAX_INGEST_BATCH_ROWS} rows.",
                    code="MEDALLION_INVALID_BATCH_SIZE",
                )
            for index, row in enumerate(request.rows):
                _validate_wire_row(row, index)
        else:
            _validate_arrow_payload(request.arrow_rows.serialized_record_batch)
        key = _stamp_request_id(request, idempotency_key)
        response = ingest_pb2.AppendRowsResponse()
        envelope = self._requests._post_proto(
            APPEND_ROWS,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
            idempotency_key=key,
        )
        _validate_append_response(response, submitted, envelope.request_id)
        return response, envelope.request_id

    def run_query(
        self,
        request: ingest_pb2.RunQueryRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.RunQueryResponse, str | None]:
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
        _validate_page_size(request.page_size)
        response = ingest_pb2.RunQueryResponse()
        envelope = self._requests._post_proto(
            RUN_QUERY,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        _validate_query_response(response, envelope.request_id)
        return response, envelope.request_id

    def get_query_results(
        self,
        request: ingest_pb2.GetQueryResultsRequest,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> tuple[ingest_pb2.GetQueryResultsResponse, str | None]:
        if not _QUERY_NAME_PATTERN.match(request.name):
            raise MedallionError(
                'query results require the "queries/{query}" name returned by run_query().',
                code="MEDALLION_INVALID_QUERY",
            )
        _validate_token(request.page_token, "query.page_token")
        _validate_page_size(request.page_size)
        response = ingest_pb2.GetQueryResultsResponse()
        envelope = self._requests._post_proto(
            GET_QUERY_RESULTS,
            request,
            response,
            timeout=timeout,
            cancellation_event=cancellation_event,
            retry_safe=True,
        )
        _validate_query_response(response, envelope.request_id)
        return response, envelope.request_id


class TablesClient:
    """Ergonomic tables surface: declare tables, append rows, run SQL."""

    def __init__(self, ingest: IngestClient) -> None:
        self._ingest = ingest

    def create(
        self,
        table_id: str,
        *,
        columns: Sequence[TableColumn | Mapping[str, Any]],
        time_column: str,
        sort_columns: Sequence[str] | None = None,
        idempotency_key: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> Table:
        """Declare one table: an ordered schema, a TIMESTAMP time column, and
        an optional sort key. Re-declaring an identical table returns the
        existing one, so retries are safe.
        """

        key = _batch_key(idempotency_key)
        request = ingest_pb2.CreateTableRequest(
            table_id=table_id,
            table=ingest_pb2.Table(
                schema=ingest_pb2.TableSchema(columns=_columns_to_wire(columns)),
                time_column=time_column,
                sort_columns=list(sort_columns or []),
            ),
        )
        response, _request_id = self._ingest.create_table(
            request,
            idempotency_key=key,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return _table_from_proto(response.table)

    def update(
        self,
        table_id: str,
        *,
        columns: Sequence[TableColumn | Mapping[str, Any]],
        idempotency_key: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> Table:
        """Evolve one table's schema.

        Pass the FULL desired schema: the existing columns repeated unchanged
        and in order, then the new nullable columns. Resending the current
        schema is a no-op success, so retries are safe.
        """

        key = _batch_key(idempotency_key)
        request = ingest_pb2.UpdateTableRequest(
            table=ingest_pb2.Table(
                name=_table_resource_name(table_id, "tables.update.table_id"),
                schema=ingest_pb2.TableSchema(columns=_columns_to_wire(columns)),
            ),
        )
        response, _request_id = self._ingest.update_table(
            request,
            idempotency_key=key,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return _table_from_proto(response.table)

    def get(
        self,
        table_id: str,
        *,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> Table:
        """Read one table by identifier."""

        response, _request_id = self._ingest.get_table(
            ingest_pb2.GetTableRequest(
                name=_table_resource_name(table_id, "tables.get.table_id"),
            ),
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return _table_from_proto(response.table)

    def list(
        self,
        *,
        page_size: int | None = None,
        page_token: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> TablePage:
        """List one page of tables."""

        response, request_id = self._ingest.list_tables(
            ingest_pb2.ListTablesRequest(
                page_size=page_size or 0,
                page_token=page_token or "",
            ),
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return TablePage(
            tables=[_table_from_proto(table) for table in response.tables],
            next_page_token=response.next_page_token or None,
            request_id=request_id,
        )

    def iterate(
        self,
        *,
        page_size: int | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> Iterator[Table]:
        """Iterate every table across pages without touching page tokens."""

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
            yield from page.tables
            if not page.next_page_token:
                return
            if page.next_page_token in seen:
                raise _repeated_token()
            seen.add(page.next_page_token)
            page_token = page.next_page_token

    def append(
        self,
        table_id: str,
        rows: Any,
        *,
        insert_ids: Sequence[str | None] | None = None,
        skip_invalid_rows: bool = False,
        idempotency_key: str | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> TableAppendResult:
        """Append one batch of rows to a table.

        ``rows`` may be a sequence of plain dict rows, a ``polars.DataFrame``,
        a ``pyarrow.Table`` or ``pyarrow.RecordBatch``, or ``bytes`` holding a
        pre-encoded Arrow IPC stream. A batch idempotency key is generated
        automatically, sent as the Idempotency-Key header and stamped into the
        request's ``request_id``, and returned so the exact batch can be
        replayed safely; per-row ``insert_ids`` correlate row errors and pass
        through only with dict rows.
        """

        key = _batch_key(idempotency_key)
        request = ingest_pb2.AppendRowsRequest(
            table=_table_resource_name(table_id, "tables.append.table_id"),
            skip_invalid_rows=skip_invalid_rows,
        )
        arrow_payload = _arrow_ipc_payload(rows)
        if arrow_payload is not None:
            if insert_ids is not None:
                raise MedallionError(
                    "insert_ids apply only to dict rows; Arrow payloads carry no row identifiers.",
                    code="MEDALLION_INVALID_ROW",
                )
            request.arrow_rows.serialized_record_batch = arrow_payload
        else:
            request.rows.extend(_json_wire_rows(rows, insert_ids))
        response, request_id = self._ingest.append_rows(
            request,
            idempotency_key=key,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        return TableAppendResult(
            idempotency_key=key,
            accepted_rows=response.accepted_rows,
            row_errors=[
                TableRowError(
                    index=error.index,
                    code=error.error.code or None,
                    message=error.error.message or None,
                )
                for error in response.row_errors
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
        page_size: int | None = None,
        timeout: float | None = None,
        cancellation_event: Event | None = None,
    ) -> TableQueryResult:
        """Run one SQL statement in the declared ClickHouse dialect.

        The call is synchronous first; while the server reports the query as
        still running, the SDK polls transparently, then returns a result
        whose iteration crosses every page without exposing page tokens.
        """

        request = ingest_pb2.RunQueryRequest(
            query=sql,
            timeout_ms=_server_timeout(server_timeout_ms),
            dry_run=dry_run,
            page_size=page_size or 0,
        )
        response, request_id = self._ingest.run_query(
            request,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )
        page: Any = response
        polls = 0
        while page.state == "RUNNING":
            polls += 1
            if polls > MAX_QUERY_POLLS:
                raise MedallionError(
                    f"Medallion query polling exceeded {MAX_QUERY_POLLS} attempts without completing.",
                    code="MEDALLION_QUERY_POLL_LIMIT",
                    request_id=request_id,
                )
            page, request_id = self._ingest.get_query_results(
                ingest_pb2.GetQueryResultsRequest(
                    name=page.name,
                    page_size=page_size or 0,
                ),
                timeout=timeout,
                cancellation_event=cancellation_event,
            )
        _require_succeeded(page, request_id)
        return TableQueryResult(
            self._ingest,
            page,
            request_id=request_id,
            page_size=page_size,
            dry_run=dry_run,
            timeout=timeout,
            cancellation_event=cancellation_event,
        )


class TableQueryResult:
    """A succeeded query.

    Iterating yields each row as a dict and paginates transparently;
    ``to_polars()`` collects every page into one ``polars.DataFrame``. Each
    result is single-consumption so one read never silently replays another.
    """

    def __init__(
        self,
        ingest: IngestClient,
        page: Any,
        *,
        request_id: str | None,
        page_size: int | None,
        dry_run: bool,
        timeout: float | None,
        cancellation_event: Event | None,
    ) -> None:
        self._ingest = ingest
        self._first_page: Any | None = page
        self._page_size = page_size
        self._timeout = timeout
        self._cancellation_event = cancellation_event
        self.request_id = request_id
        self.query_name = page.name or None
        self.dry_run = dry_run
        self.columns = [
            TableColumn(name=column.name, type=column.type, nullable=column.nullable)
            for column in page.schema.columns
        ]
        self.total_rows = page.total_rows

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return self.rows()

    def rows(self) -> Iterator[dict[str, Any]]:
        """Yield each result row as a dict, across every page."""

        for page in self._pages():
            for row in page.rows:
                yield MessageToDict(row)

    def to_polars(self) -> Any:
        """Collect every page into one ``polars.DataFrame``."""

        polars = _import_polars()
        collected = list(self.rows())
        if not collected:
            return polars.DataFrame()
        return polars.DataFrame(collected)

    def _pages(self) -> Iterator[Any]:
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
            if self.query_name is None:
                raise _invalid_ingest_response(
                    "Medallion returned a continuation token without a query resource name.",
                    self.request_id,
                )
            if token in seen:
                raise _repeated_token()
            seen.add(token)
            page, request_id = self._ingest.get_query_results(
                ingest_pb2.GetQueryResultsRequest(
                    name=self.query_name,
                    page_token=token,
                    page_size=self._page_size or 0,
                ),
                timeout=self._timeout,
                cancellation_event=self._cancellation_event,
            )
            _require_succeeded(page, request_id)


def _batch_key(idempotency_key: str | None) -> str:
    if idempotency_key is None:
        return str(uuid.uuid4())
    return _valid_idempotency_key(idempotency_key)


def _stamp_request_id(request: Any, idempotency_key: str) -> str:
    """Resolve the batch key and copy it into an empty ``request_id``.

    ``request_id`` is the field the contract deduplicates on, so the key rides
    in the body as well as the Idempotency-Key header.
    """

    key = _valid_idempotency_key(idempotency_key)
    if len(request.request_id.encode("utf-8")) > MAX_REQUEST_ID_BYTES:
        raise MedallionError(
            f"request_id must not exceed {MAX_REQUEST_ID_BYTES} bytes.",
            code="MEDALLION_INVALID_IDEMPOTENCY_KEY",
        )
    if not request.request_id:
        request.request_id = key
    return key


def _table_resource_name(table_id: str, path: str) -> str:
    bare = table_id[len("tables/") :] if table_id.startswith("tables/") else table_id
    if not TABLE_ID_PATTERN.match(bare):
        raise _invalid_table_id(
            f"{path} must be a lowercase identifier of at most 63 characters."
        )
    return f"tables/{bare}"


def _required_table_name(name: str, path: str) -> None:
    if not _TABLE_NAME_PATTERN.match(name):
        raise _invalid_table_id(
            f'{path} must be a resource name of the form "tables/{{table}}".'
        )


def _columns_to_wire(
    columns: Sequence[TableColumn | Mapping[str, Any]],
) -> list[ingest_pb2.ColumnSchema]:
    if isinstance(columns, str | bytes | Mapping) or not isinstance(columns, Sequence):
        raise _invalid_schema("columns must be a sequence of column declarations.")
    wire: list[ingest_pb2.ColumnSchema] = []
    for column in columns:
        if isinstance(column, TableColumn):
            wire.append(
                ingest_pb2.ColumnSchema(
                    name=column.name,
                    type=column.type,
                    nullable=column.nullable,
                )
            )
            continue
        if not isinstance(column, Mapping):
            raise _invalid_schema(
                "each column must be a TableColumn or a mapping of name, type, nullable."
            )
        wire.append(
            ingest_pb2.ColumnSchema(
                name=str(column.get("name", "")),
                type=str(column.get("type", "")),
                nullable=bool(column.get("nullable", False)),
            )
        )
    return wire


def _validate_table_spec(table: ingest_pb2.Table, path: str) -> None:
    columns = table.schema.columns
    if not 1 <= len(columns) <= MAX_TABLE_COLUMNS:
        raise _invalid_schema(
            f"{path} requires between 1 and {MAX_TABLE_COLUMNS} declared columns."
        )
    names: set[str] = set()
    for index, column in enumerate(columns):
        if not COLUMN_NAME_PATTERN.match(column.name):
            raise _invalid_schema(
                f"{path}.columns[{index}].name must be a lowercase identifier."
            )
        if column.name in names:
            raise _invalid_schema(f'{path} declares the column "{column.name}" twice.')
        names.add(column.name)
        if column.type not in COLUMN_TYPES:
            raise _invalid_schema(
                f"{path}.columns[{index}].type must be one of {sorted(COLUMN_TYPES)}."
            )
    # update_table accepts an empty time column meaning "unchanged".
    if table.time_column and table.time_column not in names:
        raise _invalid_schema(
            f"{path}.time_column must name one of the declared columns."
        )
    if len(table.sort_columns) > MAX_SORT_COLUMNS:
        raise _invalid_schema(
            f"{path}.sort_columns accepts at most {MAX_SORT_COLUMNS} columns."
        )
    seen: set[str] = set()
    for column_name in table.sort_columns:
        if column_name not in names or column_name in seen:
            raise _invalid_schema(
                f"{path}.sort_columns must be distinct declared column names."
            )
        seen.add(column_name)


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
            values = json.loads(json.dumps(dict(row), allow_nan=False))
        except TypeError, ValueError:
            raise MedallionError(
                f"append rows[{index}] must be JSON serializable with finite numbers.",
                code="MEDALLION_INVALID_ROW",
            ) from None
        wire = ingest_pb2.Row()
        wire.json.update(values)
        insert_id = insert_ids[index] if insert_ids is not None else None
        if insert_id is not None:
            wire.insert_id = insert_id
        wire_rows.append(wire)
    return wire_rows


def _arrow_ipc_payload(rows: Any) -> bytes | None:
    """Return Arrow IPC bytes for arrow-shaped inputs, or None for dict rows."""

    if isinstance(rows, bytes | bytearray | memoryview):
        payload = bytes(rows)
        _validate_arrow_payload(payload)
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


def _validate_wire_row(row: ingest_pb2.Row, index: int) -> None:
    if len(row.insert_id.encode("utf-8")) > MAX_INSERT_ID_BYTES:
        raise MedallionError(
            f"append rows[{index}].insert_id must not exceed {MAX_INSERT_ID_BYTES} bytes.",
            code="MEDALLION_INVALID_ROW",
        )
    if not row.HasField("json"):
        raise MedallionError(
            f"append rows[{index}].json must contain one JSON object of columns.",
            code="MEDALLION_INVALID_ROW",
        )


def _validate_arrow_payload(payload: bytes) -> None:
    if not payload:
        raise MedallionError(
            "An Arrow append payload must not be empty.",
            code="MEDALLION_INVALID_ROW",
        )
    if len(payload) > MAX_ARROW_PAYLOAD_BYTES:
        raise MedallionError(
            f"An Arrow append payload must not exceed {MAX_ARROW_PAYLOAD_BYTES} bytes.",
            code="MEDALLION_INVALID_BATCH_SIZE",
        )


def _validate_page_size(page_size: int) -> None:
    if page_size and not 0 <= page_size <= MAX_QUERY_PAGE_SIZE:
        raise MedallionError(
            f"page_size must be an integer from 0 through {MAX_QUERY_PAGE_SIZE}.",
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
        or not 0 <= server_timeout_ms <= MAX_QUERY_TIMEOUT_MS
    ):
        raise MedallionError(
            f"server_timeout_ms must be an integer from 0 through {MAX_QUERY_TIMEOUT_MS}.",
            code="MEDALLION_INVALID_OPTIONS",
        )
    return server_timeout_ms


def _validate_append_response(
    response: ingest_pb2.AppendRowsResponse,
    submitted: int | None,
    request_id: str | None,
) -> None:
    if submitted is not None:
        if response.accepted_rows > submitted:
            raise _invalid_ingest_response(
                "Medallion acknowledged more rows than this batch submitted.",
                request_id,
            )
        for error in response.row_errors:
            if not 0 <= error.index < submitted:
                raise _invalid_ingest_response(
                    "Medallion returned an append row error outside the submitted batch.",
                    request_id,
                )


def _validate_query_response(response: Any, request_id: str | None) -> None:
    if response.state not in _QUERY_STATES:
        raise _invalid_ingest_response(
            "Medallion returned an unknown query state.",
            request_id,
        )
    if response.name and not _QUERY_NAME_PATTERN.match(response.name):
        raise _invalid_ingest_response(
            "Medallion returned a malformed query resource name.",
            request_id,
        )
    if response.state == "RUNNING" and not response.name:
        raise _invalid_ingest_response(
            "Medallion returned a running query without a resource name to poll.",
            request_id,
        )
    if response.state != "SUCCEEDED" and len(response.rows) > 0:
        raise _invalid_ingest_response(
            "Medallion returned result rows for a query that has not succeeded.",
            request_id,
        )
    _validate_token(response.next_page_token, "query.next_page_token")
    if response.next_page_token and (
        response.state != "SUCCEEDED" or not response.name
    ):
        raise _invalid_ingest_response(
            "Medallion returned a continuation token no page can be fetched with.",
            request_id,
        )


def _require_succeeded(response: Any, request_id: str | None) -> None:
    if response.state == "SUCCEEDED":
        return
    if response.state == "FAILED":
        raise MedallionError(
            response.error.message or "Medallion reported the query as failed.",
            code="MEDALLION_QUERY_FAILED",
            request_id=request_id,
        )
    raise _invalid_ingest_response(
        "Medallion reported a settled query as still running.",
        request_id,
    )


def _validate_table(table: ingest_pb2.Table, request_id: str | None) -> None:
    if not _TABLE_NAME_PATTERN.match(table.name):
        raise _invalid_ingest_response(
            "Medallion returned a table without a valid resource name.",
            request_id,
        )


def _table_from_proto(table: ingest_pb2.Table) -> Table:
    create_time = (
        table.create_time.ToJsonString() if table.HasField("create_time") else None
    )
    return Table(
        table_id=table.name[len("tables/") :],
        name=table.name,
        columns=[
            TableColumn(name=column.name, type=column.type, nullable=column.nullable)
            for column in table.schema.columns
        ],
        time_column=table.time_column,
        sort_columns=list(table.sort_columns),
        create_time=create_time,
        proto=table,
    )


def _repeated_token() -> MedallionError:
    return MedallionError(
        "Medallion returned a repeated non-empty continuation token; iteration stopped.",
        code="MEDALLION_REPEATED_CURSOR",
    )


def _invalid_table_id(message: str) -> MedallionError:
    return MedallionError(message, code="MEDALLION_INVALID_TABLE_ID")


def _invalid_schema(message: str) -> MedallionError:
    return MedallionError(message, code="MEDALLION_INVALID_SCHEMA")


def _invalid_ingest_response(
    message: str,
    request_id: str | None,
) -> MedallionError:
    return MedallionError(
        message,
        code="MEDALLION_INVALID_INGEST_RESPONSE",
        request_id=request_id,
    )

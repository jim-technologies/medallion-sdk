import { Buffer } from "node:buffer";

import { MedallionError } from "./errors.js";
import {
  ingestIdempotencyKey,
  invalidIngestResponse,
  MAX_ARROW_PAYLOAD_BYTES,
  MAX_INGEST_BATCH_ROWS,
  nonNegativeCount,
  type ProtocolIngestClient,
  tableResourceName,
} from "./ingest.js";
import { assertIteratorPageWithinLimit, repeatedCursor } from "./ingestion.js";
import { jsonString } from "./payload.js";
import type {
  IngestColumnSchema,
  IngestQueryResponse,
  IngestRow,
  IngestTable,
  IngestWriteOptions,
  RequestOptions,
  Table,
  TableAppendOptions,
  TableAppendResult,
  TableColumn,
  TableCreateInput,
  TableListOptions,
  TablePage,
  TableQueryOptions,
  TableRow,
  TableUpdateInput,
} from "./types.js";

/** Poll budget for one query() call before the SDK refuses to spin. */
export const MAX_QUERY_POLLS = 1_000;

/**
 * The ergonomic tables surface: declare tables, evolve their schema
 * additively, append rows, and run ClickHouse SQL queries. SQL passes through
 * verbatim; this is not an ORM or a query builder.
 */
export class TablesClient {
  readonly #ingest: ProtocolIngestClient;

  constructor(ingest: ProtocolIngestClient) {
    this.#ingest = ingest;
  }

  /**
   * Append one batch of rows to a table: plain JSON objects, or one
   * pre-encoded Arrow IPC stream. A batch idempotency key is generated
   * automatically and returned so the exact batch can be replayed safely;
   * per-row insert IDs pass through via options.insertIds and correlate row
   * errors only.
   */
  async append(
    tableId: string,
    rows: readonly TableRow[] | Uint8Array,
    options: TableAppendOptions = {},
  ): Promise<TableAppendResult> {
    const idempotencyKey = ingestIdempotencyKey(options.idempotencyKey);
    const requestOptions = {
      signal: options.signal,
      timeoutMs: options.timeoutMs,
      idempotencyKey,
    } satisfies IngestWriteOptions;
    const table = tableResourceName(tableId, "tables.append.tableId");
    const skipInvalidRows = options.skipInvalidRows === true ? true : undefined;
    const response =
      rows instanceof Uint8Array
        ? await this.#ingest.appendRows(
            {
              table,
              arrow_rows: { serialized_record_batch: arrowPayload(rows) },
              skip_invalid_rows: skipInvalidRows,
            },
            requestOptions,
          )
        : await this.#ingest.appendRows(
            {
              table,
              rows: jsonRowsToWire(rows, options.insertIds),
              skip_invalid_rows: skipInvalidRows,
            },
            requestOptions,
          );
    return {
      requestId: response.requestId,
      idempotencyKey,
      acceptedRows: nonNegativeCount(
        response.body.accepted_rows,
        "append.acceptedRows",
        response.requestId,
      ),
      rowErrors: (response.body.row_errors ?? []).map((error) => ({
        index: nonNegativeCount(
          error.index,
          "append.rowErrors",
          response.requestId,
        ),
        code: error.error?.code || undefined,
        message: error.error?.message || undefined,
      })),
    };
  }

  /**
   * Run one SQL statement in the declared ClickHouse dialect. The call is
   * synchronous first; while the server reports the query as still running,
   * the SDK polls transparently, then returns a result whose rows iterate
   * across every page without exposing page tokens.
   */
  async query(
    sql: string,
    options: TableQueryOptions = {},
  ): Promise<TableQueryResult> {
    const requestOptions: RequestOptions = {
      signal: options.signal,
      timeoutMs: options.timeoutMs,
    };
    const first = await this.#ingest.runQuery(
      {
        query: sql,
        timeout_ms: options.serverTimeoutMs,
        dry_run: options.dryRun ? true : undefined,
        page_size: options.pageSize,
      },
      requestOptions,
    );
    let requestId = first.requestId;
    let body = first.body;
    let polls = 0;
    while ((body.state ?? "") === "RUNNING") {
      polls += 1;
      if (polls > MAX_QUERY_POLLS) {
        throw new MedallionError(
          `Medallion query polling exceeded ${MAX_QUERY_POLLS} attempts without completing.`,
          { code: "MEDALLION_QUERY_POLL_LIMIT", requestId },
        );
      }
      const poll = await this.#ingest.getQueryResults(
        { name: body.name ?? "", page_size: options.pageSize },
        requestOptions,
      );
      requestId = poll.requestId;
      body = poll.body;
    }
    requireSucceeded(body, requestId);
    return new TableQueryResult(this.#ingest, body, {
      requestId,
      requestOptions,
      pageSize: options.pageSize,
      dryRun: options.dryRun === true,
    });
  }

  /** Declare one table; replay-safe through the batch idempotency key. */
  async create(
    input: TableCreateInput,
    options: IngestWriteOptions = {},
  ): Promise<Table> {
    const response = await this.#ingest.createTable(
      {
        table_id: input.tableId,
        table: {
          schema: { columns: columnsToWire(input.columns) },
          time_column: input.timeColumn,
          sort_columns:
            input.sortColumns === undefined
              ? undefined
              : [...input.sortColumns],
        },
      },
      options,
    );
    return tableFromWire(response.body.table, response.requestId);
  }

  /**
   * Evolve one table's schema. Pass the FULL desired schema: existing columns
   * repeated unchanged and in order, then the new nullable columns. Resending
   * the current schema is a no-op success, so retries are safe.
   */
  async update(
    input: TableUpdateInput,
    options: IngestWriteOptions = {},
  ): Promise<Table> {
    const response = await this.#ingest.updateTable(
      {
        table: {
          name: tableResourceName(input.tableId, "tables.update.tableId"),
          schema: { columns: columnsToWire(input.columns) },
        },
      },
      options,
    );
    return tableFromWire(response.body.table, response.requestId);
  }

  /** Read one table by identifier. */
  async get(tableId: string, options: RequestOptions = {}): Promise<Table> {
    const response = await this.#ingest.getTable(
      { name: tableResourceName(tableId, "tables.get.tableId") },
      options,
    );
    return tableFromWire(response.body.table, response.requestId);
  }

  /** List one page of tables. */
  async list(
    input: TableListOptions = {},
    options: RequestOptions = {},
  ): Promise<TablePage> {
    const response = await this.#ingest.listTables(
      { page_size: input.pageSize, page_token: input.pageToken },
      options,
    );
    return {
      requestId: response.requestId,
      nextPageToken: response.body.next_page_token || undefined,
      tables: (response.body.tables ?? []).map((table) =>
        tableFromWire(table, response.requestId),
      ),
    };
  }

  /** Iterate every table across pages without touching page tokens. */
  async *iterate(
    input: TableListOptions = {},
    options: RequestOptions = {},
  ): AsyncGenerator<Table, void, undefined> {
    let pageToken = input.pageToken;
    const seen = new Set<string>();
    if (pageToken !== undefined && pageToken.length > 0) seen.add(pageToken);
    let pages = 0;
    for (;;) {
      pages += 1;
      assertIteratorPageWithinLimit(pages);
      const page = await this.list(
        { pageSize: input.pageSize, pageToken },
        options,
      );
      for (const table of page.tables) yield table;
      const next = page.nextPageToken;
      if (next === undefined || next.length === 0) return;
      if (seen.has(next)) throw repeatedCursor();
      seen.add(next);
      pageToken = next;
    }
  }
}

interface QueryResultState {
  requestId: string | undefined;
  requestOptions: RequestOptions;
  pageSize: number | undefined;
  dryRun: boolean;
}

/**
 * A succeeded query. Row iteration paginates transparently; each result is
 * single-consumption so one iteration never silently replays another's pages.
 */
export class TableQueryResult {
  readonly requestId?: string;
  /** Query resource name, "queries/{query}"; absent for a dry run. */
  readonly queryName?: string;
  /** Result columns with their declared types. */
  readonly columns: TableColumn[];
  readonly totalRows: number;
  readonly dryRun: boolean;

  readonly #ingest: ProtocolIngestClient;
  readonly #state: QueryResultState;
  #firstPage: IngestQueryResponse | undefined;

  constructor(
    ingest: ProtocolIngestClient,
    body: IngestQueryResponse,
    state: QueryResultState,
  ) {
    this.#ingest = ingest;
    this.#state = state;
    this.#firstPage = body;
    this.requestId = state.requestId;
    this.queryName = body.name || undefined;
    this.dryRun = state.dryRun;
    this.columns = columnsFromWire(body.schema?.columns);
    this.totalRows = nonNegativeCount(
      body.total_rows,
      "query.totalRows",
      state.requestId,
    );
  }

  /** Iterate every result row as a JSON object, across all pages. */
  async *rows(): AsyncGenerator<Record<string, unknown>, void, undefined> {
    for await (const page of this.#pages()) {
      for (const row of page.rows ?? []) yield row;
    }
  }

  [Symbol.asyncIterator](): AsyncGenerator<
    Record<string, unknown>,
    void,
    undefined
  > {
    return this.rows();
  }

  async *#pages(): AsyncGenerator<IngestQueryResponse, void, undefined> {
    const first = this.#firstPage;
    if (first === undefined) {
      throw new MedallionError(
        "This query result was already consumed; run the query again to re-read it.",
        { code: "MEDALLION_RESULT_CONSUMED" },
      );
    }
    this.#firstPage = undefined;
    let page = first;
    const seen = new Set<string>();
    let pages = 0;
    for (;;) {
      pages += 1;
      assertIteratorPageWithinLimit(pages);
      yield page;
      const next = page.next_page_token ?? "";
      if (next.length === 0) return;
      if (this.queryName === undefined) {
        throw invalidIngestResponse(
          "Medallion returned a continuation token without a query resource name.",
          this.#state.requestId,
        );
      }
      if (seen.has(next)) throw repeatedCursor();
      seen.add(next);
      const response = await this.#ingest.getQueryResults(
        {
          name: this.queryName,
          page_token: next,
          page_size: this.#state.pageSize,
        },
        this.#state.requestOptions,
      );
      requireSucceeded(response.body, response.requestId);
      page = response.body;
    }
  }
}

function requireSucceeded(
  body: IngestQueryResponse,
  requestId: string | undefined,
): void {
  const state = body.state ?? "";
  if (state === "SUCCEEDED") return;
  if (state === "FAILED") {
    throw new MedallionError(
      body.error?.message || "Medallion reported the query as failed.",
      { code: "MEDALLION_QUERY_FAILED", requestId },
    );
  }
  throw invalidIngestResponse(
    "Medallion reported a settled query as still running.",
    requestId,
  );
}

function jsonRowsToWire(
  rows: readonly TableRow[],
  insertIds: readonly (string | undefined)[] | undefined,
): IngestRow[] {
  if (!Array.isArray(rows)) {
    throw new MedallionError(
      "tables.append rows must be an array of JSON objects or one Uint8Array Arrow IPC stream.",
      { code: "MEDALLION_INVALID_ROW" },
    );
  }
  if (rows.length < 1 || rows.length > MAX_INGEST_BATCH_ROWS) {
    throw new MedallionError(
      `An append batch must contain between 1 and ${MAX_INGEST_BATCH_ROWS} rows.`,
      { code: "MEDALLION_INVALID_BATCH_SIZE" },
    );
  }
  if (insertIds !== undefined && insertIds.length !== rows.length) {
    throw new MedallionError(
      "tables.append insertIds must align one-to-one with the submitted rows.",
      { code: "MEDALLION_INVALID_ROW" },
    );
  }
  return rows.map((row, index) => {
    if (row === null || typeof row !== "object" || Array.isArray(row)) {
      throw new MedallionError(
        `tables.append.rows[${index}] must be a plain JSON object of column values.`,
        { code: "MEDALLION_INVALID_ROW" },
      );
    }
    const wire: IngestRow = {
      json: JSON.parse(
        jsonString(row, `tables.append.rows[${index}]`),
      ) as Record<string, unknown>,
    };
    const insertId = insertIds?.[index];
    if (insertId !== undefined) wire.insert_id = insertId;
    return wire;
  });
}

function arrowPayload(rows: Uint8Array): string {
  if (rows.length === 0) {
    throw new MedallionError("tables.append Arrow payload must not be empty.", {
      code: "MEDALLION_INVALID_ROW",
    });
  }
  if (rows.length > MAX_ARROW_PAYLOAD_BYTES) {
    throw new MedallionError(
      `tables.append Arrow payload must not exceed ${MAX_ARROW_PAYLOAD_BYTES} bytes.`,
      { code: "MEDALLION_INVALID_BATCH_SIZE" },
    );
  }
  return Buffer.from(rows).toString("base64");
}

function columnsToWire(columns: readonly TableColumn[]): IngestColumnSchema[] {
  if (!Array.isArray(columns)) {
    throw new MedallionError("tables schema columns must be an array.", {
      code: "MEDALLION_INVALID_SCHEMA",
    });
  }
  return columns.map((column) => ({
    name: column.name,
    type: column.type,
    nullable: column.nullable === true ? true : undefined,
  }));
}

function columnsFromWire(
  columns: IngestColumnSchema[] | undefined,
): TableColumn[] {
  return (columns ?? []).map((column) => ({
    name: column.name ?? "",
    type: column.type ?? "",
    nullable: column.nullable === true,
  }));
}

function tableFromWire(
  table: IngestTable | undefined,
  requestId: string | undefined,
): Table {
  const name = table?.name ?? "";
  if (table === undefined || name.length === 0) {
    throw invalidIngestResponse(
      "Medallion returned a table without a resource name.",
      requestId,
    );
  }
  return {
    tableId: name.slice("tables/".length),
    name,
    columns: columnsFromWire(table.schema?.columns),
    timeColumn: table.time_column ?? "",
    sortColumns: [...(table.sort_columns ?? [])],
    createTime: table.create_time || undefined,
  };
}

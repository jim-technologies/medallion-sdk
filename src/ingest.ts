import { randomUUID } from "node:crypto";

import { MedallionError } from "./errors.js";
import { ingestDescriptorBytes } from "./ingest-descriptor.js";
import { InvariantProtocolRuntime } from "./protocol.js";
import type { RequestClient, ResponseEnvelope } from "./request.js";
import type {
  IngestAppendRowsRequest,
  IngestAppendRowsResponse,
  IngestCreateTableRequest,
  IngestGetQueryResultsRequest,
  IngestGetTableRequest,
  IngestListTablesRequest,
  IngestListTablesResponse,
  IngestQueryResponse,
  IngestRunQueryRequest,
  IngestTable,
  IngestTableResponse,
  IngestUpdateTableRequest,
  IngestWriteOptions,
  RequestOptions,
} from "./types.js";

const INGEST_SERVICE_NAME = "medallion.ingest.v1.MedallionIngestService";

export const MAX_INGEST_BATCH_ROWS = 50_000;
export const MAX_INSERT_ID_BYTES = 128;
export const MAX_REQUEST_ID_BYTES = 512;
export const MAX_QUERY_BYTES = 262_144;
export const MAX_QUERY_TIMEOUT_MS = 600_000;
export const MAX_QUERY_PAGE_SIZE = 100_000;
export const MAX_INGEST_PAGE_TOKEN_BYTES = 4_096;
export const MAX_TABLE_PAGE_SIZE = 1_000;
export const MAX_ARROW_PAYLOAD_BYTES = 16_777_216;
export const MAX_TABLE_COLUMNS = 512;
export const MAX_SORT_COLUMNS = 8;
/** Stripe-convention bound for the Idempotency-Key header value. */
export const MAX_IDEMPOTENCY_KEY_BYTES = 255;

/** Lowercase table identifier, as the contract declares it. */
export const TABLE_ID_PATTERN = /^[a-z][a-z0-9_]{0,62}$/;
/** Lowercase column identifier, as the contract declares it. */
export const COLUMN_NAME_PATTERN = /^[a-z][a-z0-9_]{0,63}$/;
const TABLE_NAME_PATTERN = /^tables\/[a-z][a-z0-9_]{0,62}$/;
const QUERY_NAME_PATTERN = /^queries\/[0-9a-hjkmnp-tv-z]{26}$/;

/** The BigQuery-style column types the tabular schema accepts. */
export const TABLE_COLUMN_TYPES = [
  "BOOL",
  "INT64",
  "FLOAT64",
  "STRING",
  "BYTES",
  "TIMESTAMP",
  "DATE",
  "JSON",
] as const;

const COLUMN_TYPES = new Set<string>(TABLE_COLUMN_TYPES);
const QUERY_STATES = new Set(["RUNNING", "SUCCEEDED", "FAILED"]);
const BASE64_TEXT = /^[A-Za-z0-9+/]*={0,2}$/;

let ingestRuntime: InvariantProtocolRuntime | undefined;

function invariantIngestRuntime(): InvariantProtocolRuntime {
  ingestRuntime ??= new InvariantProtocolRuntime({
    descriptor: ingestDescriptorBytes(),
    serviceName: INGEST_SERVICE_NAME,
    ignoreUnknownResponseFields: true,
  });
  return ingestRuntime;
}

/**
 * Low-level access to the seven medallion.ingest.v1 RPCs. Every write carries
 * a batch idempotency key: it is sent as a Stripe-style Idempotency-Key header
 * and stamped into the request's `request_id`, which is the field the contract
 * deduplicates on. One is generated when the caller does not pass its own.
 */
export class ProtocolIngestClient {
  readonly #requests: RequestClient;
  private readonly runtime: InvariantProtocolRuntime;

  constructor(requests: RequestClient) {
    this.#requests = requests;
    this.runtime = invariantIngestRuntime();
  }

  createTable(
    request: IngestCreateTableRequest,
    options: IngestWriteOptions = {},
  ): Promise<ResponseEnvelope<IngestTableResponse>> {
    requiredTableId(request.table_id, "tables.create.tableId");
    preflightTable(request.table, "tables.create");
    return this.#write<IngestTableResponse>(
      "CreateTable",
      request,
      options,
    ).then((response) => {
      validateTableResponse(response.body, response.requestId);
      return response;
    });
  }

  updateTable(
    request: IngestUpdateTableRequest,
    options: IngestWriteOptions = {},
  ): Promise<ResponseEnvelope<IngestTableResponse>> {
    requiredTableName(request.table?.name, "tables.update.name");
    preflightTable(request.table, "tables.update");
    return this.#write<IngestTableResponse>(
      "UpdateTable",
      request,
      options,
    ).then((response) => {
      validateTableResponse(response.body, response.requestId);
      return response;
    });
  }

  getTable(
    request: IngestGetTableRequest,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<IngestTableResponse>> {
    requiredTableName(request.name, "tables.get.name");
    return this.#rpc<IngestTableResponse>("GetTable", request, options).then(
      (response) => {
        validateTableResponse(response.body, response.requestId);
        return response;
      },
    );
  }

  listTables(
    request: IngestListTablesRequest = {},
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<IngestListTablesResponse>> {
    ingestPageSize(request.page_size);
    optionalBoundedText(
      request.page_token,
      "tables.list.pageToken",
      MAX_INGEST_PAGE_TOKEN_BYTES,
      "MEDALLION_INVALID_PAGE_TOKEN",
    );
    return this.#rpc<IngestListTablesResponse>(
      "ListTables",
      request,
      options,
    ).then((response) => {
      validateListTablesResponse(response.body, response.requestId);
      return response;
    });
  }

  appendRows(
    request: IngestAppendRowsRequest,
    options: IngestWriteOptions = {},
  ): Promise<ResponseEnvelope<IngestAppendRowsResponse>> {
    preflightAppendRows(request);
    const submittedRows = request.rows?.length;
    return this.#write<IngestAppendRowsResponse>(
      "AppendRows",
      request,
      options,
    ).then((response) => {
      validateAppendRowsResponse(
        response.body,
        submittedRows,
        response.requestId,
      );
      return response;
    });
  }

  runQuery(
    request: IngestRunQueryRequest,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<IngestQueryResponse>> {
    preflightRunQuery(request);
    return this.#rpc<IngestQueryResponse>("RunQuery", request, options).then(
      (response) => {
        validateQueryResponse(response.body, response.requestId);
        return response;
      },
    );
  }

  getQueryResults(
    request: IngestGetQueryResultsRequest,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<IngestQueryResponse>> {
    preflightGetQueryResults(request);
    return this.#rpc<IngestQueryResponse>(
      "GetQueryResults",
      request,
      options,
    ).then((response) => {
      validateQueryResponse(response.body, response.requestId);
      return response;
    });
  }

  #write<TResponse>(
    methodName: string,
    body: { request_id?: string },
    options: IngestWriteOptions,
  ): Promise<ResponseEnvelope<TResponse>> {
    const key = ingestIdempotencyKey(options.idempotencyKey);
    optionalBoundedText(
      body.request_id,
      `${methodName}.requestId`,
      MAX_REQUEST_ID_BYTES,
      "MEDALLION_INVALID_IDEMPOTENCY_KEY",
    );
    const stamped =
      (body.request_id ?? "").length > 0 ? body : { ...body, request_id: key };
    return this.#rpc<TResponse>(methodName, stamped, options, key);
  }

  async #rpc<TResponse>(
    methodName: string,
    body: unknown,
    options: RequestOptions,
    idempotencyKey?: string,
  ): Promise<ResponseEnvelope<TResponse>> {
    const method = this.runtime.method(methodName);
    const encodedBody = this.runtime.encodeInput(
      method,
      this.runtime.normalizeInput(method, body),
    );
    const response = await this.#requests.requestJson<unknown>({
      method: "POST",
      path: this.runtime.path(method),
      body: encodedBody,
      signal: options.signal,
      timeoutMs: options.timeoutMs,
      connectProtocol: true,
      retrySafe: true,
      idempotencyKey,
    });
    return {
      requestId: response.requestId,
      body: this.runtime.decodeOutput<TResponse>(
        method,
        response.body,
        response.requestId,
      ),
    };
  }
}

/** Validate a caller key or generate a fresh Stripe-style batch key. */
export function ingestIdempotencyKey(value: string | undefined): string {
  if (value === undefined) return randomUUID();
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    new TextEncoder().encode(value).length > MAX_IDEMPOTENCY_KEY_BYTES ||
    !/^[\x21-\x7e]+$/.test(value)
  ) {
    throw new MedallionError(
      `idempotencyKey must be a printable ASCII string of at most ${MAX_IDEMPOTENCY_KEY_BYTES} bytes.`,
      { code: "MEDALLION_INVALID_IDEMPOTENCY_KEY" },
    );
  }
  return value;
}

/** Validate a bare table identifier, as CreateTable declares it. */
export function requiredTableId(value: unknown, path: string): string {
  if (typeof value !== "string" || !TABLE_ID_PATTERN.test(value)) {
    throw new MedallionError(
      `${path} must be a lowercase identifier matching ${TABLE_ID_PATTERN.source}.`,
      { code: "MEDALLION_INVALID_TABLE_ID" },
    );
  }
  return value;
}

/** Validate a "tables/{table}" resource name. */
export function requiredTableName(value: unknown, path: string): string {
  if (typeof value !== "string" || !TABLE_NAME_PATTERN.test(value)) {
    throw new MedallionError(
      `${path} must be a resource name matching ${TABLE_NAME_PATTERN.source}.`,
      { code: "MEDALLION_INVALID_TABLE_ID" },
    );
  }
  return value;
}

/** Turn a bare table id, or an already-qualified name, into "tables/{id}". */
export function tableResourceName(tableId: string, path: string): string {
  const bare =
    typeof tableId === "string" && tableId.startsWith("tables/")
      ? tableId.slice("tables/".length)
      : tableId;
  return `tables/${requiredTableId(bare, path)}`;
}

export function ingestPageSize(value: number | undefined): void {
  if (value === undefined || value === 0) return;
  if (!Number.isInteger(value) || value < 1 || value > MAX_TABLE_PAGE_SIZE) {
    throw new MedallionError(
      `pageSize must be an integer from 1 through ${MAX_TABLE_PAGE_SIZE}.`,
      { code: "MEDALLION_INVALID_PAGE_SIZE" },
    );
  }
}

function preflightTable(table: IngestTable | undefined, path: string): void {
  if (table === undefined || table === null) {
    throw invalidSchema(`${path} requires a table declaration.`);
  }
  const columns = table.schema?.columns ?? [];
  if (
    !Array.isArray(columns) ||
    columns.length < 1 ||
    columns.length > MAX_TABLE_COLUMNS
  ) {
    throw invalidSchema(
      `${path} requires between 1 and ${MAX_TABLE_COLUMNS} declared columns.`,
    );
  }
  const names = new Set<string>();
  for (const [index, column] of columns.entries()) {
    const name = column.name ?? "";
    if (!COLUMN_NAME_PATTERN.test(name)) {
      throw invalidSchema(
        `${path}.columns[${index}].name must match ${COLUMN_NAME_PATTERN.source}.`,
      );
    }
    if (names.has(name)) {
      throw invalidSchema(`${path} declares the column "${name}" twice.`);
    }
    names.add(name);
    if (!COLUMN_TYPES.has(column.type ?? "")) {
      throw invalidSchema(
        `${path}.columns[${index}].type must be one of ${TABLE_COLUMN_TYPES.join(", ")}.`,
      );
    }
  }
  const timeColumn = table.time_column ?? "";
  // UpdateTable accepts an empty time column meaning "unchanged".
  if (timeColumn.length > 0 && !names.has(timeColumn)) {
    throw invalidSchema(
      `${path}.timeColumn must name one of the declared columns.`,
    );
  }
  const sortColumns = table.sort_columns ?? [];
  if (!Array.isArray(sortColumns) || sortColumns.length > MAX_SORT_COLUMNS) {
    throw invalidSchema(
      `${path}.sortColumns accepts at most ${MAX_SORT_COLUMNS} columns.`,
    );
  }
  const seen = new Set<string>();
  for (const column of sortColumns) {
    if (!names.has(column) || seen.has(column)) {
      throw invalidSchema(
        `${path}.sortColumns must be distinct declared column names.`,
      );
    }
    seen.add(column);
  }
}

function preflightAppendRows(request: IngestAppendRowsRequest): void {
  requiredTableName(request.table, "tables.append.table");
  const hasJsonRows = (request.rows?.length ?? 0) > 0;
  const hasArrowRows = request.arrow_rows !== undefined;
  if (hasJsonRows === hasArrowRows) {
    throw new MedallionError(
      "tables.append requires exactly one of JSON rows or one Arrow record batch.",
      { code: "MEDALLION_AMBIGUOUS_ROW_PAYLOAD" },
    );
  }
  if (hasJsonRows) {
    const rows = request.rows ?? [];
    if (rows.length > MAX_INGEST_BATCH_ROWS) {
      throw new MedallionError(
        `An append batch must contain between 1 and ${MAX_INGEST_BATCH_ROWS} rows.`,
        { code: "MEDALLION_INVALID_BATCH_SIZE" },
      );
    }
    for (const [index, row] of rows.entries()) {
      const path = `tables.append.rows[${index}]`;
      optionalBoundedText(
        row.insert_id,
        `${path}.insertId`,
        MAX_INSERT_ID_BYTES,
        "MEDALLION_INVALID_ROW",
      );
      requireJsonObject(row.json, `${path}.json`);
    }
    return;
  }
  requireArrowPayloadText(
    request.arrow_rows?.serialized_record_batch,
    "tables.append.arrowRows",
  );
}

function preflightRunQuery(request: IngestRunQueryRequest): void {
  if (typeof request.query !== "string" || request.query.trim().length === 0) {
    throw new MedallionError("query requires one SQL statement.", {
      code: "MEDALLION_INVALID_QUERY",
    });
  }
  if (new TextEncoder().encode(request.query).length > MAX_QUERY_BYTES) {
    throw new MedallionError(
      `query must not exceed ${MAX_QUERY_BYTES} bytes.`,
      { code: "MEDALLION_INVALID_QUERY" },
    );
  }
  if (request.timeout_ms !== undefined) {
    if (
      !Number.isInteger(request.timeout_ms) ||
      request.timeout_ms < 0 ||
      request.timeout_ms > MAX_QUERY_TIMEOUT_MS
    ) {
      throw new MedallionError(
        `query.serverTimeoutMs must be an integer from 0 through ${MAX_QUERY_TIMEOUT_MS}.`,
        { code: "MEDALLION_INVALID_TIMEOUT" },
      );
    }
  }
  queryPageSize(request.page_size, "query");
}

function preflightGetQueryResults(request: IngestGetQueryResultsRequest): void {
  if (
    typeof request.name !== "string" ||
    !QUERY_NAME_PATTERN.test(request.name)
  ) {
    throw new MedallionError(
      `query.results.name must be a resource name matching ${QUERY_NAME_PATTERN.source}.`,
      { code: "MEDALLION_INVALID_QUERY" },
    );
  }
  optionalBoundedText(
    request.page_token,
    "query.results.pageToken",
    MAX_INGEST_PAGE_TOKEN_BYTES,
    "MEDALLION_INVALID_PAGE_TOKEN",
  );
  queryPageSize(request.page_size, "query.results");
}

function queryPageSize(value: number | undefined, path: string): void {
  if (value === undefined) return;
  if (!Number.isInteger(value) || value < 0 || value > MAX_QUERY_PAGE_SIZE) {
    throw new MedallionError(
      `${path}.pageSize must be an integer from 0 through ${MAX_QUERY_PAGE_SIZE}.`,
      { code: "MEDALLION_INVALID_PAGE_SIZE" },
    );
  }
}

function requireJsonObject(value: unknown, path: string): void {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new MedallionError(`${path} must be one JSON object of columns.`, {
      code: "MEDALLION_INVALID_ROW",
    });
  }
}

function requireArrowPayloadText(value: unknown, path: string): void {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length % 4 !== 0 ||
    !BASE64_TEXT.test(value)
  ) {
    throw new MedallionError(
      `${path} must contain one base64 Arrow IPC stream.`,
      { code: "MEDALLION_INVALID_ROW" },
    );
  }
  if ((value.length / 4) * 3 > MAX_ARROW_PAYLOAD_BYTES) {
    throw new MedallionError(
      `${path} must not exceed ${MAX_ARROW_PAYLOAD_BYTES} bytes.`,
      { code: "MEDALLION_INVALID_BATCH_SIZE" },
    );
  }
}

function optionalBoundedText(
  value: unknown,
  path: string,
  maxBytes: number,
  code: string,
): void {
  if (value === undefined || value === "") return;
  if (
    typeof value !== "string" ||
    new TextEncoder().encode(value).length > maxBytes
  ) {
    throw new MedallionError(
      `${path} must be a string of at most ${maxBytes} bytes.`,
      { code },
    );
  }
}

export function nonNegativeCount(
  value: string | number | undefined,
  path: string,
  requestId: string | undefined,
): number {
  if (value === undefined) return 0;
  const parsed = typeof value === "string" ? Number(value) : value;
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw invalidIngestResponse(
      `Medallion returned a non-integer or unsafe ${path}.`,
      requestId,
    );
  }
  return parsed;
}

function validateAppendRowsResponse(
  body: IngestAppendRowsResponse,
  submittedRows: number | undefined,
  requestId: string | undefined,
): void {
  const accepted = nonNegativeCount(
    body.accepted_rows,
    "append.acceptedRows",
    requestId,
  );
  const errors = body.row_errors ?? [];
  if (!Array.isArray(errors)) {
    throw invalidIngestResponse(
      "Medallion returned malformed append row errors.",
      requestId,
    );
  }
  for (const error of errors) {
    const index = nonNegativeCount(error.index, "append.rowErrors", requestId);
    if (submittedRows !== undefined && index >= submittedRows) {
      throw invalidIngestResponse(
        "Medallion returned an append row error outside the submitted batch.",
        requestId,
      );
    }
  }
  if (submittedRows !== undefined && accepted > submittedRows) {
    throw invalidIngestResponse(
      "Medallion acknowledged more rows than this batch submitted.",
      requestId,
    );
  }
}

function validateQueryResponse(
  body: IngestQueryResponse,
  requestId: string | undefined,
): void {
  if (body === undefined || typeof body !== "object") {
    throw invalidIngestResponse(
      "Medallion returned a query acknowledgement without a body.",
      requestId,
    );
  }
  const state = body.state ?? "";
  if (!QUERY_STATES.has(state)) {
    throw invalidIngestResponse(
      "Medallion returned an unknown query state.",
      requestId,
    );
  }
  const name = body.name ?? "";
  if (name.length > 0 && !QUERY_NAME_PATTERN.test(name)) {
    throw invalidIngestResponse(
      "Medallion returned a malformed query resource name.",
      requestId,
    );
  }
  if (state === "RUNNING" && name.length === 0) {
    throw invalidIngestResponse(
      "Medallion returned a running query without a resource name to poll.",
      requestId,
    );
  }
  const rows = body.rows ?? [];
  if (
    !Array.isArray(rows) ||
    rows.some((row) => row === null || typeof row !== "object")
  ) {
    throw invalidIngestResponse(
      "Medallion returned malformed query result rows.",
      requestId,
    );
  }
  if (state !== "SUCCEEDED" && rows.length > 0) {
    throw invalidIngestResponse(
      "Medallion returned result rows for a query that has not succeeded.",
      requestId,
    );
  }
  const nextPageToken = body.next_page_token ?? "";
  if (
    typeof nextPageToken !== "string" ||
    new TextEncoder().encode(nextPageToken).length > MAX_INGEST_PAGE_TOKEN_BYTES
  ) {
    throw invalidIngestResponse(
      "Medallion returned a malformed query continuation token.",
      requestId,
    );
  }
  if (
    nextPageToken.length > 0 &&
    (state !== "SUCCEEDED" || name.length === 0)
  ) {
    throw invalidIngestResponse(
      "Medallion returned a continuation token no page can be fetched with.",
      requestId,
    );
  }
  validateSchemaColumns(body.schema?.columns, requestId);
  nonNegativeCount(body.total_rows, "query.totalRows", requestId);
}

function validateTableResponse(
  body: IngestTableResponse,
  requestId: string | undefined,
): void {
  validateTable(body.table, requestId);
}

function validateListTablesResponse(
  body: IngestListTablesResponse,
  requestId: string | undefined,
): void {
  const tables = body.tables ?? [];
  if (!Array.isArray(tables)) {
    throw invalidIngestResponse(
      "Medallion returned a malformed table list.",
      requestId,
    );
  }
  for (const table of tables) validateTable(table, requestId);
  if (
    body.next_page_token !== undefined &&
    (typeof body.next_page_token !== "string" ||
      new TextEncoder().encode(body.next_page_token).length >
        MAX_INGEST_PAGE_TOKEN_BYTES)
  ) {
    throw invalidIngestResponse(
      "Medallion returned a malformed table continuation token.",
      requestId,
    );
  }
}

function validateTable(
  table: IngestTable | undefined,
  requestId: string | undefined,
): void {
  if (
    table === undefined ||
    typeof table.name !== "string" ||
    !TABLE_NAME_PATTERN.test(table.name)
  ) {
    throw invalidIngestResponse(
      "Medallion returned a table without a valid resource name.",
      requestId,
    );
  }
  validateSchemaColumns(table.schema?.columns, requestId);
}

function validateSchemaColumns(
  columns: Array<{ name?: string; type?: string }> | undefined,
  requestId: string | undefined,
): void {
  if (columns === undefined) return;
  if (
    !Array.isArray(columns) ||
    columns.some(
      (column) =>
        typeof (column.name ?? "") !== "string" ||
        typeof (column.type ?? "") !== "string",
    )
  ) {
    throw invalidIngestResponse(
      "Medallion returned a malformed table schema.",
      requestId,
    );
  }
}

function invalidSchema(message: string): MedallionError {
  return new MedallionError(message, { code: "MEDALLION_INVALID_SCHEMA" });
}

export function invalidIngestResponse(
  message: string,
  requestId?: string,
): MedallionError {
  return new MedallionError(message, {
    code: "MEDALLION_INVALID_INGEST_RESPONSE",
    requestId,
  });
}

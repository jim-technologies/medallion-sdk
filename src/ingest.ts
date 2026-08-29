import { randomUUID } from "node:crypto";

import { MedallionError } from "./errors.js";
import { ingestDescriptorBytes } from "./ingest-descriptor.js";
import { InvariantProtocolRuntime } from "./protocol.js";
import type { RequestClient, ResponseEnvelope } from "./request.js";
import type {
  IngestAppendRequest,
  IngestAppendResponse,
  IngestCreateDatasetRequest,
  IngestDatasetResponse,
  IngestGetQueryResultsRequest,
  IngestListDatasetsRequest,
  IngestListDatasetsResponse,
  IngestQueryRequest,
  IngestQueryResponse,
  IngestQueryResults,
  IngestWriteOptions,
  RequestOptions,
} from "./types.js";

const INGEST_SERVICE_NAME = "medallion.ingest.v1.MedallionIngestService";

export const MAX_INGEST_BATCH_ROWS = 50_000;
export const MAX_DATASET_ID_BYTES = 256;
export const MAX_DATASET_DESCRIPTION_BYTES = 4_096;
export const MAX_INSERT_ID_BYTES = 128;
export const MAX_QUERY_BYTES = 1_048_576;
export const MAX_QUERY_ID_BYTES = 1_024;
export const MAX_INGEST_PAGE_TOKEN_BYTES = 2_048;
export const MAX_QUERY_MAX_RESULTS = 100_000;
export const MAX_INGEST_PAGE_SIZE = 500;
/** Stripe-convention bound for the Idempotency-Key header value. */
export const MAX_IDEMPOTENCY_KEY_BYTES = 255;

const RESULT_FORMATS = new Set([
  "RESULT_FORMAT_UNSPECIFIED",
  "RESULT_FORMAT_JSON",
  "RESULT_FORMAT_ARROW_IPC",
]);
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
 * Low-level access to the six medallion.ingest.v1 RPCs. Every append and
 * dataset creation carries a Stripe-style Idempotency-Key header; one is
 * generated when the caller does not pass its own.
 */
export class ProtocolIngestClient {
  readonly #requests: RequestClient;
  private readonly runtime: InvariantProtocolRuntime;

  constructor(requests: RequestClient) {
    this.#requests = requests;
    this.runtime = invariantIngestRuntime();
  }

  append(
    request: IngestAppendRequest,
    options: IngestWriteOptions = {},
  ): Promise<ResponseEnvelope<IngestAppendResponse>> {
    preflightAppend(request);
    const submittedRows = request.json_rows?.rows.length;
    return this.#rpc<IngestAppendResponse>(
      "Append",
      request,
      options,
      ingestIdempotencyKey(options.idempotencyKey),
    ).then((response) => {
      validateAppendResponse(response.body, submittedRows, response.requestId);
      return response;
    });
  }

  query(
    request: IngestQueryRequest,
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<IngestQueryResponse>> {
    preflightQuery(request);
    return this.#rpc<IngestQueryResponse>("Query", request, options).then(
      (response) => {
        validateQueryResults(response.body.results, response.requestId);
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
      validateQueryResults(response.body.results, response.requestId);
      return response;
    });
  }

  createDataset(
    request: IngestCreateDatasetRequest,
    options: IngestWriteOptions = {},
  ): Promise<ResponseEnvelope<IngestDatasetResponse>> {
    requiredDatasetId(request.dataset_id, "datasets.create.datasetId");
    optionalBoundedText(
      request.description,
      "datasets.create.description",
      MAX_DATASET_DESCRIPTION_BYTES,
      "MEDALLION_INVALID_DATASET_ID",
    );
    return this.#rpc<IngestDatasetResponse>(
      "CreateDataset",
      request,
      options,
      ingestIdempotencyKey(options.idempotencyKey),
    ).then((response) => {
      validateDatasetResponse(response.body, response.requestId);
      return response;
    });
  }

  getDataset(
    request: { dataset_id: string },
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<IngestDatasetResponse>> {
    requiredDatasetId(request.dataset_id, "datasets.get.datasetId");
    return this.#rpc<IngestDatasetResponse>(
      "GetDataset",
      request,
      options,
    ).then((response) => {
      validateDatasetResponse(response.body, response.requestId);
      return response;
    });
  }

  listDatasets(
    request: IngestListDatasetsRequest = {},
    options: RequestOptions = {},
  ): Promise<ResponseEnvelope<IngestListDatasetsResponse>> {
    ingestPageSize(request.page_size);
    optionalBoundedText(
      request.page_token,
      "datasets.list.pageToken",
      MAX_INGEST_PAGE_TOKEN_BYTES,
      "MEDALLION_INVALID_PAGE_TOKEN",
    );
    return this.#rpc<IngestListDatasetsResponse>(
      "ListDatasets",
      request,
      options,
    ).then((response) => {
      validateListDatasetsResponse(response.body, response.requestId);
      return response;
    });
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

export function requiredDatasetId(value: unknown, path: string): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new MedallionError(`${path} is required.`, {
      code: "MEDALLION_INVALID_DATASET_ID",
    });
  }
  if (new TextEncoder().encode(value).length > MAX_DATASET_ID_BYTES) {
    throw new MedallionError(
      `${path} must not exceed ${MAX_DATASET_ID_BYTES} bytes.`,
      { code: "MEDALLION_INVALID_DATASET_ID" },
    );
  }
  return value;
}

export function ingestPageSize(value: number | undefined): void {
  if (value === undefined || value === 0) return;
  if (!Number.isInteger(value) || value < 1 || value > MAX_INGEST_PAGE_SIZE) {
    throw new MedallionError(
      `pageSize must be an integer from 1 through ${MAX_INGEST_PAGE_SIZE}.`,
      { code: "MEDALLION_INVALID_PAGE_SIZE" },
    );
  }
}

function preflightAppend(request: IngestAppendRequest): void {
  requiredDatasetId(request.dataset_id, "datasets.append.datasetId");
  const hasJsonRows = request.json_rows !== undefined;
  const hasArrowRows = request.arrow_rows !== undefined;
  if (hasJsonRows === hasArrowRows) {
    throw new MedallionError(
      "datasets.append requires exactly one of JSON rows or one Arrow record batch.",
      { code: "MEDALLION_AMBIGUOUS_ROW_PAYLOAD" },
    );
  }
  if (hasJsonRows) {
    const rows = request.json_rows?.rows;
    if (
      !Array.isArray(rows) ||
      rows.length < 1 ||
      rows.length > MAX_INGEST_BATCH_ROWS
    ) {
      throw new MedallionError(
        `An append batch must contain between 1 and ${MAX_INGEST_BATCH_ROWS} rows.`,
        { code: "MEDALLION_INVALID_BATCH_SIZE" },
      );
    }
    for (const [index, row] of rows.entries()) {
      const path = `datasets.append.rows[${index}]`;
      optionalBoundedText(
        row.insert_id,
        `${path}.insertId`,
        MAX_INSERT_ID_BYTES,
        "MEDALLION_INVALID_ROW",
      );
      requireJsonObjectText(row.json, `${path}.json`);
    }
    return;
  }
  requireArrowPayloadText(
    request.arrow_rows?.serialized_record_batch,
    "datasets.append.arrowRows",
  );
}

function preflightQuery(request: IngestQueryRequest): void {
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
  preflightResultPageInputs(request, "query");
}

function preflightGetQueryResults(request: IngestGetQueryResultsRequest): void {
  optionalBoundedText(
    request.query_id,
    "query.results.queryId",
    MAX_QUERY_ID_BYTES,
    "MEDALLION_INVALID_QUERY",
  );
  if (typeof request.query_id !== "string" || request.query_id.length === 0) {
    throw new MedallionError("query.results.queryId is required.", {
      code: "MEDALLION_INVALID_QUERY",
    });
  }
  optionalBoundedText(
    request.page_token,
    "query.results.pageToken",
    MAX_INGEST_PAGE_TOKEN_BYTES,
    "MEDALLION_INVALID_PAGE_TOKEN",
  );
  preflightResultPageInputs(request, "query.results");
}

function preflightResultPageInputs(
  request: Pick<IngestQueryRequest, "timeout_ms" | "max_results" | "format">,
  path: string,
): void {
  if (request.timeout_ms !== undefined) {
    if (
      !Number.isInteger(request.timeout_ms) ||
      request.timeout_ms < 0 ||
      request.timeout_ms > 2_147_483_647
    ) {
      throw new MedallionError(
        `${path}.serverTimeoutMs must be an integer from 0 through 2147483647.`,
        { code: "MEDALLION_INVALID_TIMEOUT" },
      );
    }
  }
  if (request.max_results !== undefined) {
    if (
      !Number.isInteger(request.max_results) ||
      request.max_results < 0 ||
      request.max_results > MAX_QUERY_MAX_RESULTS
    ) {
      throw new MedallionError(
        `${path}.maxResults must be an integer from 0 through ${MAX_QUERY_MAX_RESULTS}.`,
        { code: "MEDALLION_INVALID_PAGE_SIZE" },
      );
    }
  }
  if (request.format !== undefined && !RESULT_FORMATS.has(request.format)) {
    throw new MedallionError(`${path}.format is invalid.`, {
      code: "MEDALLION_INVALID_QUERY",
    });
  }
}

function requireJsonObjectText(value: unknown, path: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new MedallionError(`${path} must contain exactly one JSON object.`, {
      code: "MEDALLION_INVALID_ROW",
    });
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new MedallionError(`${path} must contain exactly one JSON object.`, {
      code: "MEDALLION_INVALID_ROW",
    });
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new MedallionError(`${path} must contain exactly one JSON object.`, {
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

function validateAppendResponse(
  body: IngestAppendResponse,
  submittedRows: number | undefined,
  requestId: string | undefined,
): void {
  const accepted = nonNegativeCount(
    body.accepted_rows,
    "append.acceptedRows",
    requestId,
  );
  const errors = body.insert_errors ?? [];
  if (!Array.isArray(errors)) {
    throw invalidIngestResponse(
      "Medallion returned malformed append row errors.",
      requestId,
    );
  }
  for (const error of errors) {
    const index = error.index ?? 0;
    if (!Number.isSafeInteger(index) || index < 0) {
      throw invalidIngestResponse(
        "Medallion returned an append row error without a valid row index.",
        requestId,
      );
    }
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

function validateQueryResults(
  results: IngestQueryResults | undefined,
  requestId: string | undefined,
): void {
  if (results === undefined || typeof results !== "object") {
    throw invalidIngestResponse(
      "Medallion returned a query acknowledgement without results.",
      requestId,
    );
  }
  const completed = results.completed ?? false;
  if (typeof completed !== "boolean") {
    throw invalidIngestResponse(
      "Medallion returned a malformed query completion state.",
      requestId,
    );
  }
  if (
    results.query_id !== undefined &&
    (typeof results.query_id !== "string" ||
      new TextEncoder().encode(results.query_id).length > MAX_QUERY_ID_BYTES)
  ) {
    throw invalidIngestResponse(
      "Medallion returned a malformed query identifier.",
      requestId,
    );
  }
  if (!completed && (results.query_id ?? "").length === 0) {
    throw invalidIngestResponse(
      "Medallion returned an incomplete query without a query identifier to poll.",
      requestId,
    );
  }
  if (
    results.next_page_token !== undefined &&
    (typeof results.next_page_token !== "string" ||
      new TextEncoder().encode(results.next_page_token).length >
        MAX_INGEST_PAGE_TOKEN_BYTES)
  ) {
    throw invalidIngestResponse(
      "Medallion returned a malformed query continuation token.",
      requestId,
    );
  }
  if ((results.next_page_token ?? "") !== "" && !completed) {
    throw invalidIngestResponse(
      "Medallion returned a continuation token for an incomplete query.",
      requestId,
    );
  }
  const rows = results.rows_json ?? [];
  if (
    !Array.isArray(rows) ||
    rows.some((row) => typeof row !== "string" || row.length === 0)
  ) {
    throw invalidIngestResponse(
      "Medallion returned malformed query result rows.",
      requestId,
    );
  }
  if (rows.length > 0 && !completed) {
    throw invalidIngestResponse(
      "Medallion returned result rows for an incomplete query.",
      requestId,
    );
  }
  const columns = results.schema?.columns ?? [];
  if (
    !Array.isArray(columns) ||
    columns.some(
      (column) =>
        typeof (column.name ?? "") !== "string" ||
        typeof (column.type ?? "") !== "string",
    )
  ) {
    throw invalidIngestResponse(
      "Medallion returned a malformed query result schema.",
      requestId,
    );
  }
  nonNegativeCount(results.total_rows, "query.totalRows", requestId);
  nonNegativeCount(
    results.total_bytes_processed,
    "query.totalBytesProcessed",
    requestId,
  );
  const arrow = results.arrow_rows?.serialized_record_batch;
  if (
    arrow !== undefined &&
    (typeof arrow !== "string" ||
      arrow.length % 4 !== 0 ||
      !BASE64_TEXT.test(arrow))
  ) {
    throw invalidIngestResponse(
      "Medallion returned a malformed Arrow result payload.",
      requestId,
    );
  }
}

function validateDatasetResponse(
  body: IngestDatasetResponse,
  requestId: string | undefined,
): void {
  validateDataset(body.dataset, requestId);
}

function validateListDatasetsResponse(
  body: IngestListDatasetsResponse,
  requestId: string | undefined,
): void {
  const datasets = body.datasets ?? [];
  if (!Array.isArray(datasets)) {
    throw invalidIngestResponse(
      "Medallion returned a malformed dataset list.",
      requestId,
    );
  }
  for (const dataset of datasets) validateDataset(dataset, requestId);
  if (
    body.next_page_token !== undefined &&
    (typeof body.next_page_token !== "string" ||
      new TextEncoder().encode(body.next_page_token).length >
        MAX_INGEST_PAGE_TOKEN_BYTES)
  ) {
    throw invalidIngestResponse(
      "Medallion returned a malformed dataset continuation token.",
      requestId,
    );
  }
}

function validateDataset(
  dataset: { dataset_id?: string } | undefined,
  requestId: string | undefined,
): void {
  if (
    dataset === undefined ||
    typeof dataset.dataset_id !== "string" ||
    dataset.dataset_id.length === 0 ||
    new TextEncoder().encode(dataset.dataset_id).length > MAX_DATASET_ID_BYTES
  ) {
    throw invalidIngestResponse(
      "Medallion returned a dataset without a valid identifier.",
      requestId,
    );
  }
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

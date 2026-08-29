import { Buffer } from "node:buffer";

import { MedallionError } from "./errors.js";
import {
  ingestIdempotencyKey,
  invalidIngestResponse,
  nonNegativeCount,
  type ProtocolIngestClient,
} from "./ingest.js";
import { assertIteratorPageWithinLimit, repeatedCursor } from "./ingestion.js";
import { jsonString } from "./payload.js";
import type {
  Dataset,
  DatasetAppendOptions,
  DatasetAppendResult,
  DatasetColumn,
  DatasetCreateInput,
  DatasetListOptions,
  DatasetPage,
  DatasetQueryOptions,
  DatasetRow,
  IngestDataset,
  IngestQueryResults,
  IngestResultFormat,
  IngestRow,
  IngestWriteOptions,
  RequestOptions,
} from "./types.js";

/** Poll budget for one query() call before the SDK refuses to spin. */
export const MAX_QUERY_POLLS = 1_000;

/**
 * The ergonomic datasets surface: append rows, run ClickHouse SQL queries,
 * and manage datasets. SQL passes through verbatim; this is not an ORM or a
 * query builder.
 */
export class DatasetsClient {
  readonly #ingest: ProtocolIngestClient;

  constructor(ingest: ProtocolIngestClient) {
    this.#ingest = ingest;
  }

  /**
   * Append one batch of rows to a dataset: plain JSON objects, or one
   * pre-encoded Arrow IPC stream. A Stripe-style Idempotency-Key header is
   * generated automatically and returned so the exact batch can be replayed
   * safely; per-row insert IDs pass through via options.insertIds.
   */
  async append(
    datasetId: string,
    rows: readonly DatasetRow[] | Uint8Array,
    options: DatasetAppendOptions = {},
  ): Promise<DatasetAppendResult> {
    const idempotencyKey = ingestIdempotencyKey(options.idempotencyKey);
    const requestOptions = {
      signal: options.signal,
      timeoutMs: options.timeoutMs,
      idempotencyKey,
    } satisfies IngestWriteOptions;
    const response =
      rows instanceof Uint8Array
        ? await this.#ingest.append(
            {
              dataset_id: datasetId,
              arrow_rows: { serialized_record_batch: arrowPayload(rows) },
            },
            requestOptions,
          )
        : await this.#ingest.append(
            {
              dataset_id: datasetId,
              json_rows: { rows: jsonRowsToWire(rows, options.insertIds) },
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
      duplicate: response.body.duplicate ?? false,
      rowErrors: (response.body.insert_errors ?? []).map((error) => ({
        index: error.index ?? 0,
        reason: error.reason || undefined,
        message: error.message || undefined,
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
    options: DatasetQueryOptions = {},
  ): Promise<DatasetQueryResult> {
    const requestOptions: RequestOptions = {
      signal: options.signal,
      timeoutMs: options.timeoutMs,
    };
    const format = wireFormat(options.format);
    const first = await this.#ingest.query(
      {
        query: sql,
        timeout_ms: options.serverTimeoutMs,
        dry_run: options.dryRun ? true : undefined,
        max_results: options.maxResults,
        format,
      },
      requestOptions,
    );
    let requestId = first.requestId;
    let results = requiredResults(first.body.results, requestId);
    let polls = 0;
    while (!(results.completed ?? false)) {
      polls += 1;
      if (polls > MAX_QUERY_POLLS) {
        throw new MedallionError(
          `Medallion query polling exceeded ${MAX_QUERY_POLLS} attempts without completing.`,
          { code: "MEDALLION_QUERY_POLL_LIMIT", requestId },
        );
      }
      const poll = await this.#ingest.getQueryResults(
        {
          query_id: results.query_id ?? "",
          timeout_ms: options.serverTimeoutMs,
          max_results: options.maxResults,
          format,
        },
        requestOptions,
      );
      requestId = poll.requestId;
      results = requiredResults(poll.body.results, requestId);
    }
    return new DatasetQueryResult(this.#ingest, results, {
      requestId,
      requestOptions,
      serverTimeoutMs: options.serverTimeoutMs,
      maxResults: options.maxResults,
      format,
      dryRun: options.dryRun === true,
    });
  }

  /** Create one dataset; replay-safe through the Idempotency-Key header. */
  async create(
    input: DatasetCreateInput,
    options: IngestWriteOptions = {},
  ): Promise<Dataset> {
    const response = await this.#ingest.createDataset(
      { dataset_id: input.datasetId, description: input.description },
      options,
    );
    return datasetFromWire(response.body.dataset, response.requestId);
  }

  /** Read one dataset by identifier. */
  async get(datasetId: string, options: RequestOptions = {}): Promise<Dataset> {
    const response = await this.#ingest.getDataset(
      { dataset_id: datasetId },
      options,
    );
    return datasetFromWire(response.body.dataset, response.requestId);
  }

  /** List one page of datasets. */
  async list(
    input: DatasetListOptions = {},
    options: RequestOptions = {},
  ): Promise<DatasetPage> {
    const response = await this.#ingest.listDatasets(
      { page_size: input.pageSize, page_token: input.pageToken },
      options,
    );
    return {
      requestId: response.requestId,
      nextPageToken: response.body.next_page_token || undefined,
      datasets: (response.body.datasets ?? []).map((dataset) =>
        datasetFromWire(dataset, response.requestId),
      ),
    };
  }

  /** Iterate every dataset across pages without touching page tokens. */
  async *iterate(
    input: DatasetListOptions = {},
    options: RequestOptions = {},
  ): AsyncGenerator<Dataset, void, undefined> {
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
      for (const dataset of page.datasets) yield dataset;
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
  serverTimeoutMs: number | undefined;
  maxResults: number | undefined;
  format: IngestResultFormat | undefined;
  dryRun: boolean;
}

/**
 * A completed query. Row iteration paginates transparently; each result is
 * single-consumption so one iteration never silently replays another's pages.
 */
export class DatasetQueryResult {
  readonly requestId?: string;
  readonly queryId?: string;
  /** Result columns with their declared ClickHouse types. */
  readonly columns: DatasetColumn[];
  readonly totalRows: number;
  /** Bytes the query processed, or would process for a dry run. */
  readonly totalBytesProcessed: number;
  readonly dryRun: boolean;

  readonly #ingest: ProtocolIngestClient;
  readonly #state: QueryResultState;
  #firstPage: IngestQueryResults | undefined;

  constructor(
    ingest: ProtocolIngestClient,
    results: IngestQueryResults,
    state: QueryResultState,
  ) {
    this.#ingest = ingest;
    this.#state = state;
    this.#firstPage = results;
    this.requestId = state.requestId;
    this.queryId = results.query_id || undefined;
    this.dryRun = state.dryRun;
    this.columns = (results.schema?.columns ?? []).map((column) => ({
      name: column.name ?? "",
      type: column.type ?? "",
    }));
    this.totalRows = nonNegativeCount(
      results.total_rows,
      "query.totalRows",
      state.requestId,
    );
    this.totalBytesProcessed = nonNegativeCount(
      results.total_bytes_processed,
      "query.totalBytesProcessed",
      state.requestId,
    );
  }

  /** Iterate every result row as a parsed JSON object, across all pages. */
  async *rows(): AsyncGenerator<Record<string, unknown>, void, undefined> {
    for await (const page of this.#pages()) {
      for (const [index, text] of (page.rows_json ?? []).entries()) {
        yield parseRowObject(text, index, this.#state.requestId);
      }
    }
  }

  /** Iterate raw Arrow IPC stream pages when the arrow format was selected. */
  async *arrowBatches(): AsyncGenerator<Uint8Array, void, undefined> {
    for await (const page of this.#pages()) {
      const encoded = page.arrow_rows?.serialized_record_batch;
      if (encoded === undefined || encoded.length === 0) continue;
      yield new Uint8Array(Buffer.from(encoded, "base64"));
    }
  }

  [Symbol.asyncIterator](): AsyncGenerator<
    Record<string, unknown>,
    void,
    undefined
  > {
    return this.rows();
  }

  async *#pages(): AsyncGenerator<IngestQueryResults, void, undefined> {
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
      if (this.queryId === undefined) {
        throw invalidIngestResponse(
          "Medallion returned a continuation token without a query identifier.",
          this.#state.requestId,
        );
      }
      if (seen.has(next)) throw repeatedCursor();
      seen.add(next);
      const response = await this.#ingest.getQueryResults(
        {
          query_id: this.queryId,
          page_token: next,
          timeout_ms: this.#state.serverTimeoutMs,
          max_results: this.#state.maxResults,
          format: this.#state.format,
        },
        this.#state.requestOptions,
      );
      page = requiredResults(response.body.results, response.requestId);
      if (!(page.completed ?? false)) {
        throw invalidIngestResponse(
          "Medallion reported a completed query as running during pagination.",
          response.requestId,
        );
      }
    }
  }
}

function jsonRowsToWire(
  rows: readonly DatasetRow[],
  insertIds: readonly (string | undefined)[] | undefined,
): IngestRow[] {
  if (!Array.isArray(rows)) {
    throw new MedallionError(
      "datasets.append rows must be an array of JSON objects or one Uint8Array Arrow IPC stream.",
      { code: "MEDALLION_INVALID_ROW" },
    );
  }
  if (insertIds !== undefined && insertIds.length !== rows.length) {
    throw new MedallionError(
      "datasets.append insertIds must align one-to-one with the submitted rows.",
      { code: "MEDALLION_INVALID_ROW" },
    );
  }
  return rows.map((row, index) => {
    if (row === null || typeof row !== "object" || Array.isArray(row)) {
      throw new MedallionError(
        `datasets.append.rows[${index}] must be a plain JSON object of column values.`,
        { code: "MEDALLION_INVALID_ROW" },
      );
    }
    const wire: IngestRow = {
      json: jsonString(row, `datasets.append.rows[${index}]`),
    };
    const insertId = insertIds?.[index];
    if (insertId !== undefined) wire.insert_id = insertId;
    return wire;
  });
}

function arrowPayload(rows: Uint8Array): string {
  if (rows.length === 0) {
    throw new MedallionError(
      "datasets.append Arrow payload must not be empty.",
      { code: "MEDALLION_INVALID_ROW" },
    );
  }
  return Buffer.from(rows).toString("base64");
}

function wireFormat(
  format: DatasetQueryOptions["format"],
): IngestResultFormat | undefined {
  if (format === undefined) return undefined;
  if (format === "json") return "RESULT_FORMAT_JSON";
  if (format === "arrow") return "RESULT_FORMAT_ARROW_IPC";
  throw new MedallionError("query format must be json or arrow.", {
    code: "MEDALLION_INVALID_QUERY",
  });
}

function requiredResults(
  results: IngestQueryResults | undefined,
  requestId: string | undefined,
): IngestQueryResults {
  if (results === undefined) {
    throw invalidIngestResponse(
      "Medallion returned a query acknowledgement without results.",
      requestId,
    );
  }
  return results;
}

function parseRowObject(
  text: string,
  index: number,
  requestId: string | undefined,
): Record<string, unknown> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    throw invalidIngestResponse(
      `Medallion returned a malformed JSON result row at index ${index}.`,
      requestId,
    );
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw invalidIngestResponse(
      `Medallion returned a non-object JSON result row at index ${index}.`,
      requestId,
    );
  }
  return parsed as Record<string, unknown>;
}

function datasetFromWire(
  dataset: IngestDataset | undefined,
  requestId: string | undefined,
): Dataset {
  if (dataset === undefined || (dataset.dataset_id ?? "").length === 0) {
    throw invalidIngestResponse(
      "Medallion returned a dataset without a valid identifier.",
      requestId,
    );
  }
  return {
    datasetId: dataset.dataset_id ?? "",
    description: dataset.description || undefined,
    createTime: dataset.create_time || undefined,
  };
}

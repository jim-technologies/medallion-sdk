import type { TracingConfig } from "./tracing.js";

export type IdInput = string | number | bigint;

export interface RequestOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}

export interface RetryOptions {
  /** Total attempts, including the first. Defaults to 1 (no automatic retry). */
  maxAttempts?: number;
  /** First backoff; retries and tables.query() polls double it to maxDelayMs. */
  initialDelayMs?: number;
  maxDelayMs?: number;
  /** Random delay spread from 0 through 1. */
  jitterRatio?: number;
}

export type FetchLike = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Promise<Response>;

export interface MedallionClientOptions {
  baseUrl: string;
  apiKey?: string;
  accessToken?: string;
  /** Immutable workspace bound to this client and its credential. */
  workspaceId: string;
  fetch?: FetchLike;
  timeoutMs?: number;
  retry?: RetryOptions;
  tracing?: TracingConfig;
}

export interface ResponseMetadata {
  requestId?: string;
}

export interface WriteResultMetadata extends ResponseMetadata {
  idempotencyKey: string;
  duplicate: boolean;
}

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue =
  | JsonPrimitive
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };

export interface IngestRow {
  insert_id?: string;
  /** The row values keyed by column name. */
  json: Record<string, unknown>;
}

/** Arrow rows as one base64 Arrow IPC stream in the protobuf JSON codec. */
export interface IngestArrowRecordBatch {
  serialized_record_batch?: string;
}

/** One declared column of a table schema, or of a query result schema. */
export interface IngestColumnSchema {
  name?: string;
  type?: string;
  nullable?: boolean;
}

/** The ordered columns of a table schema or query result schema. */
export interface IngestTableSchema {
  columns?: IngestColumnSchema[];
}

/** One table on the ingest wire; `name` is "tables/{table}". */
export interface IngestTable {
  name?: string;
  schema?: IngestTableSchema;
  time_column?: string;
  sort_columns?: string[];
  create_time?: string;
}

export interface IngestCreateTableRequest {
  table_id: string;
  table: IngestTable;
  request_id?: string;
}

export interface IngestUpdateTableRequest {
  table: IngestTable;
  request_id?: string;
}

export interface IngestGetTableRequest {
  name: string;
}

/** Shared acknowledgement of CreateTable, GetTable, and UpdateTable. */
export interface IngestTableResponse {
  table?: IngestTable;
}

export interface IngestListTablesRequest {
  page_size?: number;
  page_token?: string;
}

export interface IngestListTablesResponse {
  tables?: IngestTable[];
  next_page_token?: string;
}

export interface IngestAppendRowsRequest {
  table: string;
  rows?: IngestRow[];
  arrow_rows?: IngestArrowRecordBatch;
  request_id?: string;
  skip_invalid_rows?: boolean;
}

/** Wire-compatible subset of google.rpc.Status carried by per-row errors. */
export interface IngestRpcStatus {
  code?: number;
  message?: string;
}

export interface IngestRowError {
  index?: string | number;
  error?: IngestRpcStatus;
}

export interface IngestAppendRowsResponse {
  accepted_rows?: string | number;
  row_errors?: IngestRowError[];
}

export interface IngestRunQueryRequest {
  query: string;
  timeout_ms?: number;
  dry_run?: boolean;
  page_size?: number;
}

/** Lifecycle state of one query. */
export type IngestQueryState = "RUNNING" | "SUCCEEDED" | "FAILED";

/** Shared acknowledgement of RunQuery and GetQueryResults. */
export interface IngestQueryResponse {
  name?: string;
  state?: string;
  schema?: IngestTableSchema;
  rows?: Record<string, unknown>[];
  next_page_token?: string;
  total_rows?: string | number;
  error?: IngestRpcStatus;
}

export interface IngestGetQueryResultsRequest {
  name: string;
  page_token?: string;
  page_size?: number;
}

/** Options accepted by ingest calls that carry a batch idempotency key. */
export interface IngestWriteOptions extends RequestOptions {
  /**
   * Stable batch deduplication key sent as the Idempotency-Key header and as
   * the request's `request_id`. Generated automatically when omitted; pass
   * the same key to make a manual replay of the same batch safe.
   */
  idempotencyKey?: string;
}

/** One appended row: a plain JSON object of column values. */
export type TableRow = { readonly [column: string]: JsonValue };

export interface TableAppendOptions extends IngestWriteOptions {
  /**
   * Optional per-row identifiers, index-aligned with the submitted JSON rows
   * and passed through as each row's insert_id. They correlate row errors
   * only; batch deduplication uses the idempotency key.
   */
  insertIds?: readonly (string | undefined)[];
  /**
   * Report invalid rows in rowErrors and commit the valid remainder instead
   * of rejecting the whole batch.
   */
  skipInvalidRows?: boolean;
}

export interface TableRowError {
  index: number;
  /** Numeric google.rpc.Code value for the rejection. */
  code?: number;
  message?: string;
}

export interface TableAppendResult extends ResponseMetadata {
  /** The idempotency key this batch was sent with. */
  idempotencyKey: string;
  /** Rows durably accepted by this request, or by the replayed original. */
  acceptedRows: number;
  /** Per-row rejections; empty when every submitted row was accepted. */
  rowErrors: TableRowError[];
}

export interface TableQueryOptions extends RequestOptions {
  /** Synchronous server-side wait budget per request, in milliseconds. */
  serverTimeoutMs?: number;
  /** Validate the statement and report its schema without executing it. */
  dryRun?: boolean;
  /** Largest number of rows per result page. */
  pageSize?: number;
}

/** BigQuery-style column types the tabular schema accepts. */
export type TableColumnType =
  | "BOOL"
  | "INT64"
  | "FLOAT64"
  | "STRING"
  | "BYTES"
  | "TIMESTAMP"
  | "DATE"
  | "JSON";

export interface TableColumn {
  name: string;
  type: TableColumnType | (string & {});
  /** Whether the column accepts null values. */
  nullable?: boolean;
}

export interface TableCreateInput {
  tableId: string;
  /** Ordered columns of the declared schema. */
  columns: readonly TableColumn[];
  /** Name of the TIMESTAMP column carrying event time. */
  timeColumn: string;
  /** Optional sort key; defaults to the time column. */
  sortColumns?: readonly string[];
}

export interface TableUpdateInput {
  tableId: string;
  /**
   * The FULL desired schema. Evolution is additive only: existing columns
   * must be repeated unchanged and in order, and new columns must be
   * nullable and appended at the end.
   */
  columns: readonly TableColumn[];
}

export interface Table {
  tableId: string;
  /** Resource name, "tables/{table}". */
  name: string;
  columns: TableColumn[];
  timeColumn: string;
  sortColumns: string[];
  createTime?: string;
}

export interface TableListOptions {
  pageSize?: number;
  pageToken?: string;
}

export interface TablePage extends ResponseMetadata {
  tables: Table[];
  nextPageToken?: string;
}

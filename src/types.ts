import type { TracingConfig } from "./tracing.js";

export type IdInput = string | number | bigint;

export type KnownActorType = "user" | "service_account" | "system";

export interface ActorRef {
  type?: KnownActorType | (string & {});
  id: IdInput;
  provider?: string;
}

export interface ResourceRef {
  type: string;
  id: IdInput;
}

export interface NormalizedActorRef {
  type?: KnownActorType | (string & {});
  id: string;
  provider?: string;
}

export interface NormalizedResourceRef {
  type: string;
  id: string;
}

export interface RequestOptions {
  signal?: AbortSignal;
  timeoutMs?: number;
}

export interface RetryOptions {
  /** Total attempts, including the first. Defaults to 1 (no automatic retry). */
  maxAttempts?: number;
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
  defaultConnectorId?: string;
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

export interface PublishedEventResult {
  idempotencyKey: string;
  eventId: string;
  duplicate: boolean;
}

export interface EventRecordResponse extends WriteResultMetadata {
  result: "accepted" | "duplicate";
  acceptedCount: number;
  duplicateCount: number;
  events: PublishedEventResult[];
}

export interface EventBatchResponse extends ResponseMetadata {
  /** True only when every submitted event was already present. */
  duplicate: boolean;
  /** Aggregate acknowledgement across the complete atomic batch. */
  result: "accepted" | "duplicate" | "mixed";
  acceptedCount: number;
  duplicateCount: number;
  events: PublishedEventResult[];
}

// Audit publishes have the same acknowledgement fields as CDC publishes, but
// the public name keeps the two event families explicit at call sites.
export type PublishedAuditEventResult = PublishedEventResult;
export type AuditRecordResponse = EventRecordResponse;
export type AuditOutcome = "succeeded" | "failed" | "indeterminate";
export type AuditOrigin = "external_provider" | "connect";

export interface AuditRecordInput {
  connectorId?: string;
  actor?: ActorRef;
  action: string;
  outcome: AuditOutcome;
  resource: ResourceRef;
  before?: unknown;
  after?: unknown;
  metadata?: Record<string, unknown>;
  description?: string;
  evidenceUrl?: string;
  idempotencyKey: string;
  sourceEventId?: IdInput;
  occurredAt?: string | Date;
  /** Compact caller facts encoded deterministically into payload_json. */
  payload?: JsonValue;
  /** Valid raw JSON retained for compatibility. Mutually exclusive with payload. */
  payloadJson?: string;
}

export interface AuditIngestionEventInput {
  resourceType: string;
  resourceId: IdInput;
  action: string;
  outcome: AuditOutcome;
  idempotencyKey: string;
  payload?: JsonValue;
  payloadJson?: string;
  sourceEventId?: IdInput;
  actor?: ActorRef;
  occurredAt?: string | Date;
  description?: string;
}

export interface AuditIngestionRecordInput extends AuditIngestionEventInput {
  /** Request-level connector override for this single-event call. */
  connectorId?: string;
}

export interface AuditBatchInput {
  connectorId?: string;
  events: readonly AuditIngestionEventInput[];
}

export interface AuditTrailInput {
  connectorId?: string;
  resourceType: string;
  resourceId: IdInput;
  action?: string;
  cursor?: string;
  limit?: number;
  pageSize?: number;
  actor?: ActorRef;
  ingesterPrincipal?: string;
  origin?: AuditOrigin;
  outcome?: AuditOutcome;
}

export interface AuditListInput {
  connectorId?: string;
  resourceType?: string;
  resourceId?: IdInput;
  action?: string;
  cursor?: string;
  limit?: number;
  actor?: ActorRef;
  ingesterPrincipal?: string;
  sourceSystem?: string;
  occurredAtFrom?: string | Date;
  occurredAtTo?: string | Date;
  origin?: AuditOrigin;
  outcome?: AuditOutcome;
}

export type CdcOperation = "insert" | "update" | "delete" | "snapshot";

export interface CdcEventInput {
  connectorId?: string;
  source: string;
  table: string;
  operation: CdcOperation;
  primaryKey: Record<string, IdInput>;
  entityType?: string;
  entityId?: IdInput;
  actor?: ActorRef;
  before?: unknown;
  after?: unknown;
  metadata?: Record<string, unknown>;
  idempotencyKey: string;
  sourceEventId?: IdInput;
  occurredAt?: string | Date;
  description?: string;
  payload?: JsonValue;
  payloadJson?: string;
}

export interface CdcIngestionEventInput {
  streamName: string;
  entityType: string;
  entityId: IdInput;
  operation: CdcOperation;
  idempotencyKey: string;
  payload?: JsonValue;
  payloadJson?: string;
  sourceEventId?: IdInput;
  actor?: ActorRef;
  occurredAt?: string | Date;
  description?: string;
}

export interface CdcRecordInput extends CdcIngestionEventInput {
  /** Request-level connector override for this single-event call. */
  connectorId?: string;
}

export interface CdcBatchInput {
  connectorId?: string;
  events: readonly CdcIngestionEventInput[];
}

export interface CdcListInput {
  connectorId?: string;
  entityType?: string;
  entityId?: IdInput;
  streamName?: string;
  sourceSystem?: string;
  actor?: ActorRef;
  ingesterPrincipal?: string;
  occurredAtFrom?: string | Date;
  occurredAtTo?: string | Date;
  cursor?: string;
  limit?: number;
}

export interface CdcReadEvent {
  id: string;
  eventId: string;
  workspaceId: string;
  connectorId?: string;
  streamName: string;
  entityType: string;
  entityId: string;
  operation: CdcOperation;
  sourceEventId?: string;
  idempotencyKey: string;
  actor?: NormalizedActorRef;
  actorPrincipal?: string;
  payload: unknown;
  payloadJson: string;
  occurredAt?: string;
  observedAt?: string;
  description?: string;
  sourceSystem?: string;
  ingesterPrincipal?: string;
}

export interface CdcPage extends ResponseMetadata {
  events: CdcReadEvent[];
  nextCursor?: string;
}

export type JsonPrimitive = string | number | boolean | null;
export type JsonValue =
  | JsonPrimitive
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };

export interface AuditTrailResponse extends ResponseMetadata {
  events: AuditTrailEvent[];
  nextCursor?: string;
}

export interface AuditTrailEvent {
  id: string;
  eventId: string;
  workspaceId: string;
  connectorId?: string;
  actor?: NormalizedActorRef;
  ingesterPrincipal?: string;
  /** @deprecated Use actor for the source application actor or ingesterPrincipal for Connect server provenance. */
  actorPrincipal?: string;
  action: string;
  description?: string;
  idempotencyKey: string;
  targetType: string;
  targetId: string;
  entityType: string;
  entityId: string;
  requestId?: string;
  metadata?: Record<string, unknown>;
  createdAt?: string;
  occurredAt?: string;
  observedAt?: string;
  before?: unknown;
  after?: unknown;
  evidenceUrl?: string;
  sourceEventId?: string;
  sourceSystem?: string;
  origin: AuditOrigin;
  outcome: AuditOutcome;
  payload: unknown;
  /** Exact canonical payload_json text returned by Connect. */
  payloadJson: string;
}

export interface ConnectCdcEventInput {
  stream_name: string;
  entity_type: string;
  entity_id: string;
  operation: string;
  source_event_id?: string;
  idempotency_key: string;
  actor_principal?: string;
  payload_json: string;
  occurred_at?: string;
  description?: string;
}

export interface ConnectCdcEvent extends ConnectCdcEventInput {
  id?: string | number;
  connector_id?: string;
  observed_at?: string;
  source_system?: string;
  ingested_by_principal?: string;
  workspace_id?: string;
}

export interface ConnectPublishCdcEventsRequest {
  connector_id: string;
  events: ConnectCdcEventInput[];
}

export interface ConnectPublishedCdcEvent {
  idempotency_key?: string;
  event_id?: string | number;
  duplicate?: boolean;
}

export interface ConnectPublishCdcEventsResponse {
  accepted_count?: number;
  duplicate_count?: number;
  events?: ConnectPublishedCdcEvent[];
}

export interface ConnectAuditEventInput {
  resource_type: string;
  resource_id: string;
  action: string;
  source_event_id?: string;
  idempotency_key: string;
  actor_principal?: string;
  payload_json: string;
  occurred_at?: string;
  description?: string;
  outcome: string;
}

export interface ConnectAuditEvent extends ConnectAuditEventInput {
  id?: string | number;
  connector_id?: string;
  observed_at?: string;
  source_system?: string;
  ingested_by_principal?: string;
  origin?: string;
  workspace_id?: string;
}

export interface ConnectPublishAuditEventsRequest {
  connector_id: string;
  events: ConnectAuditEventInput[];
}

export interface ConnectPublishedAuditEvent {
  idempotency_key?: string;
  event_id?: string | number;
  duplicate?: boolean;
}

export interface ConnectPublishAuditEventsResponse {
  accepted_count?: number;
  duplicate_count?: number;
  events?: ConnectPublishedAuditEvent[];
}

export interface ConnectListAuditEventsRequest {
  workspace_id: string;
  connector_id?: string;
  resource_type?: string;
  resource_id?: string;
  limit?: number;
  actor_principal?: string;
  action?: string;
  occurred_at_from?: string;
  occurred_at_to?: string;
  source_system?: string;
  page_cursor?: string;
  ingested_by_principal?: string;
  origin?: string;
  outcome?: string;
}

export interface ConnectListAuditEventsResponse {
  events?: ConnectAuditEvent[];
  next_page_cursor?: string;
}

export interface ConnectListCdcEventsRequest {
  workspace_id: string;
  connector_id?: string;
  entity_type?: string;
  entity_id?: string;
  limit?: number;
  actor_principal?: string;
  ingested_by_principal?: string;
  occurred_at_from?: string;
  occurred_at_to?: string;
  source_system?: string;
  stream_name?: string;
  page_cursor?: string;
}

export interface ConnectListCdcEventsResponse {
  events?: ConnectCdcEvent[];
  next_page_cursor?: string;
}

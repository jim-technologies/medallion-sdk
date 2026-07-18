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

export type FetchLike = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Promise<Response>;

export interface MedallionClientOptions {
  baseUrl: string;
  apiKey?: string;
  accessToken?: string;
  connectBaseUrl?: string;
  ontologyBaseUrl?: string;
  storageBaseUrl?: string;
  organizationId?: string;
  tenantId?: string;
  defaultConnectorId?: string;
  fetch?: FetchLike;
  timeoutMs?: number;
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
  eventId?: string;
  duplicate: boolean;
}

export interface EventRecordResponse extends WriteResultMetadata {
  result: "accepted" | "duplicate";
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
  actor: ActorRef;
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
}

export interface AuditTrailInput {
  organizationId?: string;
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
}

export interface DatasourceRegistrationInput {
  organizationId?: string;
  name: string;
  type: string;
  /** Stable key reused only when retrying this same registration mutation. */
  idempotencyKey: string;
  displayName?: string;
  externalId?: IdInput;
  /** Caller-side annotations copied into the returned Datasource; Connect does not persist them. */
  metadata?: Record<string, unknown>;
}

export interface Datasource {
  id: string;
  organizationId?: string;
  tenantId?: string;
  kind?: string;
  type?: string;
  sourceSystem?: string;
  name?: string;
  displayName?: string;
  externalId?: string;
  status?: string;
  createdAt?: string;
  updatedAt?: string;
  metadata?: Record<string, unknown>;
}

export interface RegisterDatasourceResponse extends ResponseMetadata {
  datasource: Datasource;
}

export interface QueryInput {
  question: string;
  includeInferred?: boolean;
}

export interface QueryResponse extends ResponseMetadata {
  answer?: string;
  resourceIds: string[];
  explanations: string[];
  events: AuditTrailEvent[];
}

export interface PlanActionInput {
  actionName: string;
  input?: unknown;
}

export interface PlanActionResponse extends ResponseMetadata {
  plan: ActionInvocation;
  requiredApprovals: string[];
}

export interface ExecuteActionInput {
  actionName: string;
  input?: unknown;
  idempotencyKey: string;
}

export interface ExecuteActionResponse extends ResponseMetadata {
  idempotencyKey: string;
  result: "accepted" | "succeeded" | "failed" | "rejected";
  invocation: ActionInvocation;
}

export interface ActionInvocation {
  id?: string;
  tenantId?: string;
  actionName?: string;
  actorPrincipal?: string;
  idempotencyKey?: string;
  request?: unknown;
  response?: unknown;
  status?: string;
  explanation?: string;
  errorMessage?: string;
  createdAt?: string;
  resourceId?: string;
}

export interface AuditTrailResponse extends ResponseMetadata {
  events: AuditTrailEvent[];
  nextCursor?: string;
}

export interface AuditTrailEvent {
  id?: string;
  eventId?: string;
  tenantId?: string;
  organizationId?: string;
  connectorId?: string;
  actor?: NormalizedActorRef;
  ingesterPrincipal?: string;
  /** @deprecated Use actor for the source application actor or ingesterPrincipal for Connect server provenance. */
  actorPrincipal?: string;
  action?: string;
  targetType?: string;
  targetId?: string;
  entityType?: string;
  entityId?: string;
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
  origin?: AuditOrigin;
  outcome?: AuditOutcome;
  payload?: unknown;
}

export interface StorageUploadInput {
  org?: string;
  bucket?: string;
  path: string;
  data: BodyInit;
  repo?: string;
  contentType?: string;
  idempotencyKey?: string;
}

export interface StorageUploadResponse extends ResponseMetadata {
  result: "uploaded";
  org: string;
  path: string;
  entry: StorageCatalogEntry;
}

export type StorageCatalogEntry = Record<string, unknown>;

export interface ConnectConnector {
  id: string;
  organization_id?: string;
  kind?: string;
  source_system?: string;
  display_name?: string;
  external_id?: string;
  status?: string;
  created_at?: string;
  updated_at?: string;
}

export interface ConnectRegisterConnectorRequest {
  organization_id: string;
  kind: string;
  source_system: string;
  display_name: string;
  external_id?: string;
  idempotency_key: string;
}

export interface ConnectRegisterConnectorResponse {
  connector: ConnectConnector;
}

export interface ConnectCdcEvent {
  id?: string | number;
  organization_id?: string;
  connector_id?: string;
  stream_name: string;
  entity_type: string;
  entity_id: string;
  operation?: string;
  source_event_id?: string;
  idempotency_key: string;
  actor_principal?: string;
  payload_json: string;
  occurred_at?: string;
  observed_at?: string;
  description?: string;
  source_system?: string;
  ingested_by_principal?: string;
}

export interface ConnectPublishCdcEventsRequest {
  connector_id: string;
  events: ConnectCdcEvent[];
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

export interface ConnectAuditEvent {
  id?: string | number;
  organization_id?: string;
  connector_id?: string;
  resource_type: string;
  resource_id: string;
  action: string;
  source_event_id?: string;
  idempotency_key: string;
  actor_principal?: string;
  payload_json: string;
  occurred_at?: string;
  observed_at?: string;
  description?: string;
  source_system?: string;
  ingested_by_principal?: string;
  origin?: string;
  outcome: string;
}

export interface ConnectPublishAuditEventsRequest {
  connector_id: string;
  events: ConnectAuditEvent[];
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
  organization_id: string;
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
  organization_id: string;
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

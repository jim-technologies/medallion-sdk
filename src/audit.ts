import { MedallionError } from "./errors.js";
import {
  actorPrincipalFromRef,
  normalizeActorRef,
  normalizeResourceRef,
} from "./ids.js";
import {
  actorFromEventPayload,
  assertIteratorPageWithinLimit,
  batchResponse,
  ingestionPageSize,
  invalidListResponse,
  optionalListText,
  optionalListTimestamp,
  optionalText,
  parseListJsonPayload,
  repeatedCursor,
  requiredConnectorId,
  requiredListEventId,
  requiredListIdempotencyKey,
  requiredListText,
  requiredResponseWorkspace,
  requiredText,
  singleEventResponse,
  validateBatchSize,
} from "./ingestion.js";
import {
  compactRecord,
  occurredAt,
  optionalId,
  rawOrJsonString,
  requiredId,
  requiredIdempotencyKey,
} from "./payload.js";
import type { ProtocolConnectClient } from "./protocol.js";
import type {
  AuditBatchInput,
  AuditIngestionEventInput,
  AuditIngestionRecordInput,
  AuditListInput,
  AuditOrigin,
  AuditOutcome,
  AuditRecordInput,
  AuditRecordResponse,
  AuditTrailEvent,
  AuditTrailInput,
  AuditTrailResponse,
  ConnectAuditEvent,
  ConnectAuditEventInput,
  EventBatchResponse,
  RequestOptions,
} from "./types.js";

export interface AuditClientOptions {
  workspaceId: string;
  defaultConnectorId?: string;
}

export class AuditClient {
  readonly #connect: ProtocolConnectClient;
  readonly #defaultConnectorId?: string;
  readonly #workspaceId: string;

  constructor(connect: ProtocolConnectClient, options: AuditClientOptions) {
    this.#connect = connect;
    this.#defaultConnectorId = options.defaultConnectorId;
    this.#workspaceId = options.workspaceId;
  }

  async record(
    input: AuditRecordInput | AuditIngestionRecordInput,
    options: RequestOptions = {},
  ): Promise<AuditRecordResponse> {
    const connectorId = requiredConnectorId(
      input.connectorId ?? this.#defaultConnectorId,
      "publish an audit event",
    );
    const eventInput = { ...input };
    delete eventInput.connectorId;
    const event = isModernEvent(eventInput)
      ? modernEventToWire(eventInput)
      : legacyEventToWire(eventInput);
    const response = await this.publishWireEvents(
      connectorId,
      [event],
      options,
    );
    return singleEventResponse(response);
  }

  async publishBatch(
    input: AuditBatchInput,
    options: RequestOptions = {},
  ): Promise<EventBatchResponse> {
    validateBatchSize(input.events);
    const connectorId = requiredConnectorId(
      input.connectorId ?? this.#defaultConnectorId,
      "publish audit events",
    );
    const events = input.events.map((event) => modernEventToWire(event));
    return this.publishWireEvents(connectorId, events, options);
  }

  async list(
    input: AuditListInput = {},
    options: RequestOptions = {},
  ): Promise<AuditTrailResponse> {
    const resourceType = optionalText(
      input.resourceType,
      "audit.resourceType",
      256,
    );
    const resourceId = optionalId(input.resourceId, "audit.resourceId", 1024);
    if ((resourceType === undefined) !== (resourceId === undefined)) {
      throw new MedallionError(
        "audit resourceType and resourceId must be provided together.",
        { code: "MEDALLION_INVALID_AUDIT_FILTER" },
      );
    }
    const sourceActor =
      input.actor === undefined ? undefined : normalizeActorRef(input.actor);
    const sourceActorPrincipal =
      sourceActor === undefined
        ? undefined
        : requiredText(
            actorPrincipalFromRef(sourceActor),
            "audit.actorPrincipal",
            512,
          );
    const response = await this.#connect.listAuditEvents(
      compactRecord({
        workspace_id: this.#workspaceId,
        connector_id: optionalText(
          input.connectorId ?? this.#defaultConnectorId,
          "audit.connectorId",
          128,
        ),
        resource_type: resourceType,
        resource_id: resourceId,
        limit: ingestionPageSize(input.limit),
        actor_principal: sourceActorPrincipal,
        ingested_by_principal: optionalText(
          input.ingesterPrincipal,
          "audit.ingesterPrincipal",
          512,
        ),
        action: optionalText(input.action, "audit.action", 256),
        occurred_at_from: occurredAt(input.occurredAtFrom),
        occurred_at_to: occurredAt(input.occurredAtTo),
        source_system: optionalText(
          input.sourceSystem,
          "audit.sourceSystem",
          256,
        ),
        origin:
          input.origin === undefined ? undefined : auditOrigin(input.origin),
        outcome:
          input.outcome === undefined ? undefined : auditOutcome(input.outcome),
        page_cursor: optionalText(input.cursor, "audit.cursor", 2048),
      }),
      options,
    );
    const events = (response.body.events ?? []).map((event) =>
      auditEventFromWire(event, this.#workspaceId, response.requestId),
    );
    return {
      requestId: response.requestId,
      nextCursor: optionalListText(
        response.body.next_page_cursor,
        "audit.list.nextCursor",
        2_048,
        response.requestId,
      ),
      events,
    };
  }

  /** @deprecated Use list; retained for resource-scoped compatibility. */
  trail(
    input: AuditTrailInput,
    options: RequestOptions = {},
  ): Promise<AuditTrailResponse> {
    return this.list(
      {
        ...input,
        limit: input.limit ?? input.pageSize,
      },
      options,
    );
  }

  async *iterate(
    input: AuditListInput = {},
    options: RequestOptions = {},
  ): AsyncGenerator<AuditTrailEvent, void, undefined> {
    const filters: AuditListInput = {
      ...input,
      actor: input.actor === undefined ? undefined : { ...input.actor },
      occurredAtFrom: occurredAt(input.occurredAtFrom),
      occurredAtTo: occurredAt(input.occurredAtTo),
    };
    const requestOptions = { ...options };
    let cursor = filters.cursor;
    const seen = new Set<string>();
    let pages = 0;
    if (cursor !== undefined && cursor.length > 0) seen.add(cursor);

    for (;;) {
      pages += 1;
      assertIteratorPageWithinLimit(pages);
      const page = await this.list({ ...filters, cursor }, requestOptions);
      for (const event of page.events) yield event;
      const nextCursor = page.nextCursor;
      if (nextCursor === undefined || nextCursor.length === 0) return;
      if (seen.has(nextCursor)) throw repeatedCursor();
      seen.add(nextCursor);
      cursor = nextCursor;
    }
  }

  private async publishWireEvents(
    connectorId: string,
    events: ConnectAuditEventInput[],
    options: RequestOptions,
  ): Promise<EventBatchResponse> {
    const expectedKeys = events.map((event) => event.idempotency_key);
    const response = await this.#connect.publishAuditEvents(
      { connector_id: connectorId, events },
      options,
    );
    return batchResponse(response.body, expectedKeys, response.requestId);
  }
}

function modernEventToWire(
  input: AuditIngestionEventInput,
): ConnectAuditEventInput {
  rejectServerOwnedInput(input);
  const actor =
    input.actor === undefined ? undefined : normalizeActorRef(input.actor);
  return compactRecord({
    resource_type: requiredText(input.resourceType, "audit.resourceType", 256),
    resource_id: requiredId(input.resourceId, "audit.resourceId", 1024),
    action: requiredText(input.action, "audit.action", 256),
    outcome: auditOutcome(input.outcome),
    idempotency_key: requiredIdempotencyKey(
      input.idempotencyKey,
      "audit.idempotencyKey",
      512,
    ),
    actor_principal:
      actor === undefined
        ? undefined
        : requiredText(
            actorPrincipalFromRef(actor),
            "audit.actorPrincipal",
            512,
          ),
    payload_json: rawOrJsonString(
      input.payload,
      input.payloadJson,
      "audit.payload",
    ),
    occurred_at: occurredAt(input.occurredAt),
    description: optionalText(input.description, "audit.description", 4096),
    source_event_id: optionalId(
      input.sourceEventId,
      "audit.sourceEventId",
      1024,
    ),
  });
}

function legacyEventToWire(input: AuditRecordInput): ConnectAuditEventInput {
  rejectServerOwnedInput(input);
  const resource = normalizeResourceRef(input.resource);
  const actor =
    input.actor === undefined ? undefined : normalizeActorRef(input.actor);
  const compatibilityPayload = {
    actor: actor ?? null,
    resource,
    before: input.before ?? null,
    after: input.after ?? null,
    metadata: input.metadata ?? null,
    evidenceUrl: input.evidenceUrl ?? null,
  };
  return compactRecord({
    resource_type: requiredText(resource.type, "audit.resourceType", 256),
    resource_id: requiredId(resource.id, "audit.resourceId", 1024),
    action: requiredText(input.action, "audit.action", 256),
    outcome: auditOutcome(input.outcome),
    idempotency_key: requiredIdempotencyKey(
      input.idempotencyKey,
      "audit.idempotencyKey",
      512,
    ),
    actor_principal:
      actor === undefined
        ? undefined
        : requiredText(
            actorPrincipalFromRef(actor),
            "audit.actorPrincipal",
            512,
          ),
    payload_json: rawOrJsonString(
      input.payload === undefined ? compatibilityPayload : input.payload,
      input.payloadJson,
      "audit.payload",
    ),
    occurred_at: occurredAt(input.occurredAt),
    description: optionalText(input.description, "audit.description", 4096),
    source_event_id: optionalId(
      input.sourceEventId,
      "audit.sourceEventId",
      1024,
    ),
  });
}

export function auditEventFromWire(
  event: ConnectAuditEvent,
  expectedWorkspaceId: string,
  requestId?: string,
): AuditTrailEvent {
  const resourceType = requiredListText(
    event.resource_type,
    "audit.list.events[].resourceType",
    256,
    requestId,
  );
  const resourceId = requiredListText(
    event.resource_id,
    "audit.list.events[].resourceId",
    1_024,
    requestId,
  );
  const action = requiredListText(
    event.action,
    "audit.list.events[].action",
    256,
    requestId,
  );
  const idempotencyKey = requiredListIdempotencyKey(
    event.idempotency_key,
    "audit.list.events[].idempotencyKey",
    requestId,
  );
  const workspaceId = requiredResponseWorkspace(
    event.workspace_id,
    expectedWorkspaceId,
    "audit.list.events[].workspaceId",
    requestId,
  );
  const connectorId = optionalListText(
    event.connector_id,
    "audit.list.events[].connectorId",
    128,
    requestId,
  );
  const sourceEventId = optionalListText(
    event.source_event_id,
    "audit.list.events[].sourceEventId",
    1_024,
    requestId,
  );
  const actorPrincipal = optionalListText(
    event.actor_principal,
    "audit.list.events[].actorPrincipal",
    512,
    requestId,
  );
  const description = optionalListText(
    event.description,
    "audit.list.events[].description",
    4_096,
    requestId,
  );
  const sourceSystem = optionalListText(
    event.source_system,
    "audit.list.events[].sourceSystem",
    256,
    requestId,
  );
  const ingesterPrincipal = optionalListText(
    event.ingested_by_principal,
    "audit.list.events[].ingestedByPrincipal",
    512,
    requestId,
  );
  const payload = parseListJsonPayload(
    event.payload_json,
    "audit.list.events[].payloadJson",
    requestId,
  );
  const payloadRecord = recordValue(payload);
  const occurredAt = optionalListTimestamp(
    event.occurred_at,
    "audit.list.events[].occurredAt",
    requestId,
  );
  const observedAt = optionalListTimestamp(
    event.observed_at,
    "audit.list.events[].observedAt",
    requestId,
  );
  const actor = actorFromEventPayload(payload, actorPrincipal);
  const eventId = requiredListEventId(event.id, requestId);
  const origin = auditOriginFromWire(event.origin);
  if (origin === undefined) {
    throw invalidListResponse(
      "Medallion returned a listed audit event without a concrete origin.",
      requestId,
    );
  }
  const outcome = auditOutcomeFromWire(event.outcome);
  if (outcome === undefined) {
    throw invalidListResponse(
      "Medallion returned a listed audit event without a concrete outcome.",
      requestId,
    );
  }
  return {
    id: eventId,
    eventId,
    workspaceId,
    connectorId,
    actor,
    ingesterPrincipal,
    actorPrincipal,
    action,
    description,
    idempotencyKey,
    targetType: resourceType,
    targetId: resourceId,
    entityType: resourceType,
    entityId: resourceId,
    metadata: recordValue(payloadRecord?.metadata),
    createdAt: observedAt,
    occurredAt,
    observedAt,
    before: payloadRecord?.before,
    after: payloadRecord?.after,
    evidenceUrl: stringValue(payloadRecord?.evidenceUrl),
    sourceEventId,
    sourceSystem,
    origin,
    outcome,
    payload,
    payloadJson: event.payload_json,
  };
}

function auditOutcome(outcome: AuditOutcome): string {
  switch (outcome) {
    case "succeeded":
      return "AUDIT_EVENT_OUTCOME_SUCCEEDED";
    case "failed":
      return "AUDIT_EVENT_OUTCOME_FAILED";
    case "indeterminate":
      return "AUDIT_EVENT_OUTCOME_INDETERMINATE";
    default:
      throw new MedallionError(
        "audit outcome must be succeeded, failed, or indeterminate.",
        { code: "MEDALLION_INVALID_AUDIT_OUTCOME" },
      );
  }
}

function auditOrigin(origin: AuditOrigin): string {
  switch (origin) {
    case "external_provider":
      return "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER";
    case "connect":
      return "AUDIT_EVENT_ORIGIN_CONNECT";
    default:
      throw new MedallionError(
        "audit origin must be external_provider or connect.",
        { code: "MEDALLION_INVALID_AUDIT_ORIGIN" },
      );
  }
}

function auditOutcomeFromWire(
  value: string | undefined,
): AuditOutcome | undefined {
  switch (value) {
    case "AUDIT_EVENT_OUTCOME_SUCCEEDED":
      return "succeeded";
    case "AUDIT_EVENT_OUTCOME_FAILED":
      return "failed";
    case "AUDIT_EVENT_OUTCOME_INDETERMINATE":
      return "indeterminate";
    default:
      return undefined;
  }
}

function auditOriginFromWire(
  value: string | undefined,
): AuditOrigin | undefined {
  switch (value) {
    case "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER":
      return "external_provider";
    case "AUDIT_EVENT_ORIGIN_CONNECT":
      return "connect";
    default:
      return undefined;
  }
}

function isModernEvent(
  input: AuditRecordInput | AuditIngestionEventInput,
): input is AuditIngestionEventInput {
  return "resourceType" in input;
}

function recordValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function rejectServerOwnedInput(input: object): void {
  const record = input as Record<string, unknown>;
  const fields = [
    "id",
    "eventId",
    "workspaceId",
    "connectorId",
    "observedAt",
    "sourceSystem",
    "ingestedByPrincipal",
    "origin",
    "workspace_id",
    "connector_id",
    "observed_at",
    "source_system",
    "ingested_by_principal",
  ];
  if (fields.some((field) => Object.hasOwn(record, field))) {
    throw new MedallionError(
      "Audit events must not contain server-owned fields.",
      { code: "MEDALLION_SERVER_DERIVED_FIELD" },
    );
  }
}

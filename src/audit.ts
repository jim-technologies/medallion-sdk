import { MedallionError } from "./errors.js";
import {
  actorPrincipalFromRef,
  normalizeActorRef,
  normalizeResourceRef,
} from "./ids.js";
import {
  compactRecord,
  jsonString,
  occurredAt,
  optionalId,
  requiredIdempotencyKey,
} from "./payload.js";
import type { ProtocolConnectClient } from "./protocol.js";
import type {
  AuditOrigin,
  AuditOutcome,
  AuditRecordInput,
  AuditRecordResponse,
  AuditTrailInput,
  AuditTrailResponse,
  ConnectAuditEvent,
  ConnectPublishAuditEventsResponse,
  ConnectPublishCdcEventsResponse,
  NormalizedActorRef,
  PublishedEventResult,
  RequestOptions,
} from "./types.js";

export interface AuditClientOptions {
  organizationId?: string;
  defaultConnectorId?: string;
}

const DEFAULT_AUDIT_TRAIL_LIMIT = 100;
const MAX_AUDIT_TRAIL_LIMIT = 500;

export class AuditClient {
  constructor(
    private readonly connect: ProtocolConnectClient,
    private readonly options: AuditClientOptions = {},
  ) {}

  async record(
    input: AuditRecordInput,
    options: RequestOptions = {},
  ): Promise<AuditRecordResponse> {
    const connectorId = input.connectorId ?? this.options.defaultConnectorId;
    if (connectorId === undefined || connectorId.trim().length === 0) {
      throw new MedallionError(
        "connectorId is required to record an audit event. Pass defaultConnectorId to MedallionClient or audit.record().",
        { code: "MEDALLION_MISSING_CONNECTOR_ID" },
      );
    }

    const resource = normalizeResourceRef(input.resource);
    const actor = normalizeActorRef(input.actor);
    const key = requiredIdempotencyKey(
      input.idempotencyKey,
      "audit.idempotencyKey",
      512,
    );
    const event: ConnectAuditEvent = compactRecord({
      resource_type: resource.type,
      resource_id: resource.id,
      action: input.action,
      outcome: auditOutcome(input.outcome),
      idempotency_key: key,
      actor_principal: actorPrincipalFromRef(actor),
      payload_json: jsonString(
        {
          actor,
          resource,
          before: input.before ?? null,
          after: input.after ?? null,
          metadata: input.metadata ?? null,
          evidenceUrl: input.evidenceUrl ?? null,
        },
        "audit payload",
      ),
      occurred_at: occurredAt(input.occurredAt),
      description: input.description,
      source_event_id: optionalId(input.sourceEventId, "audit.sourceEventId"),
    });

    const response = await this.connect.publishAuditEvents(
      {
        connector_id: connectorId,
        events: [event],
      },
      options,
    );

    return eventResponse(response.body, key, response.requestId);
  }

  async trail(
    input: AuditTrailInput,
    options: RequestOptions = {},
  ): Promise<AuditTrailResponse> {
    const organizationId = input.organizationId ?? this.options.organizationId;
    if (organizationId === undefined || organizationId.trim().length === 0) {
      throw new MedallionError(
        "organizationId is required to read an audit trail from Connect. Pass organizationId to MedallionClient or audit.trail().",
        { code: "MEDALLION_MISSING_ORGANIZATION_ID" },
      );
    }

    const sourceActor =
      input.actor === undefined ? undefined : normalizeActorRef(input.actor);
    const resource = normalizeResourceRef({
      type: input.resourceType,
      id: input.resourceId,
    });
    const limit = auditTrailLimit(input.limit ?? input.pageSize);
    const response = await this.connect.listAuditEvents(
      {
        organization_id: organizationId,
        connector_id: input.connectorId ?? this.options.defaultConnectorId,
        resource_type: resource.type,
        resource_id: resource.id,
        limit,
        actor_principal:
          sourceActor === undefined
            ? undefined
            : actorPrincipalFromRef(sourceActor),
        ingested_by_principal: optionalString(input.ingesterPrincipal),
        action: input.action,
        origin:
          input.origin === undefined ? undefined : auditOrigin(input.origin),
        outcome:
          input.outcome === undefined ? undefined : auditOutcome(input.outcome),
        page_cursor: input.cursor,
      },
      options,
    );
    const events = (response.body.events ?? [])
      .map((event) => auditTrailEventFromConnect(event))
      .filter(
        (event) =>
          sourceActor === undefined || sameActorRef(event.actor, sourceActor),
      );

    return {
      requestId: response.requestId,
      nextCursor: response.body.next_page_cursor,
      events,
    };
  }
}

export function eventResponse(
  body: ConnectPublishCdcEventsResponse | ConnectPublishAuditEventsResponse,
  fallbackIdempotencyKey: string,
  requestId?: string,
): AuditRecordResponse {
  const acceptedCount = body.accepted_count ?? 0;
  const duplicateCount = body.duplicate_count ?? 0;
  const events: PublishedEventResult[] = (body.events ?? []).map((event) => ({
    idempotencyKey: event.idempotency_key ?? fallbackIdempotencyKey,
    eventId: event.event_id === undefined ? undefined : String(event.event_id),
    duplicate: event.duplicate ?? false,
  }));
  if (events.length === 0 && acceptedCount === 0 && duplicateCount === 0) {
    throw new MedallionError(
      "Medallion returned an empty event publish acknowledgement.",
      {
        code: "MEDALLION_INVALID_PUBLISH_RESPONSE",
        requestId,
      },
    );
  }
  const first = events[0];
  const duplicate =
    first?.duplicate ?? (duplicateCount > 0 && acceptedCount === 0);

  return {
    requestId,
    idempotencyKey: first?.idempotencyKey ?? fallbackIdempotencyKey,
    duplicate,
    result: duplicate ? "duplicate" : "accepted",
    acceptedCount,
    duplicateCount,
    events,
  };
}

function auditTrailLimit(value: number | undefined): number {
  if (value === undefined || value === 0) {
    return DEFAULT_AUDIT_TRAIL_LIMIT;
  }

  if (!Number.isInteger(value) || value <= 0) {
    throw new MedallionError(
      "audit.trail limit/pageSize must be a positive integer.",
      { code: "MEDALLION_INVALID_AUDIT_TRAIL_LIMIT" },
    );
  }

  if (value > MAX_AUDIT_TRAIL_LIMIT) {
    throw new MedallionError(
      `audit.trail limit/pageSize must be ${MAX_AUDIT_TRAIL_LIMIT} or less. Use cursor pagination for larger reads.`,
      { code: "MEDALLION_AUDIT_TRAIL_LIMIT_TOO_LARGE" },
    );
  }

  return value;
}

function auditTrailEventFromConnect(event: ConnectAuditEvent) {
  const payload = parseEventPayload(event.payload_json);
  const payloadRecord = recordValue(payload);
  const wireActor = actorFromPrincipal(event.actor_principal);
  const payloadActor = actorFromPayload(payloadRecord?.actor);
  const actor =
    payloadActor !== undefined &&
    actorPrincipalFromRef(payloadActor) === event.actor_principal
      ? payloadActor
      : wireActor;
  const before = payloadRecord?.before;
  const after = payloadRecord?.after;
  const metadata = recordValue(payloadRecord?.metadata);
  const evidenceUrl = stringValue(payloadRecord?.evidenceUrl);
  const eventId = event.id === undefined ? undefined : String(event.id);

  return {
    id: eventId,
    eventId,
    organizationId: event.organization_id,
    connectorId: event.connector_id,
    actor,
    ingesterPrincipal: event.ingested_by_principal,
    actorPrincipal: event.actor_principal,
    action: event.action,
    targetType: event.resource_type,
    targetId: event.resource_id,
    entityType: event.resource_type,
    entityId: event.resource_id,
    metadata,
    createdAt: event.observed_at,
    occurredAt: event.occurred_at,
    observedAt: event.observed_at,
    before,
    after,
    evidenceUrl,
    sourceEventId: event.source_event_id,
    sourceSystem: event.source_system,
    origin: auditOriginFromConnect(event.origin),
    outcome: auditOutcomeFromConnect(event.outcome),
    payload,
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

function auditOutcomeFromConnect(
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

function auditOriginFromConnect(
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

function actorFromPrincipal(
  value: string | undefined,
): NormalizedActorRef | undefined {
  if (value === undefined || value.length === 0) {
    return undefined;
  }

  const parts = value.split(":");
  if (parts.length < 2) {
    return { id: value };
  }

  const id = parts.pop();
  if (id === undefined) {
    return { id: value };
  }
  const type = parts.shift();
  const provider = parts.length > 0 ? parts.join(":") : undefined;
  return { id, type, provider };
}

function parseEventPayload(payload: string): unknown {
  try {
    return JSON.parse(payload) as unknown;
  } catch {
    return undefined;
  }
}

function recordValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function actorFromPayload(value: unknown): NormalizedActorRef | undefined {
  const record = recordValue(value);
  if (record === undefined) {
    return undefined;
  }

  const id = record.id;
  if (
    typeof id !== "string" &&
    typeof id !== "number" &&
    typeof id !== "bigint"
  ) {
    return undefined;
  }

  try {
    return normalizeActorRef({
      id,
      type: typeof record.type === "string" ? record.type : undefined,
      provider:
        typeof record.provider === "string" ? record.provider : undefined,
    });
  } catch {
    return undefined;
  }
}

function sameActorRef(
  left: NormalizedActorRef | undefined,
  right: NormalizedActorRef,
): boolean {
  return (
    left?.id === right.id &&
    left.type === right.type &&
    left.provider === right.provider
  );
}

function optionalString(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

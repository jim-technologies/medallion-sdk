import { MedallionError } from "./errors.js";
import type { ProtocolConnectClient } from "./protocol.js";
import {
  actorPrincipalFromRef,
  normalizeActorRef,
  normalizeResourceRef,
} from "./ids.js";
import {
  compactRecord,
  idempotencyKey,
  jsonString,
  occurredAt,
  optionalId,
  requiredId,
} from "./payload.js";
import type {
  AuditRecordInput,
  AuditTrailInput,
  AuditTrailResponse,
  ConnectCdcEvent,
  ConnectPublishCdcEventsResponse,
  EventRecordResponse,
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
  ): Promise<EventRecordResponse> {
    const connectorId = input.connectorId ?? this.options.defaultConnectorId;
    if (connectorId === undefined || connectorId.trim().length === 0) {
      throw new MedallionError(
        "connectorId is required to record an audit event. Pass defaultConnectorId to MedallionClient or audit.record().",
        { code: "MEDALLION_MISSING_CONNECTOR_ID" },
      );
    }

    const resource = normalizeResourceRef(input.resource);
    const actor = normalizeActorRef(input.actor);
    const key = idempotencyKey(input.idempotencyKey);
    const event: ConnectCdcEvent = compactRecord({
      stream_name: input.streamName ?? "audit_log",
      entity_type: resource.type,
      entity_id: resource.id,
      idempotency_key: key,
      actor_principal: actorPrincipalFromRef(actor),
      payload_json: jsonString(
        {
          actor,
          resource,
          before: input.before,
          after: input.after,
          metadata: input.metadata,
          evidenceUrl: input.evidenceUrl,
        },
        "audit payload",
      ),
      occurred_at: occurredAt(input.occurredAt),
      description: input.description,
      source_event_id: optionalId(input.sourceEventId, "audit.sourceEventId"),
      kind: "EVENT_KIND_AUDIT",
      action: input.action,
    });

    const response = await this.connect.publishCdcEvents(
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
    const limit = auditTrailLimit(input.limit ?? input.pageSize);
    const response = await this.connect.listCdcEvents(
      {
        organization_id: organizationId,
        connector_id: input.connectorId ?? this.options.defaultConnectorId,
        entity_type: input.resourceType,
        entity_id: requiredId(input.resourceId, "audit.resourceId"),
        limit,
        kind: "EVENT_KIND_AUDIT",
        actor_principal:
          sourceActor === undefined
            ? undefined
            : actorPrincipalFromRef(sourceActor),
        ingested_by_principal: optionalString(input.ingesterPrincipal),
        action: input.action,
        page_cursor: input.cursor,
      },
      options,
    );
    const events = (response.body.events ?? [])
      .filter((event) => isAuditEvent(event))
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
  body: ConnectPublishCdcEventsResponse,
  fallbackIdempotencyKey: string,
  requestId?: string,
): EventRecordResponse {
  const events: PublishedEventResult[] = (body.events ?? []).map((event) => ({
    idempotencyKey: event.idempotency_key ?? fallbackIdempotencyKey,
    eventId:
      event.event_id === undefined ? undefined : String(event.event_id),
    duplicate: event.duplicate ?? false,
  }));
  const first = events[0];
  const duplicate =
    first?.duplicate ?? ((body.duplicate_count ?? 0) > 0 && (body.accepted_count ?? 0) === 0);

  return {
    requestId,
    idempotencyKey: first?.idempotencyKey ?? fallbackIdempotencyKey,
    duplicate,
    result: duplicate ? "duplicate" : "accepted",
    acceptedCount: body.accepted_count ?? 0,
    duplicateCount: body.duplicate_count ?? 0,
    events,
  };
}

function auditTrailLimit(value: number | undefined): number {
  if (value === undefined) {
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

function isAuditEvent(event: ConnectCdcEvent): boolean {
  return event.kind === "EVENT_KIND_AUDIT" || event.action !== undefined;
}

function auditTrailEventFromConnect(event: ConnectCdcEvent) {
  const payload = parseEventPayload(event.payload_json);
  const payloadRecord = recordValue(payload);
  const actor =
    actorFromPayload(payloadRecord?.actor) ??
    actorFromPrincipal(event.actor_principal);
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
    targetType: event.entity_type,
    targetId: event.entity_id,
    entityType: event.entity_type,
    entityId: event.entity_id,
    metadata,
    createdAt: event.observed_at,
    occurredAt: event.occurred_at,
    observedAt: event.observed_at,
    before,
    after,
    evidenceUrl,
    sourceEventId: event.source_event_id,
    payload,
  };
}

function actorFromPrincipal(value: string | undefined): NormalizedActorRef | undefined {
  if (value === undefined || value.length === 0) {
    return undefined;
  }

  const parts = value.split(":");
  if (parts.length < 2) {
    return { id: value };
  }

  const id = parts.pop()!;
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

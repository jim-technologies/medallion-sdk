import { MedallionError } from "./errors.js";
import { actorPrincipalFromRef, normalizeActorRef } from "./ids.js";
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
  CdcBatchInput,
  CdcEventInput,
  CdcIngestionEventInput,
  CdcListInput,
  CdcOperation,
  CdcPage,
  CdcReadEvent,
  CdcRecordInput,
  ConnectCdcEvent,
  ConnectCdcEventInput,
  EventBatchResponse,
  EventRecordResponse,
  RequestOptions,
} from "./types.js";

export interface CdcClientOptions {
  workspaceId: string;
  defaultConnectorId?: string;
}

export class CdcClient {
  readonly #connect: ProtocolConnectClient;
  readonly #defaultConnectorId?: string;
  readonly #workspaceId: string;

  constructor(connect: ProtocolConnectClient, options: CdcClientOptions) {
    this.#connect = connect;
    this.#defaultConnectorId = options.defaultConnectorId;
    this.#workspaceId = options.workspaceId;
  }

  async record(
    input: CdcEventInput | CdcRecordInput,
    options: RequestOptions = {},
  ): Promise<EventRecordResponse> {
    const connectorId = requiredConnectorId(
      input.connectorId ?? this.#defaultConnectorId,
      "publish a CDC event",
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
    input: CdcBatchInput,
    options: RequestOptions = {},
  ): Promise<EventBatchResponse> {
    validateBatchSize(input.events);
    const connectorId = requiredConnectorId(
      input.connectorId ?? this.#defaultConnectorId,
      "publish CDC events",
    );
    const events = input.events.map((event) => modernEventToWire(event));
    return this.publishWireEvents(connectorId, events, options);
  }

  async list(
    input: CdcListInput = {},
    options: RequestOptions = {},
  ): Promise<CdcPage> {
    const actor =
      input.actor === undefined ? undefined : normalizeActorRef(input.actor);
    const response = await this.#connect.listCdcEvents(
      compactRecord({
        workspace_id: this.#workspaceId,
        connector_id: optionalText(
          input.connectorId ?? this.#defaultConnectorId,
          "cdc.connectorId",
          128,
        ),
        entity_type: optionalText(input.entityType, "cdc.entityType", 256),
        entity_id: optionalId(input.entityId, "cdc.entityId", 1024),
        limit: ingestionPageSize(input.limit),
        actor_principal:
          actor === undefined
            ? undefined
            : requiredText(
                actorPrincipalFromRef(actor),
                "cdc.actorPrincipal",
                512,
              ),
        ingested_by_principal: optionalText(
          input.ingesterPrincipal,
          "cdc.ingesterPrincipal",
          512,
        ),
        occurred_at_from: occurredAt(input.occurredAtFrom),
        occurred_at_to: occurredAt(input.occurredAtTo),
        source_system: optionalText(
          input.sourceSystem,
          "cdc.sourceSystem",
          256,
        ),
        stream_name: optionalText(input.streamName, "cdc.streamName", 256),
        page_cursor: optionalText(input.cursor, "cdc.cursor", 2048),
      }),
      options,
    );
    return {
      requestId: response.requestId,
      nextCursor: optionalListText(
        response.body.next_page_cursor,
        "cdc.list.nextCursor",
        2_048,
        response.requestId,
      ),
      events: (response.body.events ?? []).map((event) =>
        cdcEventFromWire(event, this.#workspaceId, response.requestId),
      ),
    };
  }

  async *iterate(
    input: CdcListInput = {},
    options: RequestOptions = {},
  ): AsyncGenerator<CdcReadEvent, void, undefined> {
    const filters: CdcListInput = {
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
    events: ConnectCdcEventInput[],
    options: RequestOptions,
  ): Promise<EventBatchResponse> {
    const expectedKeys = events.map((event) => event.idempotency_key);
    const response = await this.#connect.publishCdcEvents(
      { connector_id: connectorId, events },
      options,
    );
    return batchResponse(response.body, expectedKeys, response.requestId);
  }
}

function modernEventToWire(
  input: CdcIngestionEventInput,
): ConnectCdcEventInput {
  rejectServerOwnedInput(input);
  const actor =
    input.actor === undefined ? undefined : normalizeActorRef(input.actor);
  return compactRecord({
    stream_name: requiredText(input.streamName, "cdc.streamName", 256),
    entity_type: requiredText(input.entityType, "cdc.entityType", 256),
    entity_id: requiredId(input.entityId, "cdc.entityId", 1024),
    operation: cdcOperation(input.operation),
    source_event_id: optionalId(input.sourceEventId, "cdc.sourceEventId", 1024),
    idempotency_key: requiredIdempotencyKey(
      input.idempotencyKey,
      "cdc.idempotencyKey",
      512,
    ),
    actor_principal:
      actor === undefined
        ? undefined
        : requiredText(actorPrincipalFromRef(actor), "cdc.actorPrincipal", 512),
    payload_json: rawOrJsonString(
      input.payload,
      input.payloadJson,
      "cdc.payload",
    ),
    occurred_at: occurredAt(input.occurredAt),
    description: optionalText(input.description, "cdc.description", 4096),
  });
}

function legacyEventToWire(input: CdcEventInput): ConnectCdcEventInput {
  rejectServerOwnedInput(input);
  const primaryKey = normalizePrimaryKey(input.primaryKey);
  if (Object.keys(primaryKey).length === 0) {
    throw new MedallionError(
      "cdc.primaryKey must contain at least one field.",
      {
        code: "MEDALLION_EMPTY_CDC_PRIMARY_KEY",
      },
    );
  }
  if (Object.keys(primaryKey).length > 1 && input.entityId === undefined) {
    throw new MedallionError(
      "cdc.entityId is required when cdc.primaryKey contains more than one field.",
      { code: "MEDALLION_MISSING_CDC_ENTITY_ID" },
    );
  }
  const entityId =
    input.entityId === undefined
      ? requiredText(Object.values(primaryKey)[0], "cdc.entityId", 1024)
      : requiredId(input.entityId, "cdc.entityId", 1024);
  const actor =
    input.actor === undefined ? undefined : normalizeActorRef(input.actor);
  const compatibilityPayload = {
    source: input.source,
    table: input.table,
    actor: actor ?? null,
    primaryKey,
    before: input.before ?? null,
    after: input.after ?? null,
    metadata: input.metadata ?? null,
  };
  return compactRecord({
    stream_name: requiredText(input.table, "cdc.table", 256),
    entity_type: requiredText(
      input.entityType ?? input.table,
      "cdc.entityType",
      256,
    ),
    entity_id: entityId,
    operation: cdcOperation(input.operation),
    source_event_id: optionalId(input.sourceEventId, "cdc.sourceEventId", 1024),
    idempotency_key: requiredIdempotencyKey(
      input.idempotencyKey,
      "cdc.idempotencyKey",
      512,
    ),
    actor_principal:
      actor === undefined
        ? undefined
        : requiredText(actorPrincipalFromRef(actor), "cdc.actorPrincipal", 512),
    payload_json: rawOrJsonString(
      input.payload === undefined ? compatibilityPayload : input.payload,
      input.payloadJson,
      "cdc.payload",
    ),
    occurred_at: occurredAt(input.occurredAt),
    description: optionalText(input.description, "cdc.description", 4096),
  });
}

export function cdcEventFromWire(
  event: ConnectCdcEvent,
  expectedWorkspaceId: string,
  requestId?: string,
): CdcReadEvent {
  const eventId = requiredListEventId(event.id, requestId);
  const streamName = requiredListText(
    event.stream_name,
    "cdc.list.events[].streamName",
    256,
    requestId,
  );
  const entityType = requiredListText(
    event.entity_type,
    "cdc.list.events[].entityType",
    256,
    requestId,
  );
  const entityId = requiredListText(
    event.entity_id,
    "cdc.list.events[].entityId",
    1_024,
    requestId,
  );
  const idempotencyKey = requiredListIdempotencyKey(
    event.idempotency_key,
    "cdc.list.events[].idempotencyKey",
    requestId,
  );
  const workspaceId = requiredResponseWorkspace(
    event.workspace_id,
    expectedWorkspaceId,
    "cdc.list.events[].workspaceId",
    requestId,
  );
  const connectorId = optionalListText(
    event.connector_id,
    "cdc.list.events[].connectorId",
    128,
    requestId,
  );
  const sourceEventId = optionalListText(
    event.source_event_id,
    "cdc.list.events[].sourceEventId",
    1_024,
    requestId,
  );
  const actorPrincipal = optionalListText(
    event.actor_principal,
    "cdc.list.events[].actorPrincipal",
    512,
    requestId,
  );
  const description = optionalListText(
    event.description,
    "cdc.list.events[].description",
    4_096,
    requestId,
  );
  const sourceSystem = optionalListText(
    event.source_system,
    "cdc.list.events[].sourceSystem",
    256,
    requestId,
  );
  const ingesterPrincipal = optionalListText(
    event.ingested_by_principal,
    "cdc.list.events[].ingestedByPrincipal",
    512,
    requestId,
  );
  const payload = parseListJsonPayload(
    event.payload_json,
    "cdc.list.events[].payloadJson",
    requestId,
  );
  const operation = cdcOperationFromWire(event.operation);
  if (operation === undefined) {
    throw invalidListResponse(
      "Medallion returned a listed CDC event without a concrete operation.",
      requestId,
    );
  }
  const occurredAt = optionalListTimestamp(
    event.occurred_at,
    "cdc.list.events[].occurredAt",
    requestId,
  );
  const observedAt = optionalListTimestamp(
    event.observed_at,
    "cdc.list.events[].observedAt",
    requestId,
  );
  return {
    id: eventId,
    eventId,
    workspaceId,
    connectorId,
    streamName,
    entityType,
    entityId,
    operation,
    sourceEventId,
    idempotencyKey,
    actor: actorFromEventPayload(payload, actorPrincipal),
    actorPrincipal,
    payload,
    payloadJson: event.payload_json,
    occurredAt,
    observedAt,
    description,
    sourceSystem,
    ingesterPrincipal,
  };
}

function cdcOperation(operation: CdcOperation): string {
  switch (operation) {
    case "insert":
      return "CDC_OPERATION_INSERT";
    case "update":
      return "CDC_OPERATION_UPDATE";
    case "delete":
      return "CDC_OPERATION_DELETE";
    case "snapshot":
      return "CDC_OPERATION_SNAPSHOT";
    default:
      throw new MedallionError(
        "operation must be insert, update, delete, or snapshot.",
        { code: "MEDALLION_INVALID_CDC_OPERATION" },
      );
  }
}

function cdcOperationFromWire(
  value: string | undefined,
): CdcOperation | undefined {
  switch (value) {
    case "CDC_OPERATION_INSERT":
      return "insert";
    case "CDC_OPERATION_UPDATE":
      return "update";
    case "CDC_OPERATION_DELETE":
      return "delete";
    case "CDC_OPERATION_SNAPSHOT":
      return "snapshot";
    default:
      return undefined;
  }
}

function isModernEvent(
  input: CdcEventInput | CdcIngestionEventInput,
): input is CdcIngestionEventInput {
  return "streamName" in input;
}

function normalizePrimaryKey(
  value: Record<string, string | number | bigint>,
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [
      key,
      requiredId(item, `primaryKey.${key}`),
    ]),
  );
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
    "workspace_id",
    "connector_id",
    "observed_at",
    "source_system",
    "ingested_by_principal",
  ];
  if (fields.some((field) => Object.hasOwn(record, field))) {
    throw new MedallionError(
      "CDC events must not contain server-owned fields.",
      { code: "MEDALLION_SERVER_DERIVED_FIELD" },
    );
  }
}

import { MedallionError } from "./errors.js";
import type { ProtocolConnectClient } from "./protocol.js";
import { actorPrincipalFromRef, normalizeActorRef, normalizeIdRecord } from "./ids.js";
import {
  compactRecord,
  idempotencyKey,
  jsonString,
  occurredAt,
  optionalId,
  requiredId,
} from "./payload.js";
import { eventResponse } from "./audit.js";
import type {
  CdcEventInput,
  CdcOperation,
  ConnectCdcEvent,
  EventRecordResponse,
  RequestOptions,
} from "./types.js";

export interface CdcClientOptions {
  defaultConnectorId?: string;
}

export class CdcClient {
  constructor(
    private readonly connect: ProtocolConnectClient,
    private readonly options: CdcClientOptions = {},
  ) {}

  async record(
    input: CdcEventInput,
    options: RequestOptions = {},
  ): Promise<EventRecordResponse> {
    const connectorId = input.connectorId ?? this.options.defaultConnectorId;
    if (connectorId === undefined || connectorId.trim().length === 0) {
      throw new MedallionError(
        "connectorId is required to record a CDC event. Pass defaultConnectorId to MedallionClient or cdc.record().",
        { code: "MEDALLION_MISSING_CONNECTOR_ID" },
      );
    }

    const primaryKey = normalizeIdRecord(input.primaryKey);
    if (Object.keys(primaryKey).length === 0) {
      throw new MedallionError("cdc.primaryKey must contain at least one field.", {
        code: "MEDALLION_EMPTY_CDC_PRIMARY_KEY",
      });
    }
    const entityId =
      input.entityId === undefined
        ? entityIdFromPrimaryKey(primaryKey)
        : requiredId(input.entityId, "cdc.entityId");
    const actor =
      input.actor === undefined ? undefined : normalizeActorRef(input.actor);
    const key = idempotencyKey(input.idempotencyKey);
    const event: ConnectCdcEvent = compactRecord({
      stream_name: input.table,
      entity_type: input.entityType ?? input.table,
      entity_id: entityId,
      operation: cdcOperation(input.operation),
      source_event_id: optionalId(input.sourceEventId, "cdc.sourceEventId"),
      idempotency_key: key,
      actor_principal:
        actor === undefined ? undefined : actorPrincipalFromRef(actor),
      payload_json: jsonString(
        {
          source: input.source,
          table: input.table,
          actor: actor ?? null,
          primaryKey,
          before: input.before ?? null,
          after: input.after ?? null,
          metadata: input.metadata ?? null,
        },
        "cdc payload",
      ),
      occurred_at: occurredAt(input.occurredAt),
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
  }
}

function entityIdFromPrimaryKey(primaryKey: Record<string, string>): string {
  const keys = Object.keys(primaryKey).sort();
  if (keys.length === 1) {
    return primaryKey[keys[0]!]!;
  }

  return `{${keys
    .map((key) => `${JSON.stringify(key)}:${JSON.stringify(primaryKey[key]!)}`)
    .join(",")}}`;
}

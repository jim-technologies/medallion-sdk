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
} from "./payload.js";
import { eventResponse } from "./audit.js";
import type {
  ConnectCdcEvent,
  EventRecordResponse,
  GenericEventInput,
  RequestOptions,
} from "./types.js";

export interface EventsClientOptions {
  defaultConnectorId?: string;
}

export class EventsClient {
  constructor(
    private readonly connect: ProtocolConnectClient,
    private readonly options: EventsClientOptions = {},
  ) {}

  async record(
    input: GenericEventInput,
    options: RequestOptions = {},
  ): Promise<EventRecordResponse> {
    const connectorId = input.connectorId ?? this.options.defaultConnectorId;
    if (connectorId === undefined || connectorId.trim().length === 0) {
      throw new MedallionError(
        "connectorId is required to record an event. Pass defaultConnectorId to MedallionClient or events.record().",
        { code: "MEDALLION_MISSING_CONNECTOR_ID" },
      );
    }

    const resource =
      input.resource === undefined
        ? { type: "event", id: input.idempotencyKey ?? input.type }
        : normalizeResourceRef(input.resource);
    const actor =
      input.actor === undefined ? undefined : normalizeActorRef(input.actor);
    const key = idempotencyKey(input.idempotencyKey);
    const event: ConnectCdcEvent = compactRecord({
      stream_name: input.streamName ?? "events",
      entity_type: resource.type,
      entity_id: resource.id,
      idempotency_key: key,
      actor_principal:
        actor === undefined ? undefined : actorPrincipalFromRef(actor),
      payload_json: jsonString(
        {
          type: input.type,
          actor,
          resource,
          payload: input.payload,
          metadata: input.metadata,
        },
        "event payload",
      ),
      occurred_at: occurredAt(input.occurredAt),
      source_event_id: optionalId(input.sourceEventId, "event.sourceEventId"),
      kind: "EVENT_KIND_AUDIT",
      action: input.type,
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

import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { MedallionError } from "./errors.js";
import {
  actorPrincipalFromRef,
  normalizeActorRef,
  normalizeId,
} from "./ids.js";
import { parseRfc3339Instant, requiredIdempotencyKey } from "./payload.js";
import type {
  ConnectPublishAuditEventsResponse,
  ConnectPublishCdcEventsResponse,
  EventBatchResponse,
  EventRecordResponse,
  NormalizedActorRef,
} from "./types.js";

export const DEFAULT_INGESTION_PAGE_SIZE = 100;
export const MAX_INGESTION_PAGE_SIZE = 500;
export const MAX_INGESTION_BATCH_SIZE = 1_000;
export const MAX_INGESTION_ITERATOR_PAGES = 10_000;
export const MAX_CONNECTOR_ID_BYTES = 128;
const MIN_ANALYTICAL_TIMESTAMP_MS = Date.parse("1900-01-01T00:00:00Z");
const MAX_ANALYTICAL_TIMESTAMP_SECONDS = Math.floor(
  Date.parse("2262-04-11T23:47:16Z") / 1_000,
);
const MAX_ANALYTICAL_TIMESTAMP_NANOS = 854_775_807;
export function requiredConnectorId(
  value: string | undefined,
  operation: string,
): string {
  const connectorId = value?.trim();
  if (connectorId === undefined || connectorId.length === 0) {
    throw new MedallionError(
      `connectorId is required to ${operation}. Pass defaultConnectorId to MedallionClient or provide it for this call.`,
      { code: "MEDALLION_MISSING_CONNECTOR_ID" },
    );
  }
  return boundedText(
    connectorId,
    "connectorId",
    MAX_CONNECTOR_ID_BYTES,
    "MEDALLION_INVALID_CONNECTOR_ID",
  );
}

export function ingestionPageSize(value: number | undefined): number {
  if (value === undefined || value === 0) return DEFAULT_INGESTION_PAGE_SIZE;
  if (
    !Number.isInteger(value) ||
    value < 1 ||
    value > MAX_INGESTION_PAGE_SIZE
  ) {
    throw new MedallionError(
      `limit must be an integer from 1 through ${MAX_INGESTION_PAGE_SIZE}.`,
      { code: "MEDALLION_INVALID_PAGE_SIZE" },
    );
  }
  return value;
}

export function validateBatchSize(events: readonly unknown[]): void {
  if (events.length < 1 || events.length > MAX_INGESTION_BATCH_SIZE) {
    throw new MedallionError(
      `An ingestion batch must contain between 1 and ${MAX_INGESTION_BATCH_SIZE} events.`,
      { code: "MEDALLION_INVALID_BATCH_SIZE" },
    );
  }
}

export function batchResponse(
  body: ConnectPublishCdcEventsResponse | ConnectPublishAuditEventsResponse,
  expectedKeys: readonly string[],
  requestId?: string,
): EventBatchResponse {
  const receipts = body.events ?? [];
  if (receipts.length !== expectedKeys.length) {
    throw invalidPublishResponse(
      `Medallion returned ${receipts.length} receipts for ${expectedKeys.length} submitted events.`,
      requestId,
    );
  }
  const events = receipts.map((receipt, index) => {
    const expectedKey = expectedKeys[index];
    if (expectedKey === undefined) {
      throw invalidPublishResponse(
        "Medallion returned an unexpected publish receipt.",
        requestId,
      );
    }
    const idempotencyKey = receipt.idempotency_key;
    if (idempotencyKey !== expectedKey) {
      throw invalidPublishResponse(
        "Medallion returned a receipt for a different idempotency key or ordering.",
        requestId,
      );
    }
    const eventId = losslessInteger(receipt.event_id, requestId);
    return {
      idempotencyKey,
      eventId,
      duplicate: receipt.duplicate ?? false,
    };
  });
  const acceptedCount = events.filter((item) => !item.duplicate).length;
  const duplicateCount = events.length - acceptedCount;
  if (
    (body.accepted_count ?? 0) !== acceptedCount ||
    (body.duplicate_count ?? 0) !== duplicateCount
  ) {
    throw invalidPublishResponse(
      "Medallion returned publish counts that do not match its per-event receipts.",
      requestId,
    );
  }
  const result =
    acceptedCount > 0 && duplicateCount > 0
      ? "mixed"
      : duplicateCount > 0
        ? "duplicate"
        : "accepted";
  return {
    requestId,
    duplicate: acceptedCount === 0,
    result,
    acceptedCount,
    duplicateCount,
    events,
  };
}

export function singleEventResponse(
  response: EventBatchResponse,
): EventRecordResponse {
  const receipt = response.events[0];
  if (receipt === undefined) {
    throw invalidPublishResponse(
      "Medallion returned an empty event publish acknowledgement.",
      response.requestId,
    );
  }
  return {
    ...response,
    idempotencyKey: receipt.idempotencyKey,
    duplicate: receipt.duplicate,
    result: receipt.duplicate ? "duplicate" : "accepted",
  };
}

export function losslessInteger(
  value: string | number | undefined,
  requestId?: string,
): string {
  if (value === undefined) {
    throw invalidPublishResponse(
      "Medallion returned a publish receipt without a durable event ID.",
      requestId,
    );
  }
  if (typeof value === "string") {
    if (!/^\d+$/.test(value) || BigInt(value) <= 0n) {
      throw invalidPublishResponse(
        "Medallion returned a non-positive or malformed 64-bit event ID.",
        requestId,
      );
    }
    return value;
  }
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw invalidPublishResponse(
      "Medallion returned a non-positive or unsafe numeric 64-bit event ID; the JSON codec must return large IDs as strings.",
      requestId,
    );
  }
  return String(value);
}

export function requiredListEventId(
  value: string | number | undefined,
  requestId?: string,
): string {
  if (value === undefined) {
    throw invalidListResponse(
      "Medallion returned a listed event without a durable event ID.",
      requestId,
    );
  }
  if (typeof value === "string") {
    if (!/^\d+$/.test(value) || BigInt(value) <= 0n) {
      throw invalidListResponse(
        "Medallion returned a listed event with a non-positive or malformed 64-bit event ID.",
        requestId,
      );
    }
    return value;
  }
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw invalidListResponse(
      "Medallion returned a listed event with a non-positive or unsafe numeric 64-bit event ID.",
      requestId,
    );
  }
  return String(value);
}

export function requiredListText(
  value: unknown,
  path: string,
  maxBytes: number | undefined,
  requestId?: string,
): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw invalidListResponse(
      `${path} must be a non-empty string in the listed event.`,
      requestId,
    );
  }
  return boundedListText(value, path, maxBytes, requestId);
}

export function requiredListIdempotencyKey(
  value: unknown,
  path: string,
  requestId?: string,
): string {
  try {
    return requiredIdempotencyKey(value, path, 512);
  } catch {
    throw invalidListResponse(
      `${path} must be a non-empty valid Unicode string of at most 512 UTF-8 bytes.`,
      requestId,
    );
  }
}

export function requiredResponseWorkspace(
  value: unknown,
  expected: string,
  path: string,
  requestId?: string,
): string {
  const workspaceId = requiredListText(value, path, 29, requestId);
  if (!/^ws_[0-9a-hjkmnp-tv-z]{26}$/.test(workspaceId)) {
    throw invalidListResponse(
      `${path} is not a canonical workspace ID.`,
      requestId,
    );
  }
  if (workspaceId !== expected) {
    throw new MedallionError(
      "Medallion returned an event for a different workspace.",
      {
        code: "MEDALLION_WORKSPACE_SELECTOR_CONFLICT",
        requestId,
      },
    );
  }
  return workspaceId;
}

export function optionalListText(
  value: unknown,
  path: string,
  maxBytes: number,
  requestId?: string,
): string | undefined {
  if (value === undefined || value === "") return undefined;
  if (typeof value !== "string") {
    throw invalidListResponse(
      `${path} must be a string when present in the listed event.`,
      requestId,
    );
  }
  return boundedListText(value, path, maxBytes, requestId);
}

export function optionalListTimestamp(
  value: unknown,
  path: string,
  requestId?: string,
): string | undefined {
  if (value === undefined || value === "") return undefined;
  if (typeof value !== "string") {
    throw invalidListResponse(
      `${path} must be an RFC 3339 timestamp when present in the listed event.`,
      requestId,
    );
  }
  const instant = parseRfc3339Instant(value);
  if (instant === undefined) {
    throw invalidListResponse(
      `${path} must be an RFC 3339 timestamp when present in the listed event.`,
      requestId,
    );
  }
  if (
    instant.seconds < Math.floor(MIN_ANALYTICAL_TIMESTAMP_MS / 1_000) ||
    instant.seconds > MAX_ANALYTICAL_TIMESTAMP_SECONDS ||
    (instant.seconds === MAX_ANALYTICAL_TIMESTAMP_SECONDS &&
      instant.nanos > MAX_ANALYTICAL_TIMESTAMP_NANOS)
  ) {
    throw invalidListResponse(
      `${path} must be between 1900-01-01 and 2262-04-11T23:47:16.854775807Z.`,
      requestId,
    );
  }
  return value;
}

export function requiredText(
  value: unknown,
  path: string,
  maxBytes?: number,
): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new MedallionError(`${path} is required.`, {
      code: "MEDALLION_INVALID_EVENT",
    });
  }
  return boundedText(value, path, maxBytes, "MEDALLION_INVALID_EVENT");
}

export function optionalText(
  value: string | undefined,
  path: string,
  maxBytes: number,
): string | undefined {
  if (value === undefined || value.length === 0) return undefined;
  return boundedText(value, path, maxBytes, "MEDALLION_INVALID_EVENT");
}

export function actorFromPrincipal(
  value: string | undefined,
): NormalizedActorRef | undefined {
  if (value === undefined || value.length === 0) return undefined;
  // actor_principal is an opaque source identity. Colons may belong to an ID,
  // type, or provider, so reverse-parsing would invent structure. Preserve the
  // complete principal losslessly unless a matching structured payload exists.
  return { id: value };
}

export function parseListJsonPayload(
  value: unknown,
  path: string,
  requestId?: string,
): unknown {
  if (typeof value !== "string" || value.length === 0) {
    throw invalidListResponse(
      `${path} must contain exactly one valid JSON value.`,
      requestId,
    );
  }
  try {
    return JSON.parse(value) as unknown;
  } catch {
    throw invalidListResponse(
      `${path} must contain exactly one valid JSON value.`,
      requestId,
    );
  }
}

export function assertIteratorPageWithinLimit(page: number): void {
  if (page > MAX_INGESTION_ITERATOR_PAGES) {
    throw new MedallionError(
      `Medallion iteration exceeded ${MAX_INGESTION_ITERATOR_PAGES} pages. Resume explicitly from the last cursor.`,
      { code: "MEDALLION_PAGINATION_LIMIT" },
    );
  }
}

export function actorFromEventPayload(
  payload: unknown,
  principal: string | undefined,
): NormalizedActorRef | undefined {
  const fallback = actorFromPrincipal(principal);
  if (principal === undefined || principal.length === 0) return fallback;

  const payloadRecord = recordValue(payload);
  const actorRecord = recordValue(payloadRecord?.actor);
  if (actorRecord === undefined) return fallback;
  const id = actorRecord?.id;
  if (
    typeof id !== "string" &&
    typeof id !== "number" &&
    typeof id !== "bigint"
  ) {
    return fallback;
  }

  try {
    const actor = normalizeActorRef({
      id,
      type: typeof actorRecord.type === "string" ? actorRecord.type : undefined,
      provider:
        typeof actorRecord.provider === "string"
          ? actorRecord.provider
          : undefined,
    });
    return actorPrincipalFromRef(actor) === principal ? actor : fallback;
  } catch {
    return fallback;
  }
}

export function idempotencyKeyFromParts(
  namespace: string,
  ...sourceIdentity: readonly (string | number | bigint)[]
): string {
  const normalizedNamespace = namespace.trim();
  if (normalizedNamespace.length === 0 || sourceIdentity.length === 0) {
    throw new MedallionError(
      "A namespace and at least one stable source identity part are required.",
      {
        code: "MEDALLION_MISSING_IDEMPOTENCY_KEY",
      },
    );
  }
  const logicalIdentity = [
    normalizedNamespace,
    ...sourceIdentity.map((part, index) =>
      normalizeId(part, `sourceIdentity[${index}]`),
    ),
  ].join("\x1f");
  const namespaceUrl = Buffer.from("6ba7b8119dad11d180b400c04fd430c8", "hex");
  const digest = createHash("sha1")
    .update(namespaceUrl)
    .update(logicalIdentity, "utf8")
    .digest();
  digest[6] = ((digest[6] ?? 0) & 0x0f) | 0x50;
  digest[8] = ((digest[8] ?? 0) & 0x3f) | 0x80;
  const hex = digest.toString("hex");
  const uuid = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
  return requiredIdempotencyKey(
    `${normalizedNamespace}:${uuid}`,
    "idempotency key",
    512,
  );
}

export function repeatedCursor(): MedallionError {
  return new MedallionError(
    "Medallion returned a repeated non-empty cursor; iteration stopped to prevent an infinite loop.",
    { code: "MEDALLION_REPEATED_CURSOR" },
  );
}

function invalidPublishResponse(
  message: string,
  requestId?: string,
): MedallionError {
  return new MedallionError(message, {
    code: "MEDALLION_INVALID_PUBLISH_RESPONSE",
    requestId,
  });
}

export function invalidListResponse(
  message: string,
  requestId?: string,
): MedallionError {
  return new MedallionError(message, {
    code: "MEDALLION_INVALID_LIST_RESPONSE",
    requestId,
  });
}

function boundedListText(
  value: string,
  path: string,
  maxBytes: number | undefined,
  requestId?: string,
): string {
  if (
    maxBytes !== undefined &&
    new TextEncoder().encode(value).length > maxBytes
  ) {
    throw invalidListResponse(
      `${path} exceeds the ${maxBytes}-byte response contract limit.`,
      requestId,
    );
  }
  return value;
}

function boundedText(
  value: string,
  path: string,
  maxBytes: number | undefined,
  code: string,
): string {
  if (
    maxBytes !== undefined &&
    new TextEncoder().encode(value).length > maxBytes
  ) {
    throw new MedallionError(`${path} must not exceed ${maxBytes} bytes.`, {
      code,
    });
  }
  return value;
}

function recordValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

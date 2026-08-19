import { MedallionError } from "./errors.js";
import {
  parseRfc3339Instant,
  type Rfc3339Instant,
  requiredIdempotencyKey,
} from "./payload.js";

const MAX_BATCH_SIZE = 1_000;
const MAX_LIST_LIMIT = 500;
const MIN_TIMESTAMP_MS = Date.parse("1900-01-01T00:00:00Z");
const MAX_TIMESTAMP_SECONDS = Math.floor(
  Date.parse("2262-04-11T23:47:16Z") / 1_000,
);
const MAX_TIMESTAMP_NANOS = 854_775_807;

const CDC_OPERATIONS = new Set([
  "CDC_OPERATION_INSERT",
  "CDC_OPERATION_UPDATE",
  "CDC_OPERATION_DELETE",
  "CDC_OPERATION_SNAPSHOT",
]);
const AUDIT_OUTCOMES = new Set([
  "AUDIT_EVENT_OUTCOME_SUCCEEDED",
  "AUDIT_EVENT_OUTCOME_FAILED",
  "AUDIT_EVENT_OUTCOME_INDETERMINATE",
]);
const AUDIT_ORIGINS = new Set([
  "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
  "AUDIT_EVENT_ORIGIN_CONNECT",
]);

export function preflightConnectRequest(
  methodName: string,
  value: unknown,
): void {
  const request = requiredRecord(value, `${methodName} request`);
  switch (methodName) {
    case "PublishCdcEvents":
      preflightPublishCdc(request);
      return;
    case "PublishAuditEvents":
      preflightPublishAudit(request);
      return;
    case "ListCdcEvents":
      preflightListCdc(request);
      return;
    case "ListAuditEvents":
      preflightListAudit(request);
      return;
    default:
      throw new MedallionError("Unsupported Connect ingestion method.", {
        code: "MEDALLION_PROTOCOL_METHOD_NOT_FOUND",
      });
  }
}

function preflightPublishCdc(request: Record<string, unknown>): void {
  requiredSelector(
    request.connectorId,
    "cdc.connectorId",
    128,
    "MEDALLION_MISSING_CONNECTOR_ID",
  );
  const events = requiredBatch(request.events, "CDC");
  const keys = new Set<string>();
  for (const [index, value] of events.entries()) {
    const path = `cdc.events[${index}]`;
    const event = requiredRecord(value, path);
    rejectServerDerivedFields(event, path, [
      "id",
      "workspaceId",
      "connectorId",
      "observedAt",
      "sourceSystem",
      "ingestedByPrincipal",
    ]);
    requiredText(event.streamName, `${path}.streamName`, 256);
    requiredText(event.entityType, `${path}.entityType`, 256);
    requiredText(event.entityId, `${path}.entityId`, 1_024);
    optionalText(event.sourceEventId, `${path}.sourceEventId`, 1_024);
    optionalText(event.actorPrincipal, `${path}.actorPrincipal`, 512);
    optionalText(event.description, `${path}.description`, 4_096);
    if (!CDC_OPERATIONS.has(String(event.operation ?? ""))) {
      throw new MedallionError(
        `${path}.operation must be insert, update, delete, or snapshot.`,
        { code: "MEDALLION_INVALID_CDC_OPERATION" },
      );
    }
    const key = requiredIdempotencyKey(
      event.idempotencyKey,
      `${path}.idempotencyKey`,
      512,
    );
    if (keys.has(key)) {
      throw new MedallionError(
        `${path} repeats an idempotency key in the batch.`,
        { code: "MEDALLION_DUPLICATE_IDEMPOTENCY_KEY" },
      );
    }
    keys.add(key);
    validPayloadJson(event.payloadJson, `${path}.payloadJson`);
    optionalTimestamp(event.occurredAt, `${path}.occurredAt`);
  }
}

function preflightPublishAudit(request: Record<string, unknown>): void {
  requiredSelector(
    request.connectorId,
    "audit.connectorId",
    128,
    "MEDALLION_MISSING_CONNECTOR_ID",
  );
  const events = requiredBatch(request.events, "audit");
  const keys = new Set<string>();
  for (const [index, value] of events.entries()) {
    const path = `audit.events[${index}]`;
    const event = requiredRecord(value, path);
    rejectServerDerivedFields(event, path, [
      "id",
      "workspaceId",
      "connectorId",
      "observedAt",
      "sourceSystem",
      "ingestedByPrincipal",
      "origin",
    ]);
    requiredText(event.resourceType, `${path}.resourceType`, 256);
    requiredText(event.resourceId, `${path}.resourceId`, 1_024);
    requiredText(event.action, `${path}.action`, 256);
    optionalText(event.sourceEventId, `${path}.sourceEventId`, 1_024);
    optionalText(event.actorPrincipal, `${path}.actorPrincipal`, 512);
    optionalText(event.description, `${path}.description`, 4_096);
    if (!AUDIT_OUTCOMES.has(String(event.outcome ?? ""))) {
      throw new MedallionError(
        `${path}.outcome must be succeeded, failed, or indeterminate.`,
        { code: "MEDALLION_INVALID_AUDIT_OUTCOME" },
      );
    }
    const key = requiredIdempotencyKey(
      event.idempotencyKey,
      `${path}.idempotencyKey`,
      512,
    );
    if (keys.has(key)) {
      throw new MedallionError(
        `${path} repeats an idempotency key in the batch.`,
        { code: "MEDALLION_DUPLICATE_IDEMPOTENCY_KEY" },
      );
    }
    keys.add(key);
    validPayloadJson(event.payloadJson, `${path}.payloadJson`);
    optionalTimestamp(event.occurredAt, `${path}.occurredAt`);
  }
}

function preflightListCdc(request: Record<string, unknown>): void {
  requiredSelector(
    request.workspaceId,
    "cdc.list.workspaceId",
    29,
    "MEDALLION_MISSING_WORKSPACE_ID",
  );
  validateWorkspaceId(request.workspaceId, "cdc.list.workspaceId");
  optionalText(request.connectorId, "cdc.list.connectorId", 128);
  optionalText(request.entityType, "cdc.list.entityType", 256);
  optionalText(request.entityId, "cdc.list.entityId", 1_024);
  optionalText(request.actorPrincipal, "cdc.list.actorPrincipal", 512);
  optionalText(request.sourceSystem, "cdc.list.sourceSystem", 256);
  optionalText(request.streamName, "cdc.list.streamName", 256);
  optionalText(request.pageCursor, "cdc.list.pageCursor", 2_048);
  optionalText(
    request.ingestedByPrincipal,
    "cdc.list.ingestedByPrincipal",
    512,
  );
  listLimit(request.limit, "CDC");
  timestampBounds(request.occurredAtFrom, request.occurredAtTo, "cdc.list");
}

function preflightListAudit(request: Record<string, unknown>): void {
  requiredSelector(
    request.workspaceId,
    "audit.list.workspaceId",
    29,
    "MEDALLION_MISSING_WORKSPACE_ID",
  );
  validateWorkspaceId(request.workspaceId, "audit.list.workspaceId");
  optionalText(request.connectorId, "audit.list.connectorId", 128);
  optionalText(request.resourceType, "audit.list.resourceType", 256);
  optionalText(request.resourceId, "audit.list.resourceId", 1_024);
  optionalText(request.actorPrincipal, "audit.list.actorPrincipal", 512);
  optionalText(request.action, "audit.list.action", 256);
  optionalText(request.sourceSystem, "audit.list.sourceSystem", 256);
  optionalText(request.pageCursor, "audit.list.pageCursor", 2_048);
  optionalText(
    request.ingestedByPrincipal,
    "audit.list.ingestedByPrincipal",
    512,
  );
  const hasResourceType = nonEmptyString(request.resourceType);
  const hasResourceId = nonEmptyString(request.resourceId);
  if (hasResourceType !== hasResourceId) {
    throw new MedallionError(
      "audit resourceType and resourceId must be provided together.",
      { code: "MEDALLION_INVALID_AUDIT_FILTER" },
    );
  }
  listLimit(request.limit, "audit");
  optionalEnum(request.origin, AUDIT_ORIGINS, "audit.list.origin");
  optionalEnum(request.outcome, AUDIT_OUTCOMES, "audit.list.outcome");
  timestampBounds(request.occurredAtFrom, request.occurredAtTo, "audit.list");
}

function requiredBatch(value: unknown, kind: string): unknown[] {
  if (
    !Array.isArray(value) ||
    value.length < 1 ||
    value.length > MAX_BATCH_SIZE
  ) {
    throw new MedallionError(
      `${kind} publish batch must contain between 1 and ${MAX_BATCH_SIZE} events.`,
      { code: "MEDALLION_INVALID_BATCH_SIZE" },
    );
  }
  return value;
}

function requiredSelector(
  value: unknown,
  path: string,
  maxBytes: number,
  missingCode: string,
): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new MedallionError(`${path} is required.`, { code: missingCode });
  }
  return boundedText(value, path, maxBytes);
}

function requiredText(value: unknown, path: string, maxBytes: number): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new MedallionError(`${path} is required.`, {
      code: "MEDALLION_INVALID_EVENT",
    });
  }
  return boundedText(value, path, maxBytes);
}

function validateWorkspaceId(value: unknown, path: string): void {
  if (typeof value !== "string" || !/^ws_[0-9a-hjkmnp-tv-z]{26}$/.test(value)) {
    throw new MedallionError(
      `${path} must match ^ws_[0-9a-hjkmnp-tv-z]{26}$.`,
      { code: "MEDALLION_INVALID_WORKSPACE_ID" },
    );
  }
}

function optionalText(value: unknown, path: string, maxBytes: number): void {
  if (value === undefined) return;
  if (typeof value !== "string") {
    throw new MedallionError(`${path} must be a string.`, {
      code: "MEDALLION_INVALID_EVENT",
    });
  }
  boundedText(value, path, maxBytes);
}

function boundedText(value: string, path: string, maxBytes: number): string {
  if (new TextEncoder().encode(value).length > maxBytes) {
    throw new MedallionError(`${path} must not exceed ${maxBytes} bytes.`, {
      code: "MEDALLION_INVALID_EVENT",
    });
  }
  return value;
}

function validPayloadJson(value: unknown, path: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new MedallionError(`${path} must contain exactly one JSON value.`, {
      code: "MEDALLION_INVALID_JSON_BODY",
    });
  }
  try {
    JSON.parse(value);
  } catch {
    throw new MedallionError(`${path} must contain exactly one JSON value.`, {
      code: "MEDALLION_INVALID_JSON_BODY",
    });
  }
}

function listLimit(value: unknown, kind: string): void {
  if (value === undefined) return;
  if (
    typeof value !== "number" ||
    !Number.isInteger(value) ||
    value < 0 ||
    value > MAX_LIST_LIMIT
  ) {
    throw new MedallionError(
      `${kind} list limit must be an integer from 0 through ${MAX_LIST_LIMIT}.`,
      { code: "MEDALLION_INVALID_PAGE_SIZE" },
    );
  }
}

function optionalEnum(
  value: unknown,
  allowed: ReadonlySet<string>,
  path: string,
): void {
  if (value === undefined) return;
  if (typeof value !== "string" || !allowed.has(value)) {
    throw new MedallionError(`${path} is invalid.`, {
      code: "MEDALLION_INVALID_EVENT",
    });
  }
}

function timestampBounds(from: unknown, to: unknown, path: string): void {
  const lower = optionalTimestamp(from, `${path}.occurredAtFrom`);
  const upper = optionalTimestamp(to, `${path}.occurredAtTo`);
  if (
    lower !== undefined &&
    upper !== undefined &&
    compare(lower, upper) >= 0
  ) {
    throw new MedallionError(
      `${path}.occurredAtFrom must be earlier than occurredAtTo.`,
      { code: "MEDALLION_INVALID_TIMESTAMP_RANGE" },
    );
  }
}

type TimestampInstant = Rfc3339Instant;

function optionalTimestamp(
  value: unknown,
  path: string,
): TimestampInstant | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string") {
    throw invalidTimestamp(path);
  }
  const instant = parseRfc3339Instant(value);
  if (instant === undefined) {
    throw invalidTimestamp(path);
  }
  if (
    instant.seconds < Math.floor(MIN_TIMESTAMP_MS / 1_000) ||
    instant.seconds > MAX_TIMESTAMP_SECONDS ||
    (instant.seconds === MAX_TIMESTAMP_SECONDS &&
      instant.nanos > MAX_TIMESTAMP_NANOS)
  ) {
    throw new MedallionError(
      `${path} must be between 1900-01-01 and 2262-04-11T23:47:16.854775807Z.`,
      { code: "MEDALLION_TIMESTAMP_OUT_OF_RANGE" },
    );
  }
  return instant;
}

function compare(left: TimestampInstant, right: TimestampInstant): number {
  return left.seconds === right.seconds
    ? left.nanos - right.nanos
    : left.seconds - right.seconds;
}

function invalidTimestamp(path: string): MedallionError {
  return new MedallionError(`${path} must be a valid RFC 3339 timestamp.`, {
    code: "MEDALLION_INVALID_TIMESTAMP",
  });
}

function rejectServerDerivedFields(
  event: Record<string, unknown>,
  path: string,
  fields: readonly string[],
): void {
  if (fields.some((field) => hasNonDefaultValue(event[field]))) {
    throw new MedallionError(`${path} contains a server-derived field.`, {
      code: "MEDALLION_SERVER_DERIVED_FIELD",
    });
  }
}

function hasNonDefaultValue(value: unknown): boolean {
  return (
    value !== undefined &&
    value !== "" &&
    value !== 0 &&
    value !== "0" &&
    value !== false &&
    value !== "AUDIT_EVENT_ORIGIN_UNSPECIFIED"
  );
}

function requiredRecord(value: unknown, path: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new MedallionError(`${path} is required.`, {
      code: "MEDALLION_INVALID_EVENT",
    });
  }
  return value as Record<string, unknown>;
}

function nonEmptyString(value: unknown): boolean {
  return typeof value === "string" && value.length > 0;
}

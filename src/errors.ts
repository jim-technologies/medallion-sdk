import { Buffer } from "node:buffer";

import {
  MEDALLION_ERROR_INFO_DOMAIN,
  MEDALLION_ERROR_REASON_POLICY,
  type MedallionKnownErrorReason,
} from "./error-policy.js";

export interface MedallionErrorOptions {
  code?: string;
  requestId?: string;
  cause?: unknown;
}

export class MedallionError extends Error {
  readonly code?: string;
  readonly requestId?: string;

  constructor(message: string, options: MedallionErrorOptions = {}) {
    super(message, { cause: options.cause });
    this.name = "MedallionError";
    this.code = options.code;
    this.requestId = options.requestId;
  }
}

export type KnownConnectErrorReason = MedallionKnownErrorReason;

export type ConnectErrorReason = KnownConnectErrorReason | (string & {});

const RETRYABLE_CONNECT_CODES = new Set([
  "deadline_exceeded",
  "resource_exhausted",
  "unavailable",
]);
const RETRYABLE_HTTP_STATUSES = new Set([408, 429, 502, 503, 504]);

export interface ConnectErrorDetail {
  type: string;
  /** Base64-encoded protobuf detail bytes. */
  value: string;
}

export interface MedallionApiErrorOptions {
  status: number;
  requestId?: string;
  connectCode?: string;
  errorInfoDomain?: string;
  errorInfoReason?: ConnectErrorReason;
  errorInfoMetadata?: Readonly<Record<string, string>>;
  details?: readonly ConnectErrorDetail[];
  cause?: unknown;
}

/** A sanitized Connect error. Raw server response bodies are never retained. */
export class MedallionApiError extends MedallionError {
  readonly status: number;
  readonly connectCode?: string;
  /** Alias retained for callers that use gRPC terminology. */
  readonly grpcCode?: string;
  readonly errorInfoDomain?: string;
  readonly errorInfoReason?: ConnectErrorReason;
  readonly errorInfoMetadata: Readonly<Record<string, string>>;
  readonly details: readonly ConnectErrorDetail[];

  constructor(message: string, options: MedallionApiErrorOptions) {
    super(message, {
      code: "MEDALLION_API_ERROR",
      requestId: options.requestId,
      cause: options.cause,
    });
    this.name = "MedallionApiError";
    this.status = options.status;
    this.connectCode = options.connectCode;
    this.grpcCode = options.connectCode;
    this.errorInfoDomain = options.errorInfoDomain;
    this.errorInfoReason = options.errorInfoReason;
    this.errorInfoMetadata = Object.freeze({
      ...(options.errorInfoMetadata ?? {}),
    });
    this.details = Object.freeze([...(options.details ?? [])]);
  }
}

interface ConnectErrorEnvelope {
  code?: string;
  details?: unknown;
}

export function medallionApiErrorFromEnvelope(
  status: number,
  requestId: string | undefined,
  value: unknown,
  cause?: unknown,
  redactions: readonly string[] = [],
): MedallionApiError {
  const envelope = asConnectEnvelope(value);
  const rawDetails = parseDetails(envelope.details);
  const errorInfo = rawDetails
    .filter((detail) => isErrorInfoType(detail.type))
    .map((detail) => decodeErrorInfo(detail.value))
    .find((detail) => detail !== undefined);
  const details = sanitizeDetails(rawDetails, redactions);
  const message = apiErrorMessage(status, requestId);

  return new MedallionApiError(message, {
    status,
    requestId,
    connectCode: redactText(nonEmptyString(envelope.code), redactions),
    errorInfoDomain: redactText(errorInfo?.domain, redactions),
    errorInfoReason: redactText(
      errorInfo?.reason,
      redactions,
    ) as ConnectErrorReason,
    errorInfoMetadata: redactRecord(errorInfo?.metadata, redactions),
    details,
    cause: sanitizedErrorCause(cause, redactions),
  });
}

export function sanitizedErrorCause(
  value: unknown,
  _redactions: readonly string[] = [],
): Error | undefined {
  if (!(value instanceof Error)) return undefined;
  const normalizedName = ["AbortError", "TimeoutError", "TypeError"].includes(
    value.name,
  )
    ? value.name
    : "Error";
  const message =
    normalizedName === "AbortError"
      ? "The request operation was aborted."
      : normalizedName === "TimeoutError"
        ? "The request operation timed out."
        : normalizedName === "TypeError"
          ? "A network or protocol error occurred."
          : "A request error occurred.";
  const safe = new Error(message);
  safe.name = normalizedName;
  return safe;
}

/** Conservative guidance for a safely idempotent request. */
export function isRetryableConnectError(
  error: unknown,
  safelyIdempotent: boolean,
): boolean {
  if (!safelyIdempotent || !(error instanceof MedallionApiError)) {
    return false;
  }

  if (error.errorInfoReason !== undefined) {
    // ErrorInfo is more specific than Connect/HTTP transport metadata. Only a
    // registry entry in the expected domain can authorize an automatic retry.
    if (
      error.errorInfoDomain !== MEDALLION_ERROR_INFO_DOMAIN ||
      !Object.hasOwn(MEDALLION_ERROR_REASON_POLICY, error.errorInfoReason)
    ) {
      return false;
    }
    const policy =
      MEDALLION_ERROR_REASON_POLICY[
        error.errorInfoReason as MedallionKnownErrorReason
      ];
    return (
      error.connectCode !== undefined &&
      error.connectCode.toLowerCase() === policy.grpcCode.toLowerCase() &&
      policy.reuseIdempotencyKey &&
      (policy.retryClassification === "bounded_transient_retry" ||
        policy.retryClassification ===
          "bounded_transient_retry_idempotent_operations_only")
    );
  }
  if (error.connectCode !== undefined) {
    return RETRYABLE_CONNECT_CODES.has(error.connectCode.toLowerCase());
  }
  return RETRYABLE_HTTP_STATUSES.has(error.status);
}

function asConnectEnvelope(value: unknown): ConnectErrorEnvelope {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const object = value as Record<string, unknown>;
  return {
    code: typeof object.code === "string" ? object.code : undefined,
    details: object.details,
  };
}

function parseDetails(value: unknown): ConnectErrorDetail[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const details: ConnectErrorDetail[] = [];
  for (const item of value) {
    if (item === null || typeof item !== "object" || Array.isArray(item)) {
      continue;
    }
    const object = item as Record<string, unknown>;
    if (typeof object.type === "string" && typeof object.value === "string") {
      details.push({ type: object.type, value: object.value });
    }
  }
  return details;
}

function sanitizeDetails(
  details: readonly ConnectErrorDetail[],
  redactions: readonly string[],
): ConnectErrorDetail[] {
  const secrets = redactions.filter((value) => value.length > 0);
  return details.map((detail) => {
    const type = redactText(detail.type, secrets) ?? "[REDACTED]";
    if (secrets.length === 0) return { type, value: detail.value };
    const decoded = Buffer.from(detail.value, "base64");
    const containsSecret = secrets.some((secret) =>
      decoded.includes(Buffer.from(secret, "utf8")),
    );
    return {
      type,
      value: containsSecret
        ? Buffer.from("[REDACTED]", "utf8").toString("base64")
        : detail.value,
    };
  });
}

function isErrorInfoType(type: string): boolean {
  return (
    type === "google.rpc.ErrorInfo" || type.endsWith("/google.rpc.ErrorInfo")
  );
}

interface DecodedErrorInfo {
  reason?: ConnectErrorReason;
  domain?: string;
  metadata: Record<string, string>;
}

function decodeErrorInfo(value: string): DecodedErrorInfo | undefined {
  let bytes: Uint8Array;
  try {
    bytes = new Uint8Array(Buffer.from(value, "base64"));
  } catch {
    return undefined;
  }
  if (bytes.length === 0 && value.length > 0) {
    return undefined;
  }

  try {
    const output: DecodedErrorInfo = { metadata: {} };
    let offset = 0;
    while (offset < bytes.length) {
      const tag = readVarint(bytes, offset);
      offset = tag.offset;
      const fieldNumber = Number(tag.value >> 3n);
      const wireType = Number(tag.value & 7n);
      if (wireType !== 2) {
        offset = skipField(bytes, offset, wireType);
        continue;
      }
      const field = readLengthDelimited(bytes, offset);
      offset = field.offset;
      if (fieldNumber === 1) {
        output.reason = decodeUtf8(field.value) as ConnectErrorReason;
      } else if (fieldNumber === 2) {
        output.domain = decodeUtf8(field.value);
      } else if (fieldNumber === 3) {
        const entry = decodeStringMapEntry(field.value);
        if (entry !== undefined) {
          output.metadata[entry[0]] = entry[1];
        }
      }
    }
    return output;
  } catch {
    return undefined;
  }
}

function decodeStringMapEntry(bytes: Uint8Array): [string, string] | undefined {
  let key = "";
  let value = "";
  let offset = 0;
  while (offset < bytes.length) {
    const tag = readVarint(bytes, offset);
    offset = tag.offset;
    const fieldNumber = Number(tag.value >> 3n);
    const wireType = Number(tag.value & 7n);
    if (wireType !== 2) {
      offset = skipField(bytes, offset, wireType);
      continue;
    }
    const field = readLengthDelimited(bytes, offset);
    offset = field.offset;
    if (fieldNumber === 1) key = decodeUtf8(field.value);
    if (fieldNumber === 2) value = decodeUtf8(field.value);
  }
  return key.length === 0 ? undefined : [key, value];
}

function readVarint(
  bytes: Uint8Array,
  start: number,
): { value: bigint; offset: number } {
  let value = 0n;
  let shift = 0n;
  let offset = start;
  while (offset < bytes.length && shift <= 63n) {
    const byte = bytes[offset++];
    if (byte === undefined) break;
    value |= BigInt(byte & 0x7f) << shift;
    if ((byte & 0x80) === 0) return { value, offset };
    shift += 7n;
  }
  throw new RangeError("Invalid protobuf varint.");
}

function readLengthDelimited(
  bytes: Uint8Array,
  start: number,
): { value: Uint8Array; offset: number } {
  const length = readVarint(bytes, start);
  const size = Number(length.value);
  const end = length.offset + size;
  if (!Number.isSafeInteger(size) || end > bytes.length) {
    throw new RangeError("Invalid protobuf length-delimited field.");
  }
  return { value: bytes.subarray(length.offset, end), offset: end };
}

function skipField(
  bytes: Uint8Array,
  offset: number,
  wireType: number,
): number {
  switch (wireType) {
    case 0:
      return readVarint(bytes, offset).offset;
    case 1:
      if (offset + 8 > bytes.length) throw new RangeError("Invalid fixed64.");
      return offset + 8;
    case 2:
      return readLengthDelimited(bytes, offset).offset;
    case 5:
      if (offset + 4 > bytes.length) throw new RangeError("Invalid fixed32.");
      return offset + 4;
    default:
      throw new RangeError("Unsupported protobuf wire type.");
  }
}

function decodeUtf8(value: Uint8Array): string {
  return new TextDecoder("utf-8", { fatal: true }).decode(value);
}

function nonEmptyString(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed === undefined || trimmed.length === 0 ? undefined : trimmed;
}

function apiErrorMessage(
  status: number,
  requestId: string | undefined,
): string {
  const suffix = requestId === undefined ? "" : ` Request ID: ${requestId}.`;
  return `Medallion API request failed with HTTP ${status}.${suffix}`;
}

function redactRecord(
  value: Readonly<Record<string, string>> | undefined,
  redactions: readonly string[],
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(value ?? {}).map(([key, item]) => [
      redactText(key, redactions) ?? "[REDACTED]",
      redactText(item, redactions) ?? "",
    ]),
  );
}

function redactText(
  value: string | undefined,
  redactions: readonly string[],
): string | undefined {
  let output = value;
  for (const secret of redactions) {
    if (output === undefined || secret.length === 0) continue;
    output = output.split(secret).join("[REDACTED]");
  }
  return output;
}

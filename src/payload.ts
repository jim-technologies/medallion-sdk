import { MedallionError } from "./errors.js";
import { normalizeId } from "./ids.js";
import type { IdInput } from "./types.js";

const RFC3339_TIMESTAMP =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(?:Z|([+-])(\d{2}):(\d{2}))$/;

export interface Rfc3339Instant {
  seconds: number;
  nanos: number;
}

export function parseRfc3339Instant(value: string): Rfc3339Instant | undefined {
  const match = RFC3339_TIMESTAMP.exec(value);
  if (match === null) return undefined;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const fraction = match[7] ?? "";
  const offsetHour = Number(match[9] ?? 0);
  const offsetMinute = Number(match[10] ?? 0);
  if (
    year < 1 ||
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > daysInMonth(year, month) ||
    hour > 23 ||
    minute > 59 ||
    second > 59 ||
    offsetHour > 23 ||
    offsetMinute > 59
  ) {
    return undefined;
  }
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds)) return undefined;
  return {
    seconds: Math.floor(milliseconds / 1_000),
    nanos: Number(fraction.padEnd(9, "0")),
  };
}

export function optionalId(
  value: IdInput | undefined,
  path: string,
  maxBytes?: number,
): string | undefined {
  if (value === undefined) return undefined;
  return validatedId(normalizeId(value, path), path, maxBytes);
}

export function requiredId(
  value: IdInput,
  path: string,
  maxBytes?: number,
): string {
  return validatedId(normalizeId(value, path), path, maxBytes);
}

export function jsonString(value: unknown, path: string): string {
  try {
    return canonicalJson(value === undefined ? {} : value, new Set());
  } catch (error) {
    if (error instanceof MedallionError) throw error;
    throw new MedallionError(`${path} must be JSON serializable.`, {
      code: "MEDALLION_INVALID_JSON_BODY",
    });
  }
}

export function requiredIdempotencyKey(
  input: unknown,
  path: string,
  maxBytes?: number,
): string {
  if (typeof input !== "string") {
    throw new MedallionError(`${path} is required for retry-safe delivery.`, {
      code: "MEDALLION_MISSING_IDEMPOTENCY_KEY",
    });
  }
  if (input.length === 0) {
    throw new MedallionError(`${path} is required for retry-safe delivery.`, {
      code: "MEDALLION_MISSING_IDEMPOTENCY_KEY",
    });
  }
  if (!hasValidUnicodeScalars(input)) {
    throw new MedallionError(`${path} must contain valid Unicode.`, {
      code: "MEDALLION_INVALID_IDEMPOTENCY_KEY",
    });
  }
  if (
    maxBytes !== undefined &&
    new TextEncoder().encode(input).length > maxBytes
  ) {
    throw new MedallionError(`${path} must not exceed ${maxBytes} bytes.`, {
      code: "MEDALLION_INVALID_IDEMPOTENCY_KEY",
    });
  }
  return input;
}

function hasValidUnicodeScalars(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) return false;
      index += 1;
      continue;
    }
    if (unit >= 0xdc00 && unit <= 0xdfff) return false;
  }
  return true;
}

export function occurredAt(
  value: string | Date | undefined,
): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw new MedallionError("occurredAt must be a valid date.", {
        code: "MEDALLION_INVALID_TIMESTAMP",
      });
    }
    return value.toISOString();
  }
  if (parseRfc3339Instant(value) === undefined) {
    throw new MedallionError(
      "occurredAt must be an RFC 3339 timestamp with an explicit offset.",
      { code: "MEDALLION_INVALID_TIMESTAMP" },
    );
  }
  return value;
}

function daysInMonth(year: number, month: number): number {
  if (month === 2) {
    const leap = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
    return leap ? 29 : 28;
  }
  return [4, 6, 9, 11].includes(month) ? 30 : 31;
}

export function rawOrJsonString(
  value: unknown,
  raw: string | undefined,
  path: string,
): string {
  if (value !== undefined && raw !== undefined) {
    throw new MedallionError(
      `${path} and ${path}Json are mutually exclusive.`,
      { code: "MEDALLION_AMBIGUOUS_JSON_PAYLOAD" },
    );
  }
  if (raw !== undefined) {
    try {
      JSON.parse(raw);
    } catch {
      throw new MedallionError(`${path}Json must contain valid JSON.`, {
        code: "MEDALLION_INVALID_JSON_BODY",
      });
    }
    return raw;
  }
  return jsonString(value === undefined ? {} : value, path);
}

export function compactRecord<T extends Record<string, unknown>>(value: T): T {
  const out: Record<string, unknown> = {};

  for (const [key, item] of Object.entries(value)) {
    if (item !== undefined) {
      out[key] = item;
    }
  }

  return out as T;
}

function canonicalJson(value: unknown, ancestors: Set<object>): string {
  if (value === null) return "null";
  switch (typeof value) {
    case "string":
    case "boolean":
      return JSON.stringify(value);
    case "number":
      if (!Number.isFinite(value)) {
        throw new MedallionError("JSON payload numbers must be finite.", {
          code: "MEDALLION_INVALID_JSON_BODY",
        });
      }
      return JSON.stringify(value);
    case "undefined":
    case "function":
    case "symbol":
    case "bigint":
      throw new MedallionError(
        `JSON payloads cannot contain ${typeof value} values.`,
        { code: "MEDALLION_INVALID_JSON_BODY" },
      );
    case "object":
      break;
  }

  const object = value as object;
  if (ancestors.has(object)) {
    throw new MedallionError("JSON payloads cannot contain cycles.", {
      code: "MEDALLION_INVALID_JSON_BODY",
    });
  }
  ancestors.add(object);
  try {
    if (Array.isArray(value)) {
      for (let index = 0; index < value.length; index += 1) {
        if (!(index in value)) {
          throw new MedallionError(
            "JSON payload arrays cannot contain holes.",
            {
              code: "MEDALLION_INVALID_JSON_BODY",
            },
          );
        }
      }
      return `[${value.map((item) => canonicalJson(item, ancestors)).join(",")}]`;
    }

    const prototype = Object.getPrototypeOf(object);
    if (prototype !== Object.prototype && prototype !== null) {
      throw new MedallionError(
        "JSON payloads must contain only plain objects and arrays.",
        { code: "MEDALLION_INVALID_JSON_BODY" },
      );
    }
    if (Object.getOwnPropertySymbols(object).length > 0) {
      throw new MedallionError("JSON payloads cannot contain symbol keys.", {
        code: "MEDALLION_INVALID_JSON_BODY",
      });
    }
    const descriptors = Object.getOwnPropertyDescriptors(object);
    const entries: string[] = [];
    for (const key of Object.keys(descriptors).sort()) {
      const descriptor = descriptors[key];
      if (descriptor === undefined || descriptor.get || descriptor.set) {
        throw new MedallionError(
          "JSON payloads cannot contain accessor properties.",
          { code: "MEDALLION_INVALID_JSON_BODY" },
        );
      }
      if (!descriptor.enumerable) continue;
      entries.push(
        `${JSON.stringify(key)}:${canonicalJson(descriptor.value, ancestors)}`,
      );
    }
    return `{${entries.join(",")}}`;
  } finally {
    ancestors.delete(object);
  }
}

function validatedId(
  value: string,
  path: string,
  maxBytes: number | undefined,
): string {
  if (value.trim().length === 0) {
    throw new MedallionError(`${path} must not be empty.`, {
      code: "MEDALLION_INVALID_EVENT",
    });
  }
  if (
    maxBytes !== undefined &&
    new TextEncoder().encode(value).length > maxBytes
  ) {
    throw new MedallionError(`${path} must not exceed ${maxBytes} bytes.`, {
      code: "MEDALLION_INVALID_EVENT",
    });
  }
  return value;
}

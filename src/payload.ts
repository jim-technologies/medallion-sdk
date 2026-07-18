import { MedallionError } from "./errors.js";
import { normalizeId } from "./ids.js";
import type { IdInput } from "./types.js";

export function optionalId(
  value: IdInput | undefined,
  path: string,
): string | undefined {
  return value === undefined ? undefined : normalizeId(value, path);
}

export function requiredId(value: IdInput, path: string): string {
  return normalizeId(value, path);
}

export function jsonString(value: unknown, path: string): string {
  try {
    return JSON.stringify(value ?? {});
  } catch (error) {
    throw new MedallionError(`${path} must be JSON serializable.`, {
      code: "MEDALLION_INVALID_JSON_BODY",
      cause: error,
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
  const value = input.trim();
  if (value.length === 0) {
    throw new MedallionError(`${path} is required for retry-safe delivery.`, {
      code: "MEDALLION_MISSING_IDEMPOTENCY_KEY",
    });
  }
  if (
    maxBytes !== undefined &&
    new TextEncoder().encode(value).length > maxBytes
  ) {
    throw new MedallionError(`${path} must not exceed ${maxBytes} bytes.`, {
      code: "MEDALLION_INVALID_IDEMPOTENCY_KEY",
    });
  }
  return value;
}

export function requiredControlIdempotencyKey(
  input: unknown,
  path: string,
): string {
  if (typeof input !== "string" || input.length === 0) {
    throw new MedallionError(`${path} is required for retry-safe mutation.`, {
      code: "MEDALLION_MISSING_IDEMPOTENCY_KEY",
    });
  }
  if (new TextEncoder().encode(input).length > 256 || !/^[!-~]+$/.test(input)) {
    throw new MedallionError(
      `${path} must be at most 256 bytes of visible ASCII without spaces.`,
      { code: "MEDALLION_INVALID_IDEMPOTENCY_KEY" },
    );
  }
  return input;
}

export function occurredAt(
  value: string | Date | undefined,
): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  return value instanceof Date ? value.toISOString() : value;
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

import { MedallionError } from "./errors.js";
import { normalizeId } from "./ids.js";
import type { IdInput } from "./types.js";

export function optionalId(value: IdInput | undefined, path: string): string | undefined {
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

export function idempotencyKey(input?: string): string {
  if (input !== undefined && input.trim().length > 0) {
    return input;
  }

  if (globalThis.crypto?.randomUUID !== undefined) {
    return globalThis.crypto.randomUUID();
  }

  return `idem_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}

export function occurredAt(value: string | Date | undefined): string | undefined {
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

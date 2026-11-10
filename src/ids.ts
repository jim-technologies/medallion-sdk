import { MedallionError } from "./errors.js";
import type { IdInput } from "./types.js";

export function normalizeId(value: IdInput, path = "id"): string {
  if (typeof value === "string") {
    return value;
  }

  if (typeof value === "bigint") {
    return value.toString();
  }

  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      throw new MedallionError(
        `Unsafe numeric ID at ${path}. Pass this ID as a string to preserve precision.`,
        { code: "MEDALLION_UNSAFE_ID" },
      );
    }

    return value.toString();
  }

  throw new MedallionError(
    `Invalid ID at ${path}. Expected string, number, or bigint.`,
    { code: "MEDALLION_INVALID_ID" },
  );
}

export function normalizeIdRecord(
  values: Record<string, IdInput>,
  path = "primaryKey",
): Record<string, string> {
  const normalized: Record<string, string> = {};

  for (const [key, value] of Object.entries(values)) {
    normalized[key] = normalizeId(value, `${path}.${key}`);
  }

  return normalized;
}

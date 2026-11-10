import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { MedallionError } from "./errors.js";
import { normalizeId } from "./ids.js";
import { requiredIdempotencyKey } from "./payload.js";

export const MAX_INGESTION_ITERATOR_PAGES = 10_000;

export function assertIteratorPageWithinLimit(page: number): void {
  if (page > MAX_INGESTION_ITERATOR_PAGES) {
    throw new MedallionError(
      `Medallion iteration exceeded ${MAX_INGESTION_ITERATOR_PAGES} pages. Resume explicitly from the last cursor.`,
      { code: "MEDALLION_PAGINATION_LIMIT" },
    );
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

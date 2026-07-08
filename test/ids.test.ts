import { describe, expect, it } from "vitest";
import { MedallionError, normalizeId, normalizeIdRecord } from "../src/index.js";

describe("ID normalization", () => {
  it("preserves string IDs exactly", () => {
    expect(normalizeId("000123")).toBe("000123");
  });

  it("converts safe number and bigint IDs to strings", () => {
    expect(normalizeId(123)).toBe("123");
    expect(normalizeId(123n)).toBe("123");
  });

  it("rejects unsafe numeric IDs", () => {
    expect(() => normalizeId(Number.MAX_SAFE_INTEGER + 1)).toThrow(
      MedallionError,
    );
    expect(() => normalizeId(Number.MAX_SAFE_INTEGER + 1)).toThrow(
      "Pass this ID as a string",
    );
  });

  it("normalizes ID records without mutating input", () => {
    const primaryKey = {
      accountId: "00042",
      orderId: 123,
      lineId: 999n,
    };

    expect(normalizeIdRecord(primaryKey)).toEqual({
      accountId: "00042",
      orderId: "123",
      lineId: "999",
    });
    expect(primaryKey.orderId).toBe(123);
    expect(primaryKey.lineId).toBe(999n);
  });
});

import { describe, expect, it } from "vitest";

import {
  isRetryableConnectError,
  type KnownConnectErrorReason,
  MedallionApiError,
} from "../src/index.js";

const REASON_RETRY_POLICY = {
  AUTHORIZATION_DEPENDENCY_UNAVAILABLE: ["unavailable", true],
  BACKPRESSURE: ["resource_exhausted", true],
  CAPABILITY_UNAVAILABLE: ["failed_precondition", false],
  FEATURE_NOT_ENTITLED: ["failed_precondition", false],
  IDEMPOTENCY_MISMATCH: ["already_exists", false],
  INTEGRATION_UNAVAILABLE: ["unavailable", true],
  WORKSPACE_SELECTOR_CONFLICT: ["invalid_argument", false],
} satisfies Record<KnownConnectErrorReason, readonly [string, boolean]>;

describe("structured error retry policy", () => {
  it("matches the complete frozen ErrorInfo reason inventory", () => {
    expect(Object.keys(REASON_RETRY_POLICY).sort()).toEqual([
      "AUTHORIZATION_DEPENDENCY_UNAVAILABLE",
      "BACKPRESSURE",
      "CAPABILITY_UNAVAILABLE",
      "FEATURE_NOT_ENTITLED",
      "IDEMPOTENCY_MISMATCH",
      "INTEGRATION_UNAVAILABLE",
      "WORKSPACE_SELECTOR_CONFLICT",
    ]);
    for (const [reason, [connectCode, retryable]] of Object.entries(
      REASON_RETRY_POLICY,
    )) {
      const error = new MedallionApiError("safe", {
        status: 503,
        connectCode,
        errorInfoDomain: "medallion.jimtech.io",
        errorInfoReason: reason as KnownConnectErrorReason,
      });
      expect(isRetryableConnectError(error, true), reason).toBe(retryable);
      expect(isRetryableConnectError(error, false), reason).toBe(false);
    }
  });

  it("fails closed when a known reason has the wrong Connect code", () => {
    const error = new MedallionApiError("safe", {
      status: 503,
      connectCode: "unavailable",
      errorInfoDomain: "medallion.jimtech.io",
      errorInfoReason: "BACKPRESSURE",
    });
    expect(isRetryableConnectError(error, true)).toBe(false);
  });

  it("preserves unknown reasons without blindly retrying them", () => {
    const error = new MedallionApiError("safe", {
      status: 503,
      connectCode: "unavailable",
      errorInfoDomain: "medallion.jimtech.io",
      errorInfoReason: "FUTURE_REASON",
    });
    expect(isRetryableConnectError(error, true)).toBe(false);
    expect(error.errorInfoReason).toBe("FUTURE_REASON");
  });

  it("does not apply a known reason from an untrusted domain", () => {
    const error = new MedallionApiError("safe", {
      status: 503,
      connectCode: "unavailable",
      errorInfoDomain: "example.invalid",
      errorInfoReason: "BACKPRESSURE",
    });
    expect(isRetryableConnectError(error, true)).toBe(false);
  });

  it("uses conservative Connect and HTTP fallbacks only without ErrorInfo", () => {
    for (const connectCode of [
      "deadline_exceeded",
      "resource_exhausted",
      "unavailable",
    ]) {
      expect(
        isRetryableConnectError(
          new MedallionApiError("safe", { status: 400, connectCode }),
          true,
        ),
        connectCode,
      ).toBe(true);
    }
    for (const connectCode of ["aborted", "permission_denied", "unknown"]) {
      expect(
        isRetryableConnectError(
          new MedallionApiError("safe", {
            status: 503,
            connectCode,
          }),
          true,
        ),
        connectCode,
      ).toBe(false);
    }
    for (const status of [408, 429, 502, 503, 504]) {
      expect(
        isRetryableConnectError(
          new MedallionApiError("safe", { status }),
          true,
        ),
        String(status),
      ).toBe(true);
    }
  });
});

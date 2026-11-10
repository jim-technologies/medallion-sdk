import { Buffer } from "node:buffer";
import { describe, expect, it, vi } from "vitest";
import {
  type FetchLike,
  idempotencyKeyFromParts,
  MedallionApiError,
  MedallionClient,
  type RetryOptions,
} from "../src/index.js";

// Shared transport behavior, exercised through the retained ingest RPCs.

const WORKSPACE_ID = "ws_01jz9q5g6rsf7r5ar4rah1b2c3";
const INGEST_SERVICE = "medallion.ingest.v1.MedallionIngestService";

describe("shared transport through the ingest RPCs", () => {
  it("uses bearer JWTs exclusively and rejects ambiguous credentials", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse({ tables: [] }),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      accessToken: "end_user_jwt",
      workspaceId: WORKSPACE_ID,
      timeoutMs: 12_345,
      fetch,
    });

    await client.ingest.listTables({});
    const [url, init] = fetch.mock.calls[0]!;
    const headers = init?.headers as Headers;
    expect(url).toBe(`https://api.example.com/${INGEST_SERVICE}/ListTables`);
    expect(headers.get("authorization")).toBe("Bearer end_user_jwt");
    expect(headers.get("x-medallion-api-key")).toBeNull();
    expect(headers.get("x-medallion-workspace-id")).toBe(WORKSPACE_ID);
    expect(headers.get("connect-timeout-ms")).toBe("12345");
    expect(JSON.parse(String(init?.body))).not.toHaveProperty("workspaceId");

    expect(
      () =>
        new MedallionClient({
          baseUrl: "https://api.example.com",
          accessToken: "jwt",
          apiKey: "key",
          workspaceId: WORKSPACE_ID,
          fetch,
        }),
    ).toThrowError(/exactly one/);
  });

  it("retries an append with the exact body and batch idempotency key", async () => {
    const calls: { body: string; key: string | null }[] = [];
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, init?: RequestInit) => {
        calls.push({
          body: String(init?.body),
          key: new Headers(init?.headers).get("idempotency-key"),
        });
        if (calls.length === 1) {
          return jsonResponse(
            connectError("resource_exhausted", "BACKPRESSURE"),
            503,
            { "retry-after": "0" },
          );
        }
        return jsonResponse({ acceptedRows: "2" });
      },
    );
    const client = retryingClient(fetch, {
      maxAttempts: 2,
      initialDelayMs: 0,
      maxDelayMs: 0,
      jitterRatio: 0,
    });

    const result = await client.tables.append(
      "events",
      [{ level: "info" }, { level: "warn" }],
      { idempotencyKey: "outbox:batch:7" },
    );

    expect(fetch).toHaveBeenCalledTimes(2);
    expect(calls[1]).toEqual(calls[0]);
    expect(calls[0]?.key).toBe("outbox:batch:7");
    expect(JSON.parse(calls[0]!.body)).toMatchObject({
      table: "tables/events",
      requestId: "outbox:batch:7",
    });
    expect(result.acceptedRows).toBe(2);
  });

  it("decodes ErrorInfo and never retries an idempotency mismatch", async () => {
    const fetch = vi.fn(async () =>
      jsonResponse(connectError("already_exists", "IDEMPOTENCY_MISMATCH"), 409),
    );
    const client = retryingClient(fetch, {
      maxAttempts: 3,
      initialDelayMs: 0,
      maxDelayMs: 0,
    });

    await expect(
      client.tables.append("events", [{ level: "info" }], {
        idempotencyKey: "outbox:batch:8",
      }),
    ).rejects.toMatchObject({
      name: "MedallionApiError",
      status: 409,
      connectCode: "already_exists",
      errorInfoDomain: "medallion.jimtech.io",
      errorInfoReason: "IDEMPOTENCY_MISMATCH",
      errorInfoMetadata: { operation: "AppendRows" },
    } satisfies Partial<MedallionApiError>);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("never retries authentication failures with transient-looking HTTP status", async () => {
    const fetch = vi.fn(async () =>
      jsonResponse(
        {
          code: "unauthenticated",
          message: "The request could not be authenticated.",
        },
        503,
      ),
    );
    const client = retryingClient(fetch, {
      maxAttempts: 3,
      initialDelayMs: 0,
      maxDelayMs: 0,
    });

    await expect(client.ingest.listTables({})).rejects.toMatchObject({
      status: 503,
      connectCode: "unauthenticated",
    });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("preserves unknown ErrorInfo reasons and details", async () => {
    const fetch = vi.fn(async () =>
      jsonResponse(connectError("invalid_argument", "FUTURE_REASON"), 400),
    );
    const client = retryingClient(fetch);

    const error = await client.ingest.listTables({}).catch((thrown) => thrown);
    expect(error).toBeInstanceOf(MedallionApiError);
    expect(error).toMatchObject({
      errorInfoReason: "FUTURE_REASON",
      details: [
        { type: "google.rpc.ErrorInfo" },
        { type: "example.future.Detail", value: "AQI=" },
      ],
    });
  });

  it("redacts credentials echoed by an error envelope", async () => {
    const secret = "credential_that_must_not_escape";
    for (const credential of [
      { apiKey: secret },
      { accessToken: secret },
    ] as const) {
      const fetch = vi.fn(async () =>
        jsonResponse(
          {
            code: "invalid_argument",
            message: `The supplied credential ${secret} was rejected.`,
            details: [
              {
                type: "example.future.Detail",
                value: Buffer.from(`echo:${secret}`, "utf8").toString("base64"),
              },
              {
                type: "google.rpc.ErrorInfo",
                value: encodeErrorInfo(
                  "FUTURE_REASON",
                  "medallion.jimtech.io",
                  {
                    echo: secret,
                  },
                ),
              },
            ],
          },
          400,
          { "x-request-id": `request-${secret}` },
        ),
      );
      const client = new MedallionClient({
        baseUrl: "https://api.example.com",
        workspaceId: WORKSPACE_ID,
        fetch,
        ...credential,
      });

      const error = await client.ingest
        .listTables({})
        .catch((thrown) => thrown);
      expect(error).toBeInstanceOf(MedallionApiError);
      const apiError = error as MedallionApiError;
      expect(apiError.message).not.toContain(secret);
      expect(apiError.requestId).not.toContain(secret);
      expect(JSON.stringify(apiError)).not.toContain(secret);
      expect(apiError.errorInfoMetadata).toEqual({ echo: "[REDACTED]" });
      expect(
        Buffer.from(apiError.details[0]?.value ?? "", "base64").toString(
          "utf8",
        ),
      ).not.toContain(secret);
    }
  });

  it("does not retain credentials or request bodies in network error causes", async () => {
    const secret = "credential_that_must_not_escape";
    const fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      throw new Error(`${secret}:${String(init?.body)}`);
    });
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: secret,
      workspaceId: WORKSPACE_ID,
      fetch,
    });

    const error = await client.tables
      .append("events", [{ level: "row_value_that_must_not_escape" }])
      .catch((thrown) => thrown);
    expect(error).toMatchObject({ code: "MEDALLION_NETWORK_ERROR" });
    const cause = (error as Error & { cause?: Error }).cause;
    expect(cause?.message).toBe("A request error occurred.");
    expect(JSON.stringify(error)).not.toContain(secret);
    expect(JSON.stringify(error)).not.toContain(
      "row_value_that_must_not_escape",
    );
  });

  it("cancels bounded backpressure before another attempt", async () => {
    const controller = new AbortController();
    const fetch = vi.fn(async () => {
      queueMicrotask(() => controller.abort());
      return jsonResponse(
        connectError("resource_exhausted", "BACKPRESSURE"),
        503,
      );
    });
    const client = retryingClient(fetch, {
      maxAttempts: 3,
      initialDelayMs: 1_000,
      maxDelayMs: 1_000,
      jitterRatio: 0,
    });

    await expect(
      client.ingest.listTables({}, { signal: controller.signal }),
    ).rejects.toMatchObject({ code: "MEDALLION_ABORTED" });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("does not shorten server Retry-After guidance", async () => {
    const controller = new AbortController();
    const fetch = vi.fn(async () =>
      jsonResponse(connectError("resource_exhausted", "BACKPRESSURE"), 503, {
        "retry-after": "1",
      }),
    );
    const client = retryingClient(fetch, {
      maxAttempts: 3,
      initialDelayMs: 0,
      maxDelayMs: 0,
      jitterRatio: 0,
    });
    setTimeout(() => controller.abort(), 10);

    await expect(
      client.ingest.listTables({}, { signal: controller.signal }),
    ).rejects.toMatchObject({ code: "MEDALLION_ABORTED" });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("does not overflow oversized Retry-After delays into an early retry", async () => {
    const controller = new AbortController();
    const fetch = vi.fn(async () =>
      jsonResponse(connectError("resource_exhausted", "BACKPRESSURE"), 503, {
        "retry-after": "9999999999",
      }),
    );
    const client = retryingClient(fetch, {
      maxAttempts: 3,
      initialDelayMs: 0,
      maxDelayMs: 0,
      jitterRatio: 0,
    });
    setTimeout(() => controller.abort(), 10);

    await expect(
      client.ingest.listTables({}, { signal: controller.signal }),
    ).rejects.toMatchObject({ code: "MEDALLION_ABORTED" });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("creates stable source-derived keys matching the other SDKs", () => {
    // The same vector is pinned in the Go and Python SDKs.
    expect(idempotencyKeyFromParts(" orders ", "partition-1", 42n)).toBe(
      "orders:103ec0f8-cc69-5f19-81d9-08f2d641a5e4",
    );
    expect(idempotencyKeyFromParts("orders", "partition-1", 42)).toBe(
      idempotencyKeyFromParts("orders", "partition-1", 42n),
    );
    expect(idempotencyKeyFromParts("orders", "partition-1", 43)).not.toBe(
      idempotencyKeyFromParts("orders", "partition-1", 42),
    );
  });
});

function retryingClient(
  fetch: FetchLike,
  retry?: RetryOptions,
): MedallionClient {
  return new MedallionClient({
    baseUrl: "https://api.example.com",
    apiKey: "scoped_api_key",
    workspaceId: WORKSPACE_ID,
    fetch,
    retry,
  });
}

function jsonResponse(
  body: unknown,
  status = 200,
  headers: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

function connectError(code: string, reason: string) {
  return {
    code,
    message: "The request could not be completed.",
    details: [
      {
        type: "google.rpc.ErrorInfo",
        value: encodeErrorInfo(reason, "medallion.jimtech.io", {
          operation: "AppendRows",
        }),
      },
      { type: "example.future.Detail", value: "AQI=" },
    ],
  };
}

function encodeErrorInfo(
  reason: string,
  domain: string,
  metadata: Record<string, string>,
): string {
  const fields = [stringField(1, reason), stringField(2, domain)];
  for (const [key, value] of Object.entries(metadata)) {
    fields.push(
      bytesField(
        3,
        Buffer.concat([stringField(1, key), stringField(2, value)]),
      ),
    );
  }
  return Buffer.concat(fields).toString("base64");
}

function stringField(field: number, value: string): Buffer {
  return bytesField(field, Buffer.from(value, "utf8"));
}

function bytesField(field: number, value: Buffer): Buffer {
  return Buffer.concat([Buffer.from([(field << 3) | 2, value.length]), value]);
}

import { Buffer } from "node:buffer";
import { describe, expect, it, vi } from "vitest";
import {
  type FetchLike,
  idempotencyKeyFromParts,
  MedallionApiError,
  MedallionClient,
} from "../src/index.js";
import {
  assertIteratorPageWithinLimit,
  MAX_INGESTION_ITERATOR_PAGES,
} from "../src/ingestion.js";

const receipt = (key: string, eventId = "9223372036854775807") => ({
  accepted_count: 1,
  duplicate_count: 0,
  events: [{ idempotency_key: key, event_id: eventId }],
});

describe("external ingestion v1", () => {
  it("uses API-key, workspace, canonical Connect, and timeout headers", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse(receipt("cdc:orders:1")),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "scoped_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      timeoutMs: 12_345,
      fetch,
    });

    const result = await client.cdc.record({
      streamName: "orders",
      entityType: "order",
      entityId: "order_1",
      operation: "insert",
      idempotencyKey: "cdc:orders:1",
      payload: { amount: 42, currency: "USD" },
      occurredAt: "2026-08-01T12:34:56.123456789Z",
    });

    const [url, init] = fetch.mock.calls[0]!;
    const headers = init?.headers as Headers;
    expect(url).toBe(
      "https://api.example.com/medallion.connect.v1.MedallionConnectService/PublishCdcEvents",
    );
    expect(headers.get("authorization")).toBeNull();
    expect(headers.get("x-medallion-api-key")).toBe("scoped_api_key");
    expect(headers.get("x-medallion-workspace-id")).toBe(
      "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
    );
    expect(headers.get("connect-timeout-ms")).toBe("12345");
    expect(headers.get("content-type")).toBe("application/json");
    expect(headers.get("idempotency-key")).toBeNull();
    expect(result.events[0]?.eventId).toBe("9223372036854775807");
    const body = JSON.parse(String(init?.body)) as {
      events: Array<Record<string, unknown>>;
    };
    expect(body.events[0]).toMatchObject({
      occurredAt: "2026-08-01T12:34:56.123456789Z",
    });
    expect(body.events[0]).not.toHaveProperty("workspaceId");
    expect(body.events[0]).not.toHaveProperty("observedAt");
  });

  it("uses bearer JWTs exclusively and rejects ambiguous credentials", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        jsonResponse(receipt("audit:1", "1")),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      accessToken: "end_user_jwt",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      fetch,
    });

    await client.audit.record({
      resourceType: "invoice",
      resourceId: "invoice_1",
      action: "invoice.approve",
      outcome: "succeeded",
      idempotencyKey: "audit:1",
      payload: { approvalId: "approval_1" },
    });
    const headers = fetch.mock.calls[0]?.[1]?.headers as Headers;
    expect(headers.get("authorization")).toBe("Bearer end_user_jwt");
    expect(headers.get("x-medallion-api-key")).toBeNull();
    expect(headers.get("x-medallion-workspace-id")).toBe(
      "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
    );

    expect(
      () =>
        new MedallionClient({
          baseUrl: "https://api.example.com",
          accessToken: "jwt",
          apiKey: "key",
          workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
          fetch,
        }),
    ).toThrowError(/exactly one/);
  });

  it("rejects low-level list workspace disagreement before network I/O", async () => {
    const fetch = vi.fn();
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "workspace_bound_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      fetch,
    });

    await expect(
      client.connect.listCdcEvents({
        workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c4",
      }),
    ).rejects.toMatchObject({
      code: "MEDALLION_WORKSPACE_SELECTOR_CONFLICT",
    });
    await expect(
      client.connect.listAuditEvents({
        workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c4",
      }),
    ).rejects.toMatchObject({
      code: "MEDALLION_WORKSPACE_SELECTOR_CONFLICT",
    });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("does not widen a workspace-bound credential across clients", async () => {
    const apiKey = "workspace_bound_api_key";
    const fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      const headers = init?.headers as Headers;
      if (
        headers.get("x-medallion-workspace-id") !==
        "ws_01jz9q5g6rsf7r5ar4rah1b2c3"
      ) {
        return jsonResponse({ code: "permission_denied" }, 403);
      }
      return jsonResponse(receipt("source:bound", "1"));
    });
    const configured = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey,
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      fetch,
    });
    const denied = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey,
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c4",
      defaultConnectorId: "connector_123",
      fetch,
    });
    const event = {
      streamName: "orders",
      entityType: "order",
      entityId: "1",
      operation: "insert",
      idempotencyKey: "source:bound",
    } as const;

    await expect(configured.cdc.record(event)).resolves.toMatchObject({
      acceptedCount: 1,
    });
    await expect(denied.cdc.record(event)).rejects.toMatchObject({
      status: 403,
      connectCode: "permission_denied",
    });
  });

  it("fails missing workspace and event idempotency before network I/O", async () => {
    const fetch = vi.fn();
    expect(
      () =>
        new MedallionClient({
          baseUrl: "https://api.example.com",
          apiKey: "scoped_api_key",
          defaultConnectorId: "connector_123",
          fetch,
        } as never),
    ).toThrowError(
      expect.objectContaining({ code: "MEDALLION_MISSING_WORKSPACE_ID" }),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "scoped_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      fetch,
    });
    await expect(
      client.audit.record({
        resourceType: "order",
        resourceId: "1",
        action: "order.create",
        outcome: "succeeded",
        idempotencyKey: "",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_MISSING_IDEMPOTENCY_KEY" });
    await expect(
      client.cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "1",
        operation: "insert",
        idempotencyKey: "",
        payload: {},
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_MISSING_IDEMPOTENCY_KEY" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("rejects server-owned event fields instead of silently dropping them", async () => {
    const fetch = vi.fn();
    const client = ingestionClient(fetch);

    await expect(
      client.cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "1",
        operation: "insert",
        idempotencyKey: "source:1",
        workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      } as never),
    ).rejects.toMatchObject({ code: "MEDALLION_SERVER_DERIVED_FIELD" });
    await expect(
      client.audit.record({
        resourceType: "order",
        resourceId: "1",
        action: "order.create",
        outcome: "succeeded",
        idempotencyKey: "audit:1",
        origin: "AUDIT_EVENT_ORIGIN_CONNECT",
      } as never),
    ).rejects.toMatchObject({ code: "MEDALLION_SERVER_DERIVED_FIELD" });
    await expect(
      client.cdc.publishBatch({
        connectorId: "connector_123",
        events: [
          {
            streamName: "orders",
            entityType: "order",
            entityId: "1",
            operation: "insert",
            idempotencyKey: "source:1",
            connectorId: "connector_123",
          } as never,
        ],
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_SERVER_DERIVED_FIELD" });
    await expect(
      client.audit.publishBatch({
        connectorId: "connector_123",
        events: [
          {
            resourceType: "order",
            resourceId: "1",
            action: "order.create",
            outcome: "succeeded",
            idempotencyKey: "audit:1",
            connector_id: "connector_123",
          } as never,
        ],
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_SERVER_DERIVED_FIELD" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("accepts and preserves non-ASCII idempotency keys within 512 bytes", async () => {
    const key = " \n订单:分区-7:事件-184392";
    const fetch = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) =>
      jsonResponse(receipt(key, "42")),
    );

    const result = await ingestionClient(fetch).cdc.record({
      streamName: "orders",
      entityType: "order",
      entityId: "42",
      operation: "insert",
      idempotencyKey: key,
    });

    const body = JSON.parse(String(fetch.mock.calls[0]?.[1]?.body)) as {
      events: Array<{ idempotencyKey: string }>;
    };
    expect(body.events[0]?.idempotencyKey).toBe(key);
    expect(result.idempotencyKey).toBe(key);
  });

  it("rejects idempotency keys containing invalid Unicode scalars", async () => {
    const fetch = vi.fn();
    await expect(
      ingestionClient(fetch).cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "42",
        operation: "insert",
        idempotencyKey: "source:\ud800",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_IDEMPOTENCY_KEY" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("publishes bounded batches and retries the exact body and ordering", async () => {
    const bodies: string[] = [];
    const fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      bodies.push(String(init?.body));
      if (bodies.length === 1) {
        return jsonResponse(
          connectError("resource_exhausted", "BACKPRESSURE"),
          503,
          {
            "retry-after": "0",
          },
        );
      }
      return jsonResponse({
        accepted_count: 1,
        duplicate_count: 1,
        events: [
          { idempotency_key: "source:1", event_id: "1" },
          { idempotency_key: "source:2", event_id: "2", duplicate: true },
        ],
      });
    });
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "scoped_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      retry: {
        maxAttempts: 2,
        initialDelayMs: 0,
        maxDelayMs: 0,
        jitterRatio: 0,
      },
      fetch,
    });

    const result = await client.cdc.publishBatch({
      events: [
        {
          streamName: "orders",
          entityType: "order",
          entityId: "1",
          operation: "insert",
          idempotencyKey: "source:1",
          payload: { value: 1 },
        },
        {
          streamName: "orders",
          entityType: "order",
          entityId: "2",
          operation: "update",
          idempotencyKey: "source:2",
          payload: { value: 2 },
        },
      ],
    });

    expect(fetch).toHaveBeenCalledTimes(2);
    expect(bodies[1]).toBe(bodies[0]);
    expect(
      (
        JSON.parse(bodies[0]!) as { events: Array<{ idempotencyKey: string }> }
      ).events.map((event) => event.idempotencyKey),
    ).toEqual(["source:1", "source:2"]);
    expect(result).toMatchObject({
      result: "mixed",
      duplicate: false,
      acceptedCount: 1,
      duplicateCount: 1,
    });
    expect(result.events[1]).toMatchObject({
      idempotencyKey: "source:2",
      eventId: "2",
      duplicate: true,
    });
  });

  it("decodes ErrorInfo and never retries an idempotency mismatch", async () => {
    const fetch = vi.fn(async () =>
      jsonResponse(
        connectError("failed_precondition", "IDEMPOTENCY_MISMATCH"),
        412,
      ),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "scoped_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      retry: { maxAttempts: 3, initialDelayMs: 0, maxDelayMs: 0 },
      fetch,
    });

    const promise = client.audit.record({
      resourceType: "order",
      resourceId: "1",
      action: "order.update",
      outcome: "failed",
      idempotencyKey: "audit:1",
    });
    await expect(promise).rejects.toMatchObject({
      name: "MedallionApiError",
      status: 412,
      connectCode: "failed_precondition",
      errorInfoDomain: "medallion.jimtech.io",
      errorInfoReason: "IDEMPOTENCY_MISMATCH",
      errorInfoMetadata: { operation: "PublishAuditEvents" },
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
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "scoped_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      retry: { maxAttempts: 3, initialDelayMs: 0, maxDelayMs: 0 },
      fetch,
    });

    await expect(
      client.cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "1",
        operation: "insert",
        idempotencyKey: "source:1",
      }),
    ).rejects.toMatchObject({
      status: 503,
      connectCode: "unauthenticated",
    });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("preserves unknown ErrorInfo reasons and details", async () => {
    const fetch = vi.fn(async () =>
      jsonResponse(connectError("invalid_argument", "FUTURE_REASON"), 400),
    );
    const client = ingestionClient(fetch);
    try {
      await client.cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "1",
        operation: "insert",
        idempotencyKey: "source:1",
      });
      throw new Error("expected an API error");
    } catch (error) {
      expect(error).toBeInstanceOf(MedallionApiError);
      expect(error).toMatchObject({
        errorInfoReason: "FUTURE_REASON",
        details: [
          { type: "google.rpc.ErrorInfo" },
          { type: "example.future.Detail", value: "AQI=" },
        ],
      });
    }
  });

  it("redacts credentials echoed by an error envelope", async () => {
    const secret = "credential_that_must_not_escape";
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
          ],
        },
        400,
        { "x-request-id": `request-${secret}` },
      ),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: secret,
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      fetch,
    });

    try {
      await client.cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "1",
        operation: "insert",
        idempotencyKey: "source:1",
      });
      throw new Error("expected an API error");
    } catch (error) {
      expect(error).toBeInstanceOf(MedallionApiError);
      const apiError = error as MedallionApiError;
      expect(apiError.message).not.toContain(secret);
      expect(apiError.requestId).not.toContain(secret);
      expect(
        Buffer.from(apiError.details[0]?.value ?? "", "base64").toString(
          "utf8",
        ),
      ).not.toContain(secret);
    }
  });

  it("redacts customer payloads echoed by structured error details", async () => {
    const payloadSecret = "customer_payload_that_must_not_escape";
    const numericSecret = 98_765_432_123_456_789n;
    const fetch = vi.fn(async () =>
      jsonResponse(
        {
          code: "invalid_argument",
          details: [
            {
              type: "google.rpc.ErrorInfo",
              value: encodeErrorInfo("FUTURE_REASON", "medallion.jimtech.io", {
                echo: payloadSecret,
                numericEcho: numericSecret.toString(),
              }),
            },
            {
              type: "example.future.Detail",
              value: Buffer.from(`echo:${payloadSecret}`, "utf8").toString(
                "base64",
              ),
            },
          ],
        },
        400,
        { "x-request-id": `request-${payloadSecret}` },
      ),
    );
    const client = ingestionClient(fetch);

    try {
      await client.cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "1",
        operation: "insert",
        idempotencyKey: "source:1",
        payload: {
          private: payloadSecret,
          numericPrivate: numericSecret.toString(),
        },
      });
      throw new Error("expected an API error");
    } catch (error) {
      expect(error).toBeInstanceOf(MedallionApiError);
      const apiError = error as MedallionApiError;
      const serialized = JSON.stringify(apiError);
      expect(apiError.errorInfoReason).toBe("FUTURE_REASON");
      expect(serialized).not.toContain(payloadSecret);
      expect(serialized).not.toContain(numericSecret.toString());
      expect(apiError.requestId).not.toContain(payloadSecret);
    }
  });

  it("does not retain credentials or payloads in network error causes", async () => {
    const secret = "credential_that_must_not_escape";
    const payloadSecret = "customer_payload_that_must_not_escape";
    const fetch = vi.fn(async (_url: RequestInfo | URL, init?: RequestInit) => {
      throw new Error(`${secret}:${String(init?.body)}`);
    });
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: secret,
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      fetch,
    });

    try {
      await client.cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "1",
        operation: "insert",
        idempotencyKey: "source:1",
        payload: { private: payloadSecret },
      });
      throw new Error("expected a network error");
    } catch (error) {
      expect(error).toMatchObject({ code: "MEDALLION_NETWORK_ERROR" });
      const cause = (error as Error & { cause?: Error }).cause;
      expect(cause?.message).toBe("A request error occurred.");
      expect(cause?.message).not.toContain(secret);
      expect(cause?.message).not.toContain(payloadSecret);
    }
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
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "scoped_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      retry: {
        maxAttempts: 3,
        initialDelayMs: 1_000,
        maxDelayMs: 1_000,
        jitterRatio: 0,
      },
      fetch,
    });
    await expect(
      client.cdc.record(
        {
          streamName: "orders",
          entityType: "order",
          entityId: "1",
          operation: "insert",
          idempotencyKey: "source:1",
        },
        { signal: controller.signal },
      ),
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
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "scoped_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      retry: {
        maxAttempts: 3,
        initialDelayMs: 0,
        maxDelayMs: 0,
        jitterRatio: 0,
      },
      fetch,
    });
    setTimeout(() => controller.abort(), 10);

    await expect(
      client.cdc.record(
        {
          streamName: "orders",
          entityType: "order",
          entityId: "1",
          operation: "insert",
          idempotencyKey: "source:1",
        },
        { signal: controller.signal },
      ),
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
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "scoped_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      retry: {
        maxAttempts: 3,
        initialDelayMs: 0,
        maxDelayMs: 0,
        jitterRatio: 0,
      },
      fetch,
    });
    setTimeout(() => controller.abort(), 10);

    await expect(
      client.cdc.record(
        {
          streamName: "orders",
          entityType: "order",
          entityId: "1",
          operation: "insert",
          idempotencyKey: "source:1",
        },
        { signal: controller.signal },
      ),
    ).rejects.toMatchObject({ code: "MEDALLION_ABORTED" });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("rejects non-JSON payloads without mutation or network I/O", async () => {
    const fetch = vi.fn();
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "scoped_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "connector_123",
      fetch,
    });
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    await expect(
      client.cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "1",
        operation: "insert",
        idempotencyKey: "source:1",
        payload: cyclic as never,
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_JSON_BODY" });
    await expect(
      client.cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "1",
        operation: "insert",
        idempotencyKey: "source:2",
        payload: { unsafe: Number.NaN },
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_JSON_BODY" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("preserves explicit JSON null and enforces descriptor field bounds locally", async () => {
    const fetch = vi.fn(async (_url: RequestInfo | URL, _init?: RequestInit) =>
      jsonResponse(receipt("source:null", "7")),
    );
    const client = ingestionClient(fetch);

    await client.cdc.record({
      streamName: "orders",
      entityType: "order",
      entityId: "1",
      operation: "insert",
      idempotencyKey: "source:null",
      payload: null,
    });
    const request = JSON.parse(String(fetch.mock.calls[0]?.[1]?.body)) as {
      events: Array<{ payloadJson: string }>;
    };
    expect(request.events[0]?.payloadJson).toBe("null");

    await expect(
      client.audit.record({
        resourceType: "order",
        resourceId: "",
        action: "order.update",
        outcome: "succeeded",
        idempotencyKey: "audit:empty-id",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_EVENT" });
    await expect(
      client.cdc.record({
        streamName: "x".repeat(257),
        entityType: "order",
        entityId: "1",
        operation: "insert",
        idempotencyKey: "source:long-stream",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_EVENT" });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("rejects malformed or internally inconsistent publish receipts", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          accepted_count: 1,
          events: [{ idempotency_key: "source:1" }],
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          accepted_count: 1,
          events: [
            {
              idempotency_key: "source:2",
              event_id: "2",
              duplicate: true,
            },
          ],
        }),
      );
    const client = ingestionClient(fetch);

    await expect(
      client.cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "1",
        operation: "insert",
        idempotencyKey: "source:1",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_PUBLISH_RESPONSE" });
    await expect(
      client.cdc.record({
        streamName: "orders",
        entityType: "order",
        entityId: "2",
        operation: "update",
        idempotencyKey: "source:2",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_PUBLISH_RESPONSE" });
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it.each(["0", "-1"])(
    "rejects a non-positive durable receipt ID (%s)",
    async (eventId) => {
      const fetch = vi.fn(async () =>
        jsonResponse(receipt("source:nonpositive", eventId), 200, {
          "x-request-id": "req_nonpositive",
        }),
      );
      const client = ingestionClient(fetch);

      await expect(
        client.cdc.record({
          streamName: "orders",
          entityType: "order",
          entityId: "1",
          operation: "insert",
          idempotencyKey: "source:nonpositive",
        }),
      ).rejects.toMatchObject({
        code: "MEDALLION_INVALID_PUBLISH_RESPONSE",
        requestId: "req_nonpositive",
      });
    },
  );

  it("rejects malformed CDC list projections with request metadata", async () => {
    const responses = [
      {
        id: "0",
        operation: "CDC_OPERATION_INSERT",
        payload_json: "{}",
      },
      { id: "1", operation: "CDC_OPERATION_INSERT", payload_json: "{" },
      { id: "1", payload_json: "{}" },
      {
        id: "1",
        stream_name: undefined,
        operation: "CDC_OPERATION_INSERT",
        payload_json: "{}",
      },
      {
        id: "1",
        entity_type: "",
        operation: "CDC_OPERATION_INSERT",
        payload_json: "{}",
      },
      {
        id: "1",
        entity_id: undefined,
        operation: "CDC_OPERATION_INSERT",
        payload_json: "{}",
      },
      {
        id: "1",
        idempotency_key: "",
        operation: "CDC_OPERATION_INSERT",
        payload_json: "{}",
      },
      {
        id: "1",
        stream_name: "x".repeat(257),
        operation: "CDC_OPERATION_INSERT",
        payload_json: "{}",
      },
      {
        id: "1",
        actor_principal: "x".repeat(513),
        operation: "CDC_OPERATION_INSERT",
        payload_json: "{}",
      },
      {
        id: "1",
        occurred_at: "1800-01-01T00:00:00Z",
        operation: "CDC_OPERATION_INSERT",
        payload_json: "{}",
      },
      {
        id: "1",
        observed_at: "2262-04-11T23:47:16.854775808Z",
        operation: "CDC_OPERATION_INSERT",
        payload_json: "{}",
      },
    ];
    const responseCount = responses.length;
    const fetch = vi.fn(async () =>
      jsonResponse(
        {
          events: responses.splice(0, 1).map((event) => ({
            stream_name: "orders",
            entity_type: "order",
            entity_id: "1",
            idempotency_key: "source:1",
            workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
            ...event,
          })),
        },
        200,
        { "x-request-id": "req_bad_cdc_list" },
      ),
    );
    const client = ingestionClient(fetch);

    for (let index = 0; index < responseCount; index += 1) {
      await expect(client.cdc.list()).rejects.toMatchObject({
        code: "MEDALLION_INVALID_LIST_RESPONSE",
        requestId: "req_bad_cdc_list",
      });
    }
  });

  it("rejects audit list records without concrete provenance or outcome", async () => {
    const responses = [
      { outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED" },
      { origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER" },
      {
        resource_type: undefined,
        origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
        outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
      },
      {
        resource_id: "",
        origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
        outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
      },
      {
        action: " ",
        origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
        outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
      },
      {
        idempotency_key: undefined,
        origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
        outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
      },
      {
        action: "x".repeat(257),
        origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
        outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
      },
      {
        actor_principal: "x".repeat(513),
        origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
        outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
      },
      {
        occurred_at: "1800-01-01T00:00:00Z",
        origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
        outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
      },
      {
        observed_at: "2262-04-11T23:47:16.854775808Z",
        origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
        outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
      },
    ];
    const responseCount = responses.length;
    const fetch = vi.fn(async () =>
      jsonResponse(
        {
          events: responses.splice(0, 1).map((event) => ({
            id: "1",
            resource_type: "order",
            resource_id: "1",
            action: "read",
            idempotency_key: "audit:1",
            payload_json: "{}",
            workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
            ...event,
          })),
        },
        200,
        { "x-request-id": "req_bad_audit_list" },
      ),
    );
    const client = ingestionClient(fetch);

    for (let index = 0; index < responseCount; index += 1) {
      await expect(client.audit.list()).rejects.toMatchObject({
        code: "MEDALLION_INVALID_LIST_RESPONSE",
        requestId: "req_bad_audit_list",
      });
    }
  });

  it("fails closed when a readback event belongs to another workspace", async () => {
    const fetch = vi.fn(async () =>
      jsonResponse(
        {
          events: [
            {
              id: "1",
              workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c4",
              stream_name: "orders",
              entity_type: "order",
              entity_id: "1",
              operation: "CDC_OPERATION_INSERT",
              idempotency_key: "source:1",
              payload_json: "{}",
            },
          ],
        },
        200,
        { "x-request-id": "req_wrong_workspace" },
      ),
    );

    await expect(ingestionClient(fetch).cdc.list()).rejects.toMatchObject({
      code: "MEDALLION_WORKSPACE_SELECTOR_CONFLICT",
      requestId: "req_wrong_workspace",
    });
  });

  it("validates low-level list events and preserves opaque whitespace keys", async () => {
    const malformedFetch = vi.fn(async () =>
      jsonResponse({
        events: [{ workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3" }],
      }),
    );
    await expect(
      ingestionClient(malformedFetch).connect.listCdcEvents({
        workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_LIST_RESPONSE" });

    const key = " \n\t";
    const fetch = vi.fn(async () =>
      jsonResponse({
        events: [
          {
            id: "1",
            workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
            stream_name: "orders",
            entity_type: "order",
            entity_id: "1",
            operation: "CDC_OPERATION_INSERT",
            idempotency_key: key,
            payload_json: "{}",
          },
        ],
      }),
    );
    const page = await ingestionClient(fetch).cdc.list();
    expect(page.events[0]?.idempotencyKey).toBe(key);
  });

  it("continues across empty pages and rejects repeated opaque cursors", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({ events: [], next_page_cursor: "opaque-2" }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          events: [
            {
              id: "9223372036854775807",
              workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
              stream_name: "orders",
              entity_type: "order",
              entity_id: "1",
              operation: "CDC_OPERATION_INSERT",
              idempotency_key: "source:1",
              payload_json: "{}",
            },
          ],
        }),
      );
    const client = ingestionClient(fetch);
    const events = [];
    for await (const event of client.cdc.iterate({
      streamName: "orders",
      limit: 25,
    })) {
      events.push(event);
    }
    expect(events[0]?.eventId).toBe("9223372036854775807");
    const secondRequest = JSON.parse(
      String(fetch.mock.calls[1]?.[1]?.body),
    ) as {
      connectorId: string;
      limit: number;
      pageCursor: string;
      streamName: string;
      workspaceId: string;
    };
    expect(secondRequest).toEqual({
      connectorId: "connector_123",
      limit: 25,
      pageCursor: "opaque-2",
      streamName: "orders",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
    });

    const repeatingFetch = vi.fn(async () =>
      jsonResponse({ events: [], next_page_cursor: "same-cursor" }),
    );
    const repeating = ingestionClient(repeatingFetch);
    const consume = async () => {
      for await (const _event of repeating.audit.iterate({
        cursor: "same-cursor",
      })) {
        // No events are expected.
      }
    };
    await expect(consume()).rejects.toMatchObject({
      code: "MEDALLION_REPEATED_CURSOR",
    });
    expect(repeatingFetch).toHaveBeenCalledTimes(1);
  });

  it("caps unique-cursor iteration at 10,000 pages", () => {
    expect(() =>
      assertIteratorPageWithinLimit(MAX_INGESTION_ITERATOR_PAGES),
    ).not.toThrow();
    expect(() =>
      assertIteratorPageWithinLimit(MAX_INGESTION_ITERATOR_PAGES + 1),
    ).toThrowError(
      expect.objectContaining({
        code: "MEDALLION_PAGINATION_LIMIT",
      }),
    );
  });

  it("creates stable source-derived keys", () => {
    expect(idempotencyKeyFromParts(" orders ", "partition-1", 42n)).toBe(
      "orders:103ec0f8-cc69-5f19-81d9-08f2d641a5e4",
    );
    expect(idempotencyKeyFromParts("orders", "partition-1", 42)).toBe(
      idempotencyKeyFromParts("orders", "partition-1", 42n),
    );
  });
});

function ingestionClient(fetch: FetchLike): MedallionClient {
  return new MedallionClient({
    baseUrl: "https://api.example.com",
    apiKey: "scoped_api_key",
    workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
    defaultConnectorId: "connector_123",
    fetch,
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
          operation: "PublishAuditEvents",
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

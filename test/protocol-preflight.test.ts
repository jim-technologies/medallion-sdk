import { describe, expect, it, vi } from "vitest";
import { MedallionClient } from "../src/index.js";
import type {
  ConnectAuditEventInput,
  ConnectCdcEventInput,
  ConnectPublishAuditEventsRequest,
  ConnectPublishCdcEventsRequest,
} from "../src/types.js";

function rawClient(fetch = vi.fn(async () => jsonResponse({}))) {
  return {
    client: new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      fetch,
      retry: { maxAttempts: 3, initialDelayMs: 0, maxDelayMs: 0 },
    }),
    fetch,
  };
}

function cdcEvent(
  overrides: Partial<ConnectCdcEventInput> = {},
): ConnectCdcEventInput {
  return {
    stream_name: "orders",
    entity_type: "order",
    entity_id: "1",
    operation: "CDC_OPERATION_INSERT",
    idempotency_key: "cdc:1",
    payload_json: "{}",
    ...overrides,
  };
}

function auditEvent(
  overrides: Partial<ConnectAuditEventInput> = {},
): ConnectAuditEventInput {
  return {
    resource_type: "order",
    resource_id: "1",
    action: "create",
    outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
    idempotency_key: "audit:1",
    payload_json: "{}",
    ...overrides,
  };
}

describe("ProtocolConnectClient request preflight", () => {
  it.each([
    "cdc:internal space",
    " cdc:surrounding whitespace\n",
    "cdc:\u0000control",
    "订单:一",
  ])("allows the opaque valid UTF-8 idempotency key %j", async (key) => {
    const fetch = vi.fn(async () =>
      jsonResponse({
        accepted_count: 1,
        duplicate_count: 0,
        events: [{ idempotency_key: key, event_id: "1" }],
      }),
    );
    const { client } = rawClient(fetch);

    await expect(
      client.connect.publishCdcEvents({
        connector_id: "conn_123",
        events: [cdcEvent({ idempotency_key: key })],
      }),
    ).resolves.toMatchObject({ body: { accepted_count: 1 } });
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it.each([
    {
      name: "missing connector",
      request: { connector_id: "", events: [cdcEvent()] },
      code: "MEDALLION_MISSING_CONNECTOR_ID",
    },
    {
      name: "empty batch",
      request: { connector_id: "conn_123", events: [] },
      code: "MEDALLION_INVALID_BATCH_SIZE",
    },
    {
      name: "missing event idempotency key",
      request: {
        connector_id: "conn_123",
        events: [
          {
            ...cdcEvent(),
            idempotency_key: undefined,
          },
        ],
      },
      code: "MEDALLION_MISSING_IDEMPOTENCY_KEY",
    },
    {
      name: "an unpaired Unicode surrogate in the idempotency key",
      request: {
        connector_id: "conn_123",
        events: [cdcEvent({ idempotency_key: "cdc:\ud800" })],
      },
      code: "MEDALLION_INVALID_IDEMPOTENCY_KEY",
    },
    {
      name: "duplicate scoped idempotency key",
      request: {
        connector_id: "conn_123",
        events: [cdcEvent(), cdcEvent({ entity_id: "2" })],
      },
      code: "MEDALLION_DUPLICATE_IDEMPOTENCY_KEY",
    },
    {
      name: "unspecified CDC operation",
      request: {
        connector_id: "conn_123",
        events: [cdcEvent({ operation: "CDC_OPERATION_UNSPECIFIED" })],
      },
      code: "MEDALLION_INVALID_CDC_OPERATION",
    },
    {
      name: "invalid JSON payload",
      request: {
        connector_id: "conn_123",
        events: [cdcEvent({ payload_json: "{" })],
      },
      code: "MEDALLION_INVALID_JSON_BODY",
    },
    {
      name: "out-of-range analytical timestamp",
      request: {
        connector_id: "conn_123",
        events: [cdcEvent({ occurred_at: "1899-12-31T23:59:59Z" })],
      },
      code: "MEDALLION_TIMESTAMP_OUT_OF_RANGE",
    },
    {
      name: "calendar-invalid timestamp",
      request: {
        connector_id: "conn_123",
        events: [cdcEvent({ occurred_at: "2026-02-30T00:00:00Z" })],
      },
      code: "MEDALLION_INVALID_TIMESTAMP",
    },
    {
      name: "server-derived workspace",
      request: {
        connector_id: "conn_123",
        events: [
          {
            ...cdcEvent(),
            workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c4",
          },
        ],
      },
      code: "MEDALLION_SERVER_DERIVED_FIELD",
    },
  ])("rejects $name before a retry-safe request", async ({ request, code }) => {
    const { client, fetch } = rawClient();

    await expect(
      client.connect.publishCdcEvents(
        request as unknown as ConnectPublishCdcEventsRequest,
      ),
    ).rejects.toMatchObject({ code });
    expect(fetch).not.toHaveBeenCalled();
  });

  it.each([
    {
      name: "duplicate audit idempotency key",
      request: {
        connector_id: "conn_123",
        events: [auditEvent(), auditEvent({ resource_id: "2" })],
      },
      code: "MEDALLION_DUPLICATE_IDEMPOTENCY_KEY",
    },
    {
      name: "unspecified audit outcome",
      request: {
        connector_id: "conn_123",
        events: [auditEvent({ outcome: "AUDIT_EVENT_OUTCOME_UNSPECIFIED" })],
      },
      code: "MEDALLION_INVALID_AUDIT_OUTCOME",
    },
    {
      name: "server-derived audit origin",
      request: {
        connector_id: "conn_123",
        events: [
          {
            ...auditEvent(),
            origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
          },
        ],
      },
      code: "MEDALLION_SERVER_DERIVED_FIELD",
    },
  ])("rejects $name before network I/O", async ({ request, code }) => {
    const { client, fetch } = rawClient();

    await expect(
      client.connect.publishAuditEvents(
        request as unknown as ConnectPublishAuditEventsRequest,
      ),
    ).rejects.toMatchObject({ code });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("validates raw list selectors, limits, filters, and time bounds", async () => {
    const { client, fetch } = rawClient();

    await expect(
      client.connect.listCdcEvents({ workspace_id: "", limit: 100 }),
    ).rejects.toMatchObject({ code: "MEDALLION_MISSING_WORKSPACE_ID" });
    await expect(
      client.connect.listCdcEvents({
        workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
        limit: 501,
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_PAGE_SIZE" });
    await expect(
      client.connect.listAuditEvents({
        workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
        resource_type: "order",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_AUDIT_FILTER" });
    await expect(
      client.connect.listAuditEvents({
        workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
        occurred_at_from: "2026-01-02T00:00:00Z",
        occurred_at_to: "2026-01-01T00:00:00Z",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_TIMESTAMP_RANGE" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("applies duplicate-key preflight to high-level batches", async () => {
    const { client, fetch } = rawClient();

    await expect(
      client.cdc.publishBatch({
        connectorId: "conn_123",
        events: [
          {
            streamName: "refunds",
            entityType: "order",
            entityId: "1",
            operation: "insert",
            idempotencyKey: "same",
          },
          {
            streamName: "orders",
            entityType: "order",
            entityId: "2",
            operation: "update",
            idempotencyKey: "same",
          },
        ],
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_DUPLICATE_IDEMPOTENCY_KEY" });
    await expect(
      client.audit.publishBatch({
        connectorId: "conn_123",
        events: [
          {
            resourceType: "order",
            resourceId: "1",
            action: "create",
            outcome: "succeeded",
            idempotencyKey: "same",
          },
          {
            resourceType: "order",
            resourceId: "2",
            action: "create",
            outcome: "succeeded",
            idempotencyKey: "same",
          },
        ],
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_DUPLICATE_IDEMPOTENCY_KEY" });
    expect(fetch).not.toHaveBeenCalled();
  });
});

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}

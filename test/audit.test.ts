import { describe, expect, it, vi } from "vitest";
import { MedallionClient } from "../src/index.js";

describe("audit.record", () => {
  it("normalizes IDs, publishes an audit event through connect, and does not mutate input", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) => {
        return new Response(
          JSON.stringify({
            accepted_count: 1,
            duplicate_count: 0,
            events: [
              {
                idempotency_key: "order_456_cancelled",
                event_id: "9223372036854775807",
                duplicate: false,
              },
            ],
          }),
          {
            status: 200,
            headers: { "content-type": "application/json" },
          },
        );
      },
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "conn_123",
      fetch,
    });
    const input = {
      actor: {
        type: "user",
        id: 123,
      },
      action: "cancel",
      outcome: "succeeded",
      resource: {
        type: "order",
        id: "000456",
      },
      before: { status: "confirmed" },
      after: { status: "cancelled" },
      metadata: { reason: "user_request" },
      idempotencyKey: "order_456_cancelled",
    } as const;

    const result = await client.audit.record(input);

    const [url, init] = fetch.mock.calls[0]!;
    const headers = init?.headers as Headers;
    const body = JSON.parse(String(init?.body)) as Record<string, unknown>;

    expect(url).toBe(
      "https://api.example.com/medallion.connect.v1.MedallionConnectService/PublishAuditEvents",
    );
    // Batch idempotency is carried by each event, not a misleading batch header.
    expect(headers.get("idempotency-key")).toBeNull();
    expect(headers.get("connect-protocol-version")).toBe("1");
    expect(body).toEqual({
      connectorId: "conn_123",
      events: [
        {
          resourceType: "order",
          resourceId: "000456",
          action: "cancel",
          outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
          idempotencyKey: "order_456_cancelled",
          actorPrincipal: "user:123",
          payloadJson:
            '{"actor":{"id":"123","type":"user"},"after":{"status":"cancelled"},"before":{"status":"confirmed"},"evidenceUrl":null,"metadata":{"reason":"user_request"},"resource":{"id":"000456","type":"order"}}',
        },
      ],
    });
    expect(result).toMatchObject({
      result: "accepted",
      idempotencyKey: "order_456_cancelled",
      duplicate: false,
      acceptedCount: 1,
      duplicateCount: 0,
      events: [
        {
          idempotencyKey: "order_456_cancelled",
          eventId: "9223372036854775807",
          duplicate: false,
        },
      ],
    });
    expect(input.actor.id).toBe(123);
  });

  it("reads audit trail events from Connect with server-side filters and cursor pagination", async () => {
    const payloadJson =
      '{"actor":{"type":"user","provider":"google","id":"payload_spoof"},' +
      '"before":{"status":"confirmed"},"after":{"status":"cancelled"},' +
      '"metadata":{"reason":"user_request"},' +
      '"decimal":1234567890.123456789}';
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) => {
        return new Response(
          JSON.stringify({
            events: [
              {
                id: "1",
                workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
                connector_id: "conn_123",
                resource_type: "order",
                resource_id: "order_123",
                idempotency_key: "audit:order_123:cancel",
                payload_json: payloadJson,
                actor_principal: "user:google:user_123",
                ingested_by_principal: "service_account:orders-worker",
                action: "cancel",
                description: "customer-visible audit",
                source_system: "orders",
                origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
                outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
                observed_at: "2026-07-07T00:00:00Z",
              },
            ],
            next_page_cursor: "cursor_2",
          }),
          {
            status: 200,
            headers: { "content-type": "application/json" },
          },
        );
      },
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "conn_123",
      fetch,
    });

    const result = await client.audit.trail({
      resourceType: "order",
      resourceId: "order_123",
      actor: { type: "user", provider: "google", id: "user_123" },
      ingesterPrincipal: "service_account:orders-worker",
      action: "cancel",
      origin: "external_provider",
      outcome: "succeeded",
      cursor: "cursor_1",
      limit: 25,
    });

    const [url, init] = fetch.mock.calls[0]!;
    const body = JSON.parse(String(init?.body)) as Record<string, unknown>;

    expect(url).toBe(
      "https://api.example.com/medallion.connect.v1.MedallionConnectService/ListAuditEvents",
    );
    expect(body).toEqual({
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      connectorId: "conn_123",
      resourceType: "order",
      resourceId: "order_123",
      limit: 25,
      actorPrincipal: "user:google:user_123",
      ingestedByPrincipal: "service_account:orders-worker",
      action: "cancel",
      origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
      outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
      pageCursor: "cursor_1",
    });
    expect(result.nextCursor).toBe("cursor_2");
    expect(result.events[0]).toMatchObject({
      id: "1",
      actor: { id: "user:google:user_123" },
      ingesterPrincipal: "service_account:orders-worker",
      actorPrincipal: "user:google:user_123",
      targetType: "order",
      targetId: "order_123",
      action: "cancel",
      description: "customer-visible audit",
      idempotencyKey: "audit:order_123:cancel",
      sourceSystem: "orders",
      origin: "external_provider",
      outcome: "succeeded",
      after: { status: "cancelled" },
      payloadJson,
    });
  });

  it("uses a matching structured actor to preserve colons in actor IDs", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        new Response(
          JSON.stringify({
            events: [
              {
                id: "1",
                workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
                connector_id: "conn_123",
                resource_type: "order",
                resource_id: "order_123",
                idempotency_key: "audit:actor:account_42",
                actor_principal: "user:account:42",
                payload_json: JSON.stringify({
                  actor: { type: "user", id: "account:42" },
                }),
                action: "cancel",
                origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
                outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
              },
            ],
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        ),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      fetch,
    });

    const result = await client.audit.trail({
      resourceType: "order",
      resourceId: "order_123",
    });

    expect(result.events[0]?.actor).toEqual({
      type: "user",
      id: "account:42",
    });
  });

  it("filters colon-bearing actors by their lossless canonical principal", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        new Response(
          JSON.stringify({
            events: [
              {
                id: "1",
                workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
                connector_id: "conn_123",
                resource_type: "order",
                resource_id: "order_123",
                actor_principal: "user:account:42",
                payload_json: "{}",
                idempotency_key: "audit:account:42",
                action: "read",
                origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
                outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
              },
            ],
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        ),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      fetch,
    });

    const result = await client.audit.list({
      actor: { type: "user", id: "account:42" },
    });

    const body = JSON.parse(String(fetch.mock.calls[0]?.[1]?.body)) as {
      actorPrincipal: string;
    };
    expect(body.actorPrincipal).toBe("user:account:42");
    expect(result.events).toHaveLength(1);
    expect(result.events[0]).toMatchObject({
      actorPrincipal: "user:account:42",
      actor: { id: "user:account:42" },
    });
  });

  it("keeps the wire actor authoritative when structured payload actor is spoofed", async () => {
    const fetch = vi.fn(
      async () =>
        new Response(
          JSON.stringify({
            events: [
              {
                id: "1",
                workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
                connector_id: "conn_123",
                resource_type: "order",
                resource_id: "order_123",
                idempotency_key: "audit:actor:spoofed",
                actor_principal: "user:account:42",
                payload_json: JSON.stringify({
                  actor: { type: "system", id: "attacker" },
                }),
                action: "cancel",
                origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
                outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
              },
            ],
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        ),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      fetch,
    });

    const result = await client.audit.trail({
      resourceType: "order",
      resourceId: "order_123",
    });

    expect(result.events[0]?.actor).toEqual({
      id: "user:account:42",
    });
  });

  it("defaults audit trail reads to a bounded page size", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) => {
        return new Response(JSON.stringify({ events: [] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      },
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      fetch,
    });

    await client.audit.trail({
      resourceType: "order",
      resourceId: "order_123",
    });

    const [, init] = fetch.mock.calls[0]!;
    const body = JSON.parse(String(init?.body)) as Record<string, unknown>;

    expect(body).toMatchObject({
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      resourceType: "order",
      resourceId: "order_123",
      limit: 100,
    });
  });

  it("rejects an empty successful publish acknowledgement", async () => {
    const fetch = vi.fn(
      async () =>
        new Response("{}", {
          status: 200,
          headers: {
            "content-type": "application/json",
            "x-request-id": "req_empty",
          },
        }),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await expect(
      client.audit.record({
        actor: { type: "user", id: "user_123" },
        action: "cancel",
        outcome: "succeeded",
        resource: { type: "order", id: "order_123" },
        idempotencyKey: "audit_empty_ack",
      }),
    ).rejects.toMatchObject({
      code: "MEDALLION_INVALID_PUBLISH_RESPONSE",
      requestId: "req_empty",
    });
  });

  it("validates audit enum values for untyped JavaScript callers", async () => {
    const fetch = vi.fn();
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await expect(
      client.audit.record({
        actor: { type: "user", id: "user_123" },
        action: "cancel",
        outcome: "unknown" as never,
        resource: { type: "order", id: "order_123" },
        idempotencyKey: "audit_invalid_outcome",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_AUDIT_OUTCOME" });

    await expect(
      client.audit.trail({
        resourceType: "order",
        resourceId: "order_123",
        origin: "unknown" as never,
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_AUDIT_ORIGIN" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("requires a caller-stable idempotency key before publishing", async () => {
    const fetch = vi.fn();
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await expect(
      client.audit.record({
        actor: { type: "user", id: "user_123" },
        action: "cancel",
        outcome: "succeeded",
        resource: { type: "order", id: "order_123" },
        idempotencyKey: "",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_MISSING_IDEMPOTENCY_KEY" });
    await expect(
      client.audit.record({
        actor: { type: "user", id: "user_123" },
        action: "cancel",
        outcome: "succeeded",
        resource: { type: "order", id: "order_123" },
        idempotencyKey: "x".repeat(513),
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_IDEMPOTENCY_KEY" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("treats zero as the omitted audit trail page size", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        new Response(JSON.stringify({ events: [] }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      fetch,
    });

    await client.audit.trail({
      resourceType: "order",
      resourceId: "order_123",
      limit: 0,
    });

    const [, init] = fetch.mock.calls[0]!;
    const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
    expect(body.limit).toBe(100);
  });

  it("rejects audit trail page sizes above the backend cap", async () => {
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      fetch: vi.fn(),
    });

    await expect(
      client.audit.trail({
        resourceType: "order",
        resourceId: "order_123",
        limit: 501,
      }),
    ).rejects.toMatchObject({
      code: "MEDALLION_INVALID_PAGE_SIZE",
    });
  });
});

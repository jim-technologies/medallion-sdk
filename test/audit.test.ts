import { describe, expect, it, vi } from "vitest";
import { MedallionClient } from "../src/index.js";

describe("audit.record", () => {
  it("normalizes IDs, publishes an audit event through connect, and does not mutate input", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => {
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
    });
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      defaultConnectorId: "conn_123",
      fetch,
    });
    const input = {
      actor: {
        type: "user",
        id: 123,
      },
      action: "order.cancelled",
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
    expect(headers.get("idempotency-key")).toBe("order_456_cancelled");
    expect(headers.get("connect-protocol-version")).toBe("1");
    expect(body).toEqual({
      connectorId: "conn_123",
      events: [
        {
          resourceType: "order",
          resourceId: "000456",
          action: "order.cancelled",
          idempotencyKey: "order_456_cancelled",
          actorPrincipal: "user:123",
          payloadJson: JSON.stringify({
            actor: { type: "user", id: "123" },
            resource: { type: "order", id: "000456" },
            before: { status: "confirmed" },
            after: { status: "cancelled" },
            metadata: { reason: "user_request" },
            evidenceUrl: null,
          }),
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
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => {
      return new Response(
        JSON.stringify({
          events: [
            {
              id: "1",
              organization_id: "org_123",
              connector_id: "conn_123",
              resource_type: "order",
              resource_id: "order_123",
              payload_json: JSON.stringify({
                actor: { type: "user", provider: "google", id: "user_123" },
                before: { status: "confirmed" },
                after: { status: "cancelled" },
                metadata: { reason: "user_request" },
              }),
              actor_principal: "user:google:user_123",
              ingested_by_principal: "service_account:orders-worker",
              action: "order.cancelled",
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
    });
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      organizationId: "org_123",
      defaultConnectorId: "conn_123",
      fetch,
    });

    const result = await client.audit.trail({
      resourceType: "order",
      resourceId: "order_123",
      actor: { type: "user", provider: "google", id: "user_123" },
      ingesterPrincipal: "service_account:orders-worker",
      action: "order.cancelled",
      cursor: "cursor_1",
      limit: 25,
    });

    const [url, init] = fetch.mock.calls[0]!;
    const body = JSON.parse(String(init?.body)) as Record<string, unknown>;

    expect(url).toBe(
      "https://api.example.com/medallion.connect.v1.MedallionConnectService/ListAuditEvents",
    );
    expect(body).toEqual({
      organizationId: "org_123",
      connectorId: "conn_123",
      resourceType: "order",
      resourceId: "order_123",
      limit: 25,
      actorPrincipal: "user:google:user_123",
      ingestedByPrincipal: "service_account:orders-worker",
      action: "order.cancelled",
      pageCursor: "cursor_1",
    });
    expect(result.nextCursor).toBe("cursor_2");
    expect(result.events[0]).toMatchObject({
      id: "1",
      actor: { type: "user", provider: "google", id: "user_123" },
      ingesterPrincipal: "service_account:orders-worker",
      actorPrincipal: "user:google:user_123",
      targetType: "order",
      targetId: "order_123",
      action: "order.cancelled",
      after: { status: "cancelled" },
    });
  });

  it("defaults audit trail reads to a bounded page size", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => {
      return new Response(JSON.stringify({ events: [] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      organizationId: "org_123",
      fetch,
    });

    await client.audit.trail({
      resourceType: "order",
      resourceId: "order_123",
    });

    const [, init] = fetch.mock.calls[0]!;
    const body = JSON.parse(String(init?.body)) as Record<string, unknown>;

    expect(body).toMatchObject({
      organizationId: "org_123",
      resourceType: "order",
      resourceId: "order_123",
      limit: 100,
    });
  });

  it("treats zero as the omitted audit trail page size", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(JSON.stringify({ events: [] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      }),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      organizationId: "org_123",
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
      organizationId: "org_123",
      fetch: vi.fn(),
    });

    await expect(
      client.audit.trail({
        resourceType: "order",
        resourceId: "order_123",
        limit: 501,
      }),
    ).rejects.toMatchObject({
      code: "MEDALLION_AUDIT_TRAIL_LIMIT_TOO_LARGE",
    });
  });
});

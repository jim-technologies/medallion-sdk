import { describe, expect, it, vi } from "vitest";
import { MedallionClient } from "../src/index.js";

describe("events.record", () => {
  it("normalizes optional actor and resource IDs and publishes through connect", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => {
      return new Response(JSON.stringify({
        accepted_count: 1,
        events: [{ idempotency_key: "checkout_started_123", event_id: 9 }],
      }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await client.events.record({
      type: "checkout.started",
      actor: {
        type: "user",
        id: 123,
      },
      resource: {
        type: "cart",
        id: 456n,
      },
      payload: {
        total: 42.5,
        currency: "USD",
      },
      idempotencyKey: "checkout_started_123",
    });

    const [, init] = fetch.mock.calls[0]!;
    const body = JSON.parse(String(init?.body)) as Record<string, unknown>;

    expect(body).toEqual({
      connectorId: "conn_123",
      events: [
        {
          streamName: "events",
          entityType: "cart",
          entityId: "456",
          idempotencyKey: "checkout_started_123",
          actorPrincipal: "user:123",
          payloadJson: JSON.stringify({
            type: "checkout.started",
            actor: { type: "user", id: "123" },
            resource: { type: "cart", id: "456" },
            payload: {
              total: 42.5,
              currency: "USD",
            },
          }),
          kind: "EVENT_KIND_AUDIT",
          action: "checkout.started",
        },
      ],
    });
  });
});

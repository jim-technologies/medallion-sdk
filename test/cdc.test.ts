import { describe, expect, it, vi } from "vitest";
import { MedallionClient } from "../src/index.js";

describe("cdc.record", () => {
  it("normalizes CDC primary key values and publishes through connect", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => {
      return new Response(JSON.stringify({
        accepted_count: 1,
        duplicate_count: 0,
        events: [{ idempotency_key: "orders_update_123", event_id: 42 }],
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
    const input = {
      source: "primary_postgres",
      table: "orders",
      operation: "update",
      primaryKey: {
        id: "order_123",
        shard: 7,
      },
      before: { status: "confirmed" },
      after: { status: "cancelled" },
      idempotencyKey: "orders_update_123",
    } as const;

    await client.cdc.record(input);

    const [url, init] = fetch.mock.calls[0]!;
    const body = JSON.parse(String(init?.body)) as Record<string, unknown>;

    expect(url).toBe(
      "https://api.example.com/medallion.connect.v1.MedallionConnectService/PublishCdcEvents",
    );
    expect(body).toEqual({
      connectorId: "conn_123",
      events: [
        {
          streamName: "orders",
          entityType: "orders",
          entityId: "order_123",
          operation: "CDC_OPERATION_UPDATE",
          idempotencyKey: "orders_update_123",
          payloadJson: JSON.stringify({
            source: "primary_postgres",
            table: "orders",
            primaryKey: {
              id: "order_123",
              shard: "7",
            },
            before: { status: "confirmed" },
            after: { status: "cancelled" },
          }),
          kind: "EVENT_KIND_CDC",
        },
      ],
    });
    expect(input.primaryKey.shard).toBe(7);
  });
});

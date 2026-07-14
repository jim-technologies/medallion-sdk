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
          entityId: JSON.stringify({ id: "order_123", shard: "7" }),
          operation: "CDC_OPERATION_UPDATE",
          idempotencyKey: "orders_update_123",
          payloadJson: JSON.stringify({
            source: "primary_postgres",
            table: "orders",
            actor: null,
            primaryKey: {
              id: "order_123",
              shard: "7",
            },
            before: { status: "confirmed" },
            after: { status: "cancelled" },
            metadata: null,
          }),
        },
      ],
    });
    expect(input.primaryKey.shard).toBe(7);
  });

  it("derives collision-free entity IDs from every composite key field", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => {
      return new Response(
        JSON.stringify({ accepted_count: 1, events: [{ event_id: 1 }] }),
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

    await client.cdc.record({
      source: "primary_postgres",
      table: "orders",
      operation: "update",
      primaryKey: { tenant_id: "tenant_a", id: "1" },
      idempotencyKey: "tenant_a_order_1",
    });
    await client.cdc.record({
      source: "primary_postgres",
      table: "orders",
      operation: "update",
      primaryKey: { id: "1", tenant_id: "tenant_b" },
      idempotencyKey: "tenant_b_order_1",
    });

    const entityIds = fetch.mock.calls.map(([, init]) => {
      const body = JSON.parse(String(init?.body)) as {
        events: Array<{ entityId: string }>;
      };
      return body.events[0]!.entityId;
    });

    expect(entityIds).toEqual([
      JSON.stringify({ id: "1", tenant_id: "tenant_a" }),
      JSON.stringify({ id: "1", tenant_id: "tenant_b" }),
    ]);
    expect(new Set(entityIds).size).toBe(2);
  });

  it("keeps numeric-looking composite key names lexicographically ordered", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(
        JSON.stringify({ accepted_count: 1, events: [{ event_id: 1 }] }),
        { status: 200, headers: { "content-type": "application/json" } },
      ),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await client.cdc.record({
      source: "primary_postgres",
      table: "quoted_columns",
      operation: "update",
      primaryKey: { "2": "second", "10": "first" },
    });

    const [, init] = fetch.mock.calls[0]!;
    const body = JSON.parse(String(init?.body)) as {
      events: Array<{ entityId: string }>;
    };
    expect(body.events[0]!.entityId).toBe('{"10":"first","2":"second"}');
  });

  it("rejects an empty primary key before making a request", async () => {
    const fetch = vi.fn();
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await expect(
      client.cdc.record({
        source: "primary_postgres",
        table: "orders",
        operation: "update",
        primaryKey: {},
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_EMPTY_CDC_PRIMARY_KEY" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("rejects an empty primary key even when entityId is explicit", async () => {
    const fetch = vi.fn();
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await expect(
      client.cdc.record({
        source: "primary_postgres",
        table: "orders",
        operation: "update",
        primaryKey: {},
        entityId: "order_123",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_EMPTY_CDC_PRIMARY_KEY" });
    expect(fetch).not.toHaveBeenCalled();
  });
});

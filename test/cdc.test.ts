import { describe, expect, it, vi } from "vitest";
import { MedallionClient } from "../src/index.js";

describe("cdc.record", () => {
  it("normalizes CDC primary key values and publishes through connect", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) => {
        return new Response(
          JSON.stringify({
            accepted_count: 1,
            duplicate_count: 0,
            events: [{ idempotency_key: "orders_update_123", event_id: 42 }],
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
      source: "primary_postgres",
      table: "orders",
      operation: "update",
      primaryKey: {
        id: "order_123",
        shard: 7,
      },
      entityId: "order_123",
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
          payloadJson:
            '{"actor":null,"after":{"status":"cancelled"},"before":{"status":"confirmed"},"metadata":null,"primaryKey":{"id":"order_123","shard":"7"},"source":"primary_postgres","table":"orders"}',
        },
      ],
    });
    expect(input.primaryKey.shard).toBe(7);
  });

  it("uses caller-canonical entity IDs for composite keys", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, init?: RequestInit) => {
        const request = JSON.parse(String(init?.body)) as {
          events: Array<{ idempotencyKey: string }>;
        };
        return new Response(
          JSON.stringify({
            accepted_count: 1,
            events: [
              {
                idempotency_key: request.events[0]?.idempotencyKey,
                event_id: 1,
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

    await client.cdc.record({
      source: "primary_postgres",
      table: "orders",
      operation: "update",
      primaryKey: { account_id: "account_a", id: "1" },
      entityId: "account_a/order/1",
      idempotencyKey: "account_a_order_1",
    });
    await client.cdc.record({
      source: "primary_postgres",
      table: "orders",
      operation: "update",
      primaryKey: { id: "1", account_id: "account_b" },
      entityId: "account_b/order/1",
      idempotencyKey: "account_b_order_1",
    });

    const entityIds = fetch.mock.calls.map(([, init]) => {
      const body = JSON.parse(String(init?.body)) as {
        events: Array<{ entityId: string }>;
      };
      return body.events[0]!.entityId;
    });

    expect(entityIds).toEqual(["account_a/order/1", "account_b/order/1"]);
    expect(new Set(entityIds).size).toBe(2);
  });

  it("preserves composite key fields while using an explicit entity ID", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) =>
        new Response(
          JSON.stringify({
            accepted_count: 1,
            events: [{ idempotency_key: "cdc_ordered_key", event_id: 1 }],
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        ),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await client.cdc.record({
      source: "primary_postgres",
      table: "quoted_columns",
      operation: "update",
      primaryKey: { "2": "second", "10": "first" },
      entityId: "quoted-columns/first/second",
      idempotencyKey: "cdc_ordered_key",
    });

    const [, init] = fetch.mock.calls[0]!;
    const body = JSON.parse(String(init?.body)) as {
      events: Array<{ entityId: string }>;
    };
    expect(body.events[0]!.entityId).toBe("quoted-columns/first/second");
  });

  it("rejects a composite primary key without an explicit entity ID", async () => {
    const fetch = vi.fn();
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await expect(
      client.cdc.record({
        source: "primary_postgres",
        table: "orders",
        operation: "update",
        primaryKey: { account_id: "account_a", id: "1" },
        idempotencyKey: "cdc_composite_without_entity",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_MISSING_CDC_ENTITY_ID" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("rejects an empty primary key before making a request", async () => {
    const fetch = vi.fn();
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await expect(
      client.cdc.record({
        source: "primary_postgres",
        table: "orders",
        operation: "update",
        primaryKey: {},
        idempotencyKey: "cdc_empty_key",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_EMPTY_CDC_PRIMARY_KEY" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("rejects an empty primary key even when entityId is explicit", async () => {
    const fetch = vi.fn();
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
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
        idempotencyKey: "cdc_empty_key_with_entity",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_EMPTY_CDC_PRIMARY_KEY" });
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
      client.cdc.record({
        source: "primary_postgres",
        table: "orders",
        operation: "update",
        primaryKey: { id: "order_123" },
        idempotencyKey: "",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_MISSING_IDEMPOTENCY_KEY" });
    await expect(
      client.cdc.record({
        source: "primary_postgres",
        table: "orders",
        operation: "update",
        primaryKey: { id: "order_123" },
        idempotencyKey: "x".repeat(513),
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_IDEMPOTENCY_KEY" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("validates operation values for untyped JavaScript callers", async () => {
    const fetch = vi.fn();
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      workspaceId: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await expect(
      client.cdc.record({
        source: "primary_postgres",
        table: "orders",
        operation: "unknown" as never,
        primaryKey: { id: "order_123" },
        idempotencyKey: "cdc_invalid_operation",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_CDC_OPERATION" });
    expect(fetch).not.toHaveBeenCalled();
  });

  it("preserves opaque actor principals without ambiguous reverse parsing", async () => {
    const fetch = vi.fn(
      async () =>
        new Response(
          JSON.stringify({
            events: [
              {
                id: "1",
                workspace_id: "ws_01jz9q5g6rsf7r5ar4rah1b2c3",
                stream_name: "orders",
                entity_type: "order",
                entity_id: "1",
                operation: "CDC_OPERATION_INSERT",
                actor_principal: "user:account:42",
                idempotency_key: "cdc:1",
                payload_json: "{}",
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

    const page = await client.cdc.list({
      actor: { type: "user", id: "account:42" },
    });

    expect(page.events[0]).toMatchObject({
      actorPrincipal: "user:account:42",
      actor: { id: "user:account:42" },
    });
  });
});

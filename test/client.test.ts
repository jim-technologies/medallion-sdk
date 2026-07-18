import { describe, expect, it, vi } from "vitest";
import { MedallionClient } from "../src/index.js";

describe("MedallionClient", () => {
  it("exposes the initial medallion-connect API groups", () => {
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      fetch: vi.fn(),
    });

    expect(client.audit.record).toBeTypeOf("function");
    expect(client.cdc.record).toBeTypeOf("function");
    expect(client.connect.registerDatasource).toBeTypeOf("function");
    expect(client.datasources.register).toBeTypeOf("function");
    expect(client.ontology.query).toBeTypeOf("function");
    expect(client.ontology.planAction).toBeTypeOf("function");
    expect(client.ontology.executeAction).toBeTypeOf("function");
    expect(client.storage.upload).toBeTypeOf("function");
    expect(client.protocol.connect.publishCdcEvents).toBeTypeOf("function");
    expect(client.protocol.connect.publishAuditEvents).toBeTypeOf("function");
    expect(client.protocol.connect.listAuditEvents).toBeTypeOf("function");
    expect(client.generated.connect.publishCdcEvents).toBeTypeOf("function");
    expect("events" in client).toBe(false);
  });

  it("supports base URLs with a path prefix", async () => {
    const fetch = vi.fn(
      async (_input: RequestInfo | URL, _init?: RequestInit) => {
        return new Response(JSON.stringify({ accepted_count: 1 }), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      },
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com/connect",
      apiKey: "test_api_key",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await client.audit.record({
      actor: { type: "user", id: "user_123" },
      action: "start",
      outcome: "succeeded",
      resource: { type: "checkout", id: "checkout_123" },
      idempotencyKey: "checkout_started_123",
    });

    expect(fetch.mock.calls[0]?.[0]).toBe(
      "https://api.example.com/connect/medallion.connect.v1.MedallionConnectService/PublishAuditEvents",
    );
  });
});

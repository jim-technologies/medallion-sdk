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
    expect(client.events.record).toBeTypeOf("function");
    expect(client.cdc.record).toBeTypeOf("function");
    expect(client.connect.registerDatasource).toBeTypeOf("function");
    expect(client.datasources.register).toBeTypeOf("function");
    expect(client.ontology.query).toBeTypeOf("function");
    expect(client.ontology.planAction).toBeTypeOf("function");
    expect(client.ontology.executeAction).toBeTypeOf("function");
    expect(client.storage.upload).toBeTypeOf("function");
    expect(client.protocol.connect.publishCdcEvents).toBeTypeOf("function");
    expect(client.generated.connect.publishCdcEvents).toBeTypeOf("function");
  });

  it("supports base URLs with a path prefix", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => {
      return new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    });
    const client = new MedallionClient({
      baseUrl: "https://api.example.com/connect",
      apiKey: "test_api_key",
      defaultConnectorId: "conn_123",
      fetch,
    });

    await client.events.record({
      type: "checkout.started",
      idempotencyKey: "checkout_started_123",
    });

    expect(fetch.mock.calls[0]?.[0]).toBe(
      "https://api.example.com/connect/medallion.connect.v1.MedallionConnectService/PublishCdcEvents",
    );
  });
});

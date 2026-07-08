import { describe, expect, it, vi } from "vitest";
import { MedallionClient } from "../src/index.js";

describe("datasources.register", () => {
  it("registers datasources through the real connect RegisterConnector route", async () => {
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => {
      return new Response(JSON.stringify({
        connector: {
          id: "conn_123",
          organization_id: "org_123",
          kind: "postgres",
          source_system: "primary_postgres",
          display_name: "Primary Postgres",
          external_id: "00042",
          status: "LIFECYCLE_STATUS_ACTIVE",
        },
      }), {
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

    const result = await client.connect.registerDatasource({
      name: "primary_postgres",
      type: "postgres",
      displayName: "Primary Postgres",
      externalId: "00042",
      metadata: {
        environment: "production",
      },
    });

    const [url, init] = fetch.mock.calls[0]!;
    const body = JSON.parse(String(init?.body)) as Record<string, unknown>;

    expect(url).toBe(
      "https://api.example.com/medallion.connect.v1.MedallionConnectService/RegisterConnector",
    );
    expect(body).toEqual({
      organizationId: "org_123",
      kind: "postgres",
      sourceSystem: "primary_postgres",
      displayName: "Primary Postgres",
      externalId: "00042",
    });
    expect(result.datasource).toMatchObject({
      id: "conn_123",
      organizationId: "org_123",
      kind: "postgres",
      sourceSystem: "primary_postgres",
      displayName: "Primary Postgres",
      externalId: "00042",
      metadata: {
        environment: "production",
      },
    });
  });
});

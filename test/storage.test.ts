import { describe, expect, it, vi } from "vitest";
import { MedallionClient } from "../src/index.js";

describe("storage.upload", () => {
  it("returns the canonical location selected by storage", async () => {
    const fetch = vi.fn(
      async () =>
        new Response(
          JSON.stringify({
            org: "canonical_org",
            path: "evidence/orders/order_123.json",
            entry: {
              filename: "order_123.json",
              size_bytes: "18",
              metadata: {
                cache_control: "private, max-age=60",
                user: {
                  evidence_type: "order",
                },
              },
            },
          }),
          {
            status: 200,
            headers: {
              "content-type": "application/json",
              "x-request-id": "req_upload",
            },
          },
        ),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      fetch,
    });

    const result = await client.storage.upload({
      org: "requested_org",
      repo: "evidence",
      path: "orders/order_123.json",
      data: "{}",
      contentType: "application/json",
    });

    expect(result).toMatchObject({
      requestId: "req_upload",
      result: "uploaded",
      org: "canonical_org",
      path: "evidence/orders/order_123.json",
      entry: {
        filename: "order_123.json",
        size_bytes: "18",
        metadata: {
          cache_control: "private, max-age=60",
          user: {
            evidence_type: "order",
          },
        },
      },
    });
  });

  it("rejects an empty successful upload response", async () => {
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      fetch: vi.fn(
        async () =>
          new Response("{}", {
            status: 200,
            headers: { "content-type": "application/json" },
          }),
      ),
    });

    await expect(
      client.storage.upload({
        org: "org_123",
        path: "evidence/order_123.json",
        data: "{}",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_STORAGE_RESPONSE" });
  });
});

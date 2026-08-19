import { readFile } from "node:fs/promises";

import { describe, expect, it, vi } from "vitest";

import { MedallionClient } from "../src/index.js";

interface IngestionFixture {
  id: string;
  protocol: {
    method: "PublishCdcEvents" | "PublishAuditEvents";
    path: string;
  };
  input: {
    body: Record<string, unknown>;
    headers: Record<string, string>;
  };
  expected: {
    body: {
      acceptedCount: number;
      duplicateCount: number;
      events: Array<{
        idempotencyKey: string;
        eventId: string;
        duplicate?: boolean;
      }>;
    };
    httpStatus: number;
  };
}

describe("official external SDK ingestion conformance", () => {
  it("executes every reviewed transport exchange through the client", async () => {
    const document = JSON.parse(
      await readFile(
        new URL(
          "../proto/external-ingestion-contract/v1/conformance/external-sdk-ingestion.json",
          import.meta.url,
        ),
        "utf8",
      ),
    ) as { category: string; fixtures: IngestionFixture[] };
    expect(document.category).toBe("external_sdk_ingestion");
    expect(document.fixtures).toHaveLength(2);

    for (const fixture of document.fixtures) {
      const fetch = vi.fn(
        async (_url: RequestInfo | URL, _init?: RequestInit) =>
          new Response(JSON.stringify(fixture.expected.body), {
            status: fixture.expected.httpStatus,
            headers: { "content-type": "application/json" },
          }),
      );
      const client = new MedallionClient({
        baseUrl: "https://api.example.com",
        apiKey: "fixture_non_secret_api_key",
        workspaceId: fixture.input.headers["x-medallion-workspace-id"]!,
        fetch,
      });

      const response =
        fixture.protocol.method === "PublishCdcEvents"
          ? await client.connect.publishCdcEvents(fixture.input.body as never)
          : await client.connect.publishAuditEvents(
              fixture.input.body as never,
            );

      const [url, init] = fetch.mock.calls[0]!;
      expect(String(url), fixture.id).toBe(
        `https://api.example.com${fixture.protocol.path}`,
      );
      expect(JSON.parse(String(init?.body)), fixture.id).toEqual(
        fixture.input.body,
      );
      const headers = init?.headers as Headers;
      expect(headers.get("content-type"), fixture.id).toBe("application/json");
      expect(headers.get("x-medallion-workspace-id"), fixture.id).toBe(
        fixture.input.headers["x-medallion-workspace-id"],
      );
      expect(response.body.accepted_count, fixture.id).toBe(
        fixture.expected.body.acceptedCount,
      );
      expect(response.body.duplicate_count, fixture.id).toBe(
        fixture.expected.body.duplicateCount,
      );
      expect(
        response.body.events?.map((event) => ({
          idempotencyKey: event.idempotency_key,
          eventId: String(event.event_id),
          duplicate: event.duplicate ?? false,
        })),
        fixture.id,
      ).toEqual(
        fixture.expected.body.events.map((event) => ({
          idempotencyKey: event.idempotencyKey,
          eventId: event.eventId,
          duplicate: event.duplicate ?? false,
        })),
      );
    }
  });
});

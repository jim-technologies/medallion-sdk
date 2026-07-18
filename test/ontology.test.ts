import { describe, expect, it, vi } from "vitest";
import { MedallionClient } from "../src/index.js";

describe("ontology", () => {
  it("maps query evidence into the ergonomic audit event shape", async () => {
    const fetch = vi.fn(
      async () =>
        new Response(
          JSON.stringify({
            answer: "Order was cancelled.",
            resource_ids: ["order_123"],
            explanations: ["audit evidence"],
            events: [
              {
                id: "audit_123",
                tenant_id: "org_123",
                actor_principal: "user:google:user_123",
                action: "cancel",
                target_type: "order",
                target_id: "order_123",
                request_id: "req_source",
                metadata: { reason: "user_request" },
                created_at: "2026-07-16T00:00:00Z",
                before: { status: "confirmed" },
                after: { status: "cancelled" },
                evidence_url: "https://evidence.example/order_123",
                source_event_id: "source_123",
              },
            ],
          }),
          {
            status: 200,
            headers: {
              "content-type": "application/json",
              "x-request-id": "req_query",
            },
          },
        ),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      fetch,
    });

    const result = await client.ontology.query({
      question: "Who cancelled order 123?",
    });

    expect(result).toMatchObject({
      requestId: "req_query",
      answer: "Order was cancelled.",
      resourceIds: ["order_123"],
      explanations: ["audit evidence"],
      events: [
        {
          id: "audit_123",
          eventId: "audit_123",
          tenantId: "org_123",
          actor: { type: "user", provider: "google", id: "user_123" },
          actorPrincipal: "user:google:user_123",
          action: "cancel",
          targetType: "order",
          targetId: "order_123",
          entityType: "order",
          entityId: "order_123",
          requestId: "req_source",
          metadata: { reason: "user_request" },
          createdAt: "2026-07-16T00:00:00Z",
          before: { status: "confirmed" },
          after: { status: "cancelled" },
          evidenceUrl: "https://evidence.example/order_123",
          sourceEventId: "source_123",
        },
      ],
    });
    expect(result.events[0]).not.toHaveProperty("tenant_id");
  });

  it("does not invent duplicate metadata for action execution", async () => {
    const fetch = vi.fn(
      async () =>
        new Response(
          JSON.stringify({
            invocation: {
              id: "invoke_1",
              tenant_id: "org_123",
              action_name: "order.cancel",
              actor_principal: "user:google:user_123",
              idempotency_key: "action_1",
              status: "ACTION_INVOCATION_STATUS_SUCCEEDED",
            },
          }),
          {
            status: 200,
            headers: { "content-type": "application/json" },
          },
        ),
    );
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      fetch,
    });

    const result = await client.ontology.executeAction({
      actionName: "order.cancel",
      idempotencyKey: "action_1",
    });

    expect(result).toMatchObject({
      idempotencyKey: "action_1",
      result: "succeeded",
    });
    expect(result).not.toHaveProperty("duplicate");
  });

  it("rejects an empty successful action response", async () => {
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
      client.ontology.executeAction({
        actionName: "order.cancel",
        idempotencyKey: "action_1",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_ACTION_RESPONSE" });
  });

  it("rejects an empty successful action plan", async () => {
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
      client.ontology.planAction({ actionName: "order.cancel" }),
    ).rejects.toMatchObject({ code: "MEDALLION_INVALID_ACTION_RESPONSE" });
  });

  it("requires a caller-stable action idempotency key", async () => {
    const fetch = vi.fn();
    const client = new MedallionClient({
      baseUrl: "https://api.example.com",
      apiKey: "test_api_key",
      fetch,
    });

    await expect(
      client.ontology.executeAction({
        actionName: "order.cancel",
        idempotencyKey: " ",
      }),
    ).rejects.toMatchObject({ code: "MEDALLION_MISSING_IDEMPOTENCY_KEY" });
    expect(fetch).not.toHaveBeenCalled();
  });
});

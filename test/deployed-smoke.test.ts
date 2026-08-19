import { describe, expect, it } from "vitest";
import { MedallionApiError, MedallionClient } from "../src/index.js";

const requiredEnv = [
  "MEDALLION_SMOKE_BASE_URL",
  "MEDALLION_SMOKE_API_KEY",
  "MEDALLION_SMOKE_WORKSPACE_ID",
  "MEDALLION_SMOKE_CONNECTOR_ID",
] as const;

const smokeEnabled = requiredEnv.every((name) => process.env[name]?.trim());

describe.skipIf(!smokeEnabled)("deployed SDK smoke", () => {
  it("publishes and reads back an audit event", async () => {
    const baseUrl = requiredEnvValue("MEDALLION_SMOKE_BASE_URL");
    const apiKey = requiredEnvValue("MEDALLION_SMOKE_API_KEY");
    const workspaceId = requiredEnvValue("MEDALLION_SMOKE_WORKSPACE_ID");
    const connectorId = requiredEnvValue("MEDALLION_SMOKE_CONNECTOR_ID");
    const expectedIngester =
      process.env.MEDALLION_SMOKE_EXPECTED_INGESTER_PRINCIPAL?.trim();
    const deniedWorkspaceId =
      process.env.MEDALLION_SMOKE_DENIED_WORKSPACE_ID?.trim();
    const runId = `sdk_smoke_${Date.now()}`;
    const actor = { type: "user", id: `user_${runId}` } as const;
    const resource = { type: "order", id: `order_${runId}` } as const;
    const action = "cancel";
    const evidenceUrl =
      process.env.MEDALLION_SMOKE_EVIDENCE_URL?.trim() ||
      `https://example.com/deployments/medallion-sdk-smoke/${runId}`;

    const client = new MedallionClient({
      baseUrl,
      apiKey,
      workspaceId,
      defaultConnectorId: connectorId,
    });

    await client.audit.record({
      actor,
      action,
      outcome: "succeeded",
      resourceType: resource.type,
      resourceId: resource.id,
      payload: {
        before: { status: "confirmed" },
        after: { status: "cancelled" },
        metadata: { smokeRunId: runId },
        evidenceUrl,
      },
      idempotencyKey: `audit_${runId}`,
      sourceEventId: `source_${runId}`,
    });

    const event = await eventually(async () => {
      const trail = await client.audit.list({
        connectorId,
        resourceType: resource.type,
        resourceId: resource.id,
        action,
        origin: "external_provider",
        outcome: "succeeded",
        limit: 25,
      });
      return trail.events.find((candidate) => {
        return (
          candidate.sourceEventId === `source_${runId}` ||
          candidate.metadata?.smokeRunId === runId
        );
      });
    });

    expect(event).toMatchObject({
      actor,
      action,
      origin: "external_provider",
      outcome: "succeeded",
      targetType: resource.type,
      targetId: resource.id,
      before: { status: "confirmed" },
      after: { status: "cancelled" },
      evidenceUrl,
    });
    if (expectedIngester) {
      expect(event.ingesterPrincipal).toBe(expectedIngester);
    }

    if (deniedWorkspaceId) {
      const deniedClient = new MedallionClient({
        baseUrl,
        apiKey,
        workspaceId: deniedWorkspaceId,
        defaultConnectorId: connectorId,
      });
      await expect(
        deniedClient.audit.trail({
          connectorId,
          resourceType: resource.type,
          resourceId: resource.id,
          limit: 1,
        }),
      ).rejects.toSatisfy(
        (error: unknown) =>
          error instanceof MedallionApiError &&
          (error.status === 401 || error.status === 403),
      );
    }
  }, 60_000);
});

function requiredEnvValue(name: (typeof requiredEnv)[number]): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required for deployed smoke tests.`);
  }
  return value;
}

async function eventually<T>(
  read: () => Promise<T | undefined>,
  timeoutMs = 30_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let last: T | undefined;

  while (Date.now() < deadline) {
    last = await read();
    if (last !== undefined) {
      return last;
    }
    await new Promise((resolve) => setTimeout(resolve, 1_000));
  }

  throw new Error(
    `Expected deployed smoke event was not visible after ${timeoutMs} ms.`,
  );
}

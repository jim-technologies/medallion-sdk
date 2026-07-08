import { describe, expect, it } from "vitest";
import { MedallionClient, MedallionApiError } from "../src/index.js";

const requiredEnv = [
  "MEDALLION_SMOKE_BASE_URL",
  "MEDALLION_SMOKE_ACCESS_TOKEN",
  "MEDALLION_SMOKE_ORGANIZATION_ID",
  "MEDALLION_SMOKE_EXPECTED_INGESTER_PRINCIPAL",
] as const;

const smokeEnabled = requiredEnv.every((name) => process.env[name]?.trim());

describe.skipIf(!smokeEnabled)("deployed SDK smoke", () => {
  it(
    "registers a datasource and round-trips an audit trail event",
    async () => {
      const baseUrl = requiredEnvValue("MEDALLION_SMOKE_BASE_URL");
      const accessToken = requiredEnvValue("MEDALLION_SMOKE_ACCESS_TOKEN");
      const organizationId = requiredEnvValue("MEDALLION_SMOKE_ORGANIZATION_ID");
      const expectedIngester = requiredEnvValue(
        "MEDALLION_SMOKE_EXPECTED_INGESTER_PRINCIPAL",
      );
      const runId = `sdk_smoke_${Date.now()}`;
      const actor = { type: "user", id: `user_${runId}` } as const;
      const resource = { type: "order", id: `order_${runId}` } as const;
      const action = "order.cancelled";
      const evidenceUrl =
        process.env.MEDALLION_SMOKE_EVIDENCE_URL ??
        `https://github.com/jim-technologies/medallion-sdk/tree/${process.env.MEDALLION_SDK_REF ?? "v0.1.0"}`;

      const client = new MedallionClient({
        baseUrl,
        accessToken,
        organizationId,
      });

      const connectorId =
        process.env.MEDALLION_SMOKE_CONNECTOR_ID ??
        (
          await client.connect.registerDatasource({
            name: `medallion_sdk_smoke_${runId}`,
            type: "medallion_audit_logs",
            displayName: `Medallion SDK Smoke ${runId}`,
            externalId: runId,
          })
        ).datasource.id;

      await client.audit.record({
        connectorId,
        actor,
        action,
        resource,
        before: { status: "confirmed" },
        after: { status: "cancelled" },
        metadata: { smokeRunId: runId },
        evidenceUrl,
        idempotencyKey: `audit_${runId}`,
        sourceEventId: `source_${runId}`,
      });

      const event = await eventually(async () => {
        const trail = await client.audit.trail({
          connectorId,
          resourceType: resource.type,
          resourceId: resource.id,
          action,
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
        ingesterPrincipal: expectedIngester,
        action,
        targetType: resource.type,
        targetId: resource.id,
        before: { status: "confirmed" },
        after: { status: "cancelled" },
        evidenceUrl,
      });

      const deniedOrg = process.env.MEDALLION_SMOKE_DENIED_ORGANIZATION_ID;
      if (deniedOrg?.trim()) {
        await expect(
          client.audit.trail({
            organizationId: deniedOrg,
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
    },
    60_000,
  );
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

  throw new Error(`Expected deployed smoke event was not visible after ${timeoutMs} ms.`);
}

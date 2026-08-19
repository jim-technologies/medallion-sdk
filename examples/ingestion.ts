import { MedallionApiError, MedallionClient } from "@jimtech/medallion";

const runId = `example_${Date.now()}`;
const medallion = new MedallionClient({
  baseUrl: requiredEnv("MEDALLION_BASE_URL"),
  apiKey: requiredEnv("MEDALLION_API_KEY"),
  workspaceId: requiredEnv("MEDALLION_WORKSPACE_ID"),
  defaultConnectorId: requiredEnv("MEDALLION_CONNECTOR_ID"),
  timeoutMs: 20_000,
  retry: { maxAttempts: 3, initialDelayMs: 200, maxDelayMs: 2_000 },
  tracing: process.env.MEDALLION_TRACING === "true",
});

try {
  const one = await medallion.cdc.record({
    streamName: "orders",
    entityType: "order",
    entityId: `order_${runId}_1`,
    operation: "insert",
    idempotencyKey: `orders:${runId}:1`,
    payload: { status: "created" },
  });

  const batch = await medallion.cdc.publishBatch({
    events: [
      {
        streamName: "orders",
        entityType: "order",
        entityId: `order_${runId}_2`,
        operation: "insert",
        idempotencyKey: `orders:${runId}:2`,
        payload: { status: "created" },
      },
      {
        streamName: "orders",
        entityType: "order",
        entityId: `order_${runId}_3`,
        operation: "update",
        idempotencyKey: `orders:${runId}:3`,
        payload: { status: "paid" },
      },
    ],
  });

  const audit = await medallion.audit.record({
    resourceType: "order",
    resourceId: `order_${runId}_3`,
    action: "order.capture_payment",
    outcome: "succeeded",
    actor: { type: "user", id: "example-user" },
    idempotencyKey: `orders:audit:${runId}:3`,
    payload: { evidenceRef: `object://audit-evidence/${runId}.json` },
  });

  console.log({
    single: one.events[0],
    batch: batch.events,
    audit: audit.events[0],
  });

  for await (const event of medallion.cdc.iterate({
    streamName: "orders",
    limit: 100,
  })) {
    if (event.idempotencyKey.startsWith(`orders:${runId}:`)) {
      console.log("verified", event.eventId, event.idempotencyKey);
    }
  }
} catch (error) {
  if (error instanceof MedallionApiError) {
    console.error({
      connectCode: error.connectCode,
      reason: error.errorInfoReason,
      requestId: error.requestId,
    });
  }
  throw error;
}

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required.`);
  return value;
}

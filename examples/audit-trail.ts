import { MedallionClient } from "@jimtech/medallion";

const runId = `example_${Date.now()}`;
const resourceId = `order_${runId}`;
const idempotencyKey = `orders:audit:${runId}`;

const medallion = new MedallionClient({
  baseUrl: requiredEnv("MEDALLION_BASE_URL"),
  apiKey: requiredEnv("MEDALLION_API_KEY"),
  workspaceId: requiredEnv("MEDALLION_WORKSPACE_ID"),
  defaultConnectorId: requiredEnv("MEDALLION_CONNECTOR_ID"),
});

const receipt = await medallion.audit.record({
  resourceType: "order",
  resourceId,
  action: "order.cancel",
  outcome: "succeeded",
  actor: { type: "user", id: `user_${runId}` },
  payload: {
    after: { status: "cancelled" },
    evidenceRef: `object://audit-evidence/${runId}.json`,
  },
  idempotencyKey,
  sourceEventId: `source_${runId}`,
});

console.log({
  acceptedCount: receipt.acceptedCount,
  duplicateCount: receipt.duplicateCount,
  durableEventId: receipt.events[0]?.eventId,
});

const page = await medallion.audit.list({
  resourceType: "order",
  resourceId,
  limit: 25,
});

const recorded = page.events.find(
  (event) => event.sourceEventId === `source_${runId}`,
);
if (recorded === undefined) {
  throw new Error("Recorded audit event was not returned by ListAuditEvents.");
}

console.log({
  eventId: recorded.eventId,
  actor: recorded.actor,
  action: recorded.action,
  outcome: recorded.outcome,
  ingesterPrincipal: recorded.ingesterPrincipal,
});

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required.`);
  return value;
}

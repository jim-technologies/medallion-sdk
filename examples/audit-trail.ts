import { MedallionClient } from "@jimtech/medallion";

const baseUrl = requiredEnv("MEDALLION_BASE_URL");
const accessToken = requiredEnv("MEDALLION_SERVICE_ACCOUNT_TOKEN");
const organizationId = requiredEnv("MEDALLION_ORGANIZATION_ID");
const connectorId = requiredEnv("MEDALLION_CONNECTOR_ID");
const expectedIngester = process.env.MEDALLION_EXPECTED_INGESTER_PRINCIPAL;

const runId = `example_${Date.now()}`;
const actor = { type: "user", id: `user_${runId}` } as const;
const resource = { type: "order", id: `order_${runId}` } as const;
const action = "order.cancelled";
const evidenceUrl =
  process.env.MEDALLION_EVIDENCE_URL ??
  `https://github.com/jim-technologies/medallion-sdk/tree/${process.env.MEDALLION_SDK_REF ?? "v0.1.0"}`;

const medallion = new MedallionClient({
  baseUrl,
  accessToken,
  organizationId,
  defaultConnectorId: connectorId,
});

await medallion.audit.record({
  actor,
  action,
  resource,
  before: { status: "confirmed" },
  after: { status: "cancelled" },
  metadata: { exampleRunId: runId },
  evidenceUrl,
  idempotencyKey: `audit_${runId}`,
  sourceEventId: `source_${runId}`,
});

const trail = await medallion.audit.trail({
  resourceType: resource.type,
  resourceId: resource.id,
  action,
  limit: 25,
});

const event = trail.events.find((candidate) => {
  return (
    candidate.sourceEventId === `source_${runId}` ||
    candidate.metadata?.exampleRunId === runId
  );
});

assert(event !== undefined, "recorded audit event was not returned by audit.trail()");
assert(event.actor?.type === actor.type, "source actor type did not round-trip");
assert(event.actor?.id === actor.id, "source actor id did not round-trip");
assert(event.action === action, "action did not round-trip");
assert(event.targetType === resource.type, "resource type did not round-trip");
assert(event.targetId === resource.id, "resource id did not round-trip");
assert(recordValue(event.before)?.status === "confirmed", "before state did not round-trip");
assert(recordValue(event.after)?.status === "cancelled", "after state did not round-trip");
assert(event.evidenceUrl === evidenceUrl, "evidence URL did not round-trip");

if (expectedIngester !== undefined && expectedIngester.trim().length > 0) {
  assert(
    event.ingesterPrincipal === expectedIngester,
    `expected ingester ${expectedIngester}, got ${event.ingesterPrincipal ?? "<missing>"}`,
  );
}

console.log(
  JSON.stringify(
    {
      ok: true,
      eventId: event.eventId,
      actor: event.actor,
      ingesterPrincipal: event.ingesterPrincipal,
      action: event.action,
      targetType: event.targetType,
      targetId: event.targetId,
      evidenceUrl: event.evidenceUrl,
    },
    null,
    2,
  ),
);

function requiredEnv(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`${name} is required.`);
  }
  return value;
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) {
    throw new Error(message);
  }
}

function recordValue(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

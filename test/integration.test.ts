import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { MedallionClient } from "../src/index.js";

interface SeenRequest {
  method: string | undefined;
  path: string | undefined;
  query: URLSearchParams;
  headers: IncomingMessage["headers"];
  body: string;
}

const seen: SeenRequest[] = [];
let closeServer: (() => Promise<void>) | undefined;

afterEach(async () => {
  await closeServer?.();
  closeServer = undefined;
  seen.length = 0;
});

describe("in-process integration routes", () => {
  it("hits actual backend routes for the public client surface", async () => {
    const baseUrl = await startTestServer();
    const client = new MedallionClient({
      baseUrl,
      apiKey: "server_test_key",
      organizationId: "org_123",
      defaultConnectorId: "conn_123",
    });

    await client.connect.registerDatasource({
      name: "primary_postgres",
      type: "postgres",
      idempotencyKey: "register_primary_postgres",
      displayName: "Primary Postgres",
    });
    await client.audit.record({
      actor: { type: "user", id: "user_123" },
      action: "cancel",
      outcome: "succeeded",
      resource: { type: "order", id: "order_123" },
      evidenceUrl: "https://example.com/deployments/orders-worker/abc123",
      idempotencyKey: "audit_1",
    });
    const trail = await client.audit.trail({
      resourceType: "order",
      resourceId: "order_123",
      action: "cancel",
      origin: "external_provider",
      outcome: "succeeded",
      limit: 10,
    });
    await client.cdc.record({
      source: "primary_postgres",
      table: "orders",
      operation: "update",
      primaryKey: { id: "order_123" },
      idempotencyKey: "cdc_1",
    });
    await client.ontology.query({ question: "What happened to order_123?" });
    await client.ontology.planAction({
      actionName: "order.cancel",
      input: { order_id: "order_123" },
    });
    await client.ontology.executeAction({
      actionName: "order.cancel",
      input: { order_id: "order_123" },
      idempotencyKey: "action_1",
    });
    await client.storage.upload({
      org: "org_123",
      path: "exports/order_123.json",
      contentType: "application/json",
      data: JSON.stringify({ id: "order_123" }),
      idempotencyKey: "upload_1",
    });

    expect(seen.map((request) => request.path)).toEqual([
      "/medallion.connect.v1.MedallionConnectService/RegisterConnector",
      "/medallion.connect.v1.MedallionConnectService/PublishAuditEvents",
      "/medallion.connect.v1.MedallionConnectService/ListAuditEvents",
      "/medallion.connect.v1.MedallionConnectService/PublishCdcEvents",
      "/rpc/medallion.ontology.v1.MedallionOntologyService/Query",
      "/rpc/medallion.ontology.v1.MedallionOntologyService/PlanAction",
      "/rpc/medallion.ontology.v1.MedallionOntologyService/ExecuteAction",
      "/medallion.storage.v1.StorageService/Upload",
    ]);
    expect(
      seen.every(
        (request) => request.headers.authorization === "Bearer server_test_key",
      ),
    ).toBe(true);
    expect(seen[0]?.headers["idempotency-key"]).toBe(
      "register_primary_postgres",
    );
    expect(seen[1]?.headers["idempotency-key"]).toBe("audit_1");
    expect(JSON.parse(seen[1]?.body ?? "{}")).toMatchObject({
      events: [
        {
          payloadJson: JSON.stringify({
            actor: { type: "user", id: "user_123" },
            resource: { type: "order", id: "order_123" },
            before: null,
            after: null,
            metadata: null,
            evidenceUrl: "https://example.com/deployments/orders-worker/abc123",
          }),
        },
      ],
    });
    expect(JSON.parse(seen[2]?.body ?? "{}")).toMatchObject({
      organizationId: "org_123",
      connectorId: "conn_123",
      resourceType: "order",
      resourceId: "order_123",
      limit: 10,
    });
    expect(seen[3]?.headers["idempotency-key"]).toBe("cdc_1");
    expect(seen[6]?.headers["idempotency-key"]).toBe("action_1");
    expect(seen[7]?.headers["idempotency-key"]).toBe("upload_1");
    expect(JSON.parse(seen[7]?.body ?? "{}")).toMatchObject({
      org: "org_123",
      path: "exports/order_123.json",
      contentType: "application/json",
      requestId: "upload_1",
    });
    expect(trail.events[0]).toMatchObject({
      actor: { type: "user", id: "user_123" },
      ingesterPrincipal: "service_account:orders-worker",
      action: "cancel",
      sourceSystem: "orders",
      origin: "external_provider",
      outcome: "succeeded",
      targetType: "order",
      targetId: "order_123",
      after: { status: "cancelled" },
      evidenceUrl: "https://example.com/deployments/orders-worker/abc123",
    });
  });
});

async function startTestServer(): Promise<string> {
  const server = createServer(async (request, response) => {
    const url = new URL(request.url ?? "/", "http://localhost");
    const body = await readBody(request);
    seen.push({
      method: request.method,
      path: url.pathname,
      query: url.searchParams,
      headers: request.headers,
      body,
    });

    writeRouteResponse(url.pathname, response);
  });

  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", resolve);
  });
  closeServer = () =>
    new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  const address = server.address();
  if (address === null || typeof address === "string") {
    throw new Error("test server did not bind to a TCP port");
  }

  return `http://127.0.0.1:${address.port}`;
}

function readBody(request: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    request.on("data", (chunk: Buffer) => chunks.push(chunk));
    request.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    request.on("error", reject);
  });
}

function writeRouteResponse(path: string, response: ServerResponse): void {
  if (path.endsWith("/RegisterConnector")) {
    writeJson(response, {
      connector: {
        id: "conn_123",
        organization_id: "org_123",
        kind: "postgres",
        source_system: "primary_postgres",
        display_name: "Primary Postgres",
      },
    });
    return;
  }

  if (path.endsWith("/PublishAuditEvents")) {
    writeJson(response, {
      accepted_count: 1,
      duplicate_count: 0,
      events: [{ idempotency_key: "audit_1", event_id: "1" }],
    });
    return;
  }

  if (path.endsWith("/ListAuditEvents")) {
    writeJson(response, {
      events: [
        {
          id: "1",
          organization_id: "org_123",
          connector_id: "conn_123",
          resource_type: "order",
          resource_id: "order_123",
          idempotency_key: "audit_1",
          payload_json: JSON.stringify({
            actor: { type: "user", id: "user_123" },
            before: { status: "confirmed" },
            after: { status: "cancelled" },
            metadata: { reason: "user_request" },
            evidenceUrl: "https://example.com/deployments/orders-worker/abc123",
          }),
          actor_principal: "user:user_123",
          ingested_by_principal: "service_account:orders-worker",
          action: "cancel",
          source_system: "orders",
          origin: "AUDIT_EVENT_ORIGIN_EXTERNAL_PROVIDER",
          outcome: "AUDIT_EVENT_OUTCOME_SUCCEEDED",
          occurred_at: "2026-07-07T00:00:00Z",
        },
      ],
    });
    return;
  }

  if (path.endsWith("/PublishCdcEvents")) {
    writeJson(response, {
      accepted_count: 1,
      duplicate_count: 0,
      events: [{ idempotency_key: "cdc_1", event_id: "2" }],
    });
    return;
  }

  if (path.endsWith("/Query")) {
    writeJson(response, {
      answer: "Order was cancelled.",
      resource_ids: ["order_123"],
    });
    return;
  }

  if (path.endsWith("/PlanAction")) {
    writeJson(response, {
      plan: {
        id: "plan_1",
        tenant_id: "org_123",
        action_name: "order.cancel",
        actor_principal: "user:google:user_123",
        status: "ACTION_INVOCATION_STATUS_PLANNED",
      },
    });
    return;
  }

  if (path.endsWith("/ExecuteAction")) {
    writeJson(response, {
      invocation: {
        id: "invoke_1",
        tenant_id: "org_123",
        action_name: "order.cancel",
        actor_principal: "user:google:user_123",
        idempotency_key: "action_1",
        status: "ACTION_INVOCATION_STATUS_SUCCEEDED",
      },
    });
    return;
  }

  if (path.endsWith("/Upload")) {
    writeJson(response, {
      org: "org_123",
      path: "exports/order_123.json",
      entry: {
        filename: "order_123.json",
        content_type: "application/json",
        size_bytes: 18,
      },
    });
    return;
  }

  writeJson(response, { error: `unexpected path ${path}` }, 404);
}

function writeJson(
  response: ServerResponse,
  body: unknown,
  status = 200,
): void {
  response.statusCode = status;
  response.setHeader("content-type", "application/json");
  response.setHeader("x-request-id", "req_test");
  response.end(JSON.stringify(body));
}

import { readFile } from "node:fs/promises";
import { Server } from "@jim-technologies/invariant-protocol";
import { describe, expect, it } from "vitest";
import { CONNECT_ROUTES, ONTOLOGY_ROUTES, STORAGE_ROUTES } from "../src/index.js";

interface RouteManifest {
  connect: {
    service: string;
    methods: string[];
  };
  ontology: {
    service: string;
    rpcPrefix: string;
    methods: string[];
    rest: Record<string, string>;
  };
  storage: {
    service: string;
    rest: Record<string, string>;
    methods: string[];
  };
}

describe("client-facing route compatibility", () => {
  it("keeps SDK route constants aligned to the vendored backend route manifest", async () => {
    const manifest = JSON.parse(
      await readFile(
        new URL("../proto/client-facing-routes.json", import.meta.url),
        "utf8",
      ),
    ) as RouteManifest;
    const connectProtoMethods = await protoMethods(
      "../proto/medallion/connect/v1/connect.proto",
    );
    const ontologyProtoMethods = await protoMethods(
      "../proto/medallion/ontology/v1/ontology.proto",
    );
    const storageProtoMethods = await protoMethods(
      "../proto/medallion/storage/v1/service.proto",
    );
    const connectDescriptorMethods = await descriptorMethods(
      "../proto/medallion-connect.descriptor.binpb",
      manifest.connect.service,
    );
    const ontologyDescriptorMethods = await descriptorMethods(
      "../proto/medallion-ontology.descriptor.binpb",
      manifest.ontology.service,
    );
    const storageDescriptorMethods = await descriptorMethods(
      "../proto/medallion-storage.descriptor.binpb",
      manifest.storage.service,
    );
    const connectBase = `/${manifest.connect.service}`;
    const ontologyBase = `${manifest.ontology.rpcPrefix}/${manifest.ontology.service}`;
    const storageBase = `/${manifest.storage.service}`;

    expect(connectProtoMethods).toEqual(manifest.connect.methods);
    expect(connectDescriptorMethods).toEqual(manifest.connect.methods);
    expect(CONNECT_ROUTES.registerConnector).toBe(
      `${connectBase}/RegisterConnector`,
    );
    expect(CONNECT_ROUTES.publishCdcEvents).toBe(
      `${connectBase}/PublishCdcEvents`,
    );
    expect(CONNECT_ROUTES.listCdcEvents).toBe(`${connectBase}/ListCdcEvents`);
    expect(CONNECT_ROUTES.publishAuditEvents).toBe(
      `${connectBase}/PublishAuditEvents`,
    );
    expect(CONNECT_ROUTES.listAuditEvents).toBe(
      `${connectBase}/ListAuditEvents`,
    );
    expect(manifest.connect.methods).toContain("PublishCdcEvents");
    expect(manifest.connect.methods).toContain("PublishAuditEvents");
    expect(manifest.connect.methods).toContain("RegisterConnector");

    expect(ontologyProtoMethods).toEqual(manifest.ontology.methods);
    expect(ontologyDescriptorMethods).toEqual(manifest.ontology.methods);
    expect(`${ontologyBase}/Query`).toBe(
      "/rpc/medallion.ontology.v1.MedallionOntologyService/Query",
    );
    expect(`${ontologyBase}/PlanAction`).toBe(
      "/rpc/medallion.ontology.v1.MedallionOntologyService/PlanAction",
    );
    expect(`${ontologyBase}/ExecuteAction`).toBe(
      "/rpc/medallion.ontology.v1.MedallionOntologyService/ExecuteAction",
    );
    expect(ONTOLOGY_ROUTES.query).toBe(manifest.ontology.rest.query);
    expect(ONTOLOGY_ROUTES.planAction("sync")).toBe("/v1/actions/sync:plan");
    expect(manifest.ontology.rest.planAction).toBe(
      "/v1/actions/{action_name}:plan",
    );
    expect(ONTOLOGY_ROUTES.executeAction("sync")).toBe(
      "/v1/actions/sync:execute",
    );

    expect(storageProtoMethods).toEqual(manifest.storage.methods);
    expect(storageDescriptorMethods).toEqual(manifest.storage.methods);
    expect(STORAGE_ROUTES.upload).toBe(manifest.storage.rest.upload);
    expect(STORAGE_ROUTES.uploadRpc).toBe(`${storageBase}/Upload`);
  });

  it("pins the dedicated canonical AuditEvent wire shape", async () => {
    const descriptor = await readFile(
      new URL("../proto/medallion-connect.descriptor.binpb", import.meta.url),
    );
    const parsed = Server.fromBytes(descriptor).parsed;
    const auditEvent = parsed.getMessage("medallion.connect.v1.AuditEvent");

    expect(
      auditEvent?.fields.map((field) => [field.number, field.name]),
    ).toEqual([
      [1, "id"],
      [2, "organization_id"],
      [3, "connector_id"],
      [4, "resource_type"],
      [5, "resource_id"],
      [6, "action"],
      [7, "source_event_id"],
      [8, "idempotency_key"],
      [9, "actor_principal"],
      [10, "payload_json"],
      [11, "occurred_at"],
      [12, "observed_at"],
      [13, "description"],
      [14, "source_system"],
      [15, "ingested_by_principal"],
    ]);
  });

  it("pins the dedicated canonical CDC wire shapes", async () => {
    const descriptor = await readFile(
      new URL("../proto/medallion-connect.descriptor.binpb", import.meta.url),
    );
    const parsed = Server.fromBytes(descriptor).parsed;
    const cdcEvent = parsed.getMessage("medallion.connect.v1.CdcEvent");
    const listRequest = parsed.getMessage(
      "medallion.connect.v1.ListCdcEventsRequest",
    );

    expect(cdcEvent?.fields.map((field) => [field.number, field.name])).toEqual([
      [1, "id"],
      [2, "organization_id"],
      [3, "connector_id"],
      [4, "stream_name"],
      [5, "entity_type"],
      [6, "entity_id"],
      [7, "operation"],
      [8, "source_event_id"],
      [9, "idempotency_key"],
      [10, "actor_principal"],
      [11, "payload_json"],
      [12, "occurred_at"],
      [13, "observed_at"],
      [14, "description"],
      [15, "source_system"],
      [16, "ingested_by_principal"],
    ]);
    expect(
      listRequest?.fields.map((field) => [field.number, field.name]),
    ).toEqual([
      [1, "organization_id"],
      [2, "connector_id"],
      [3, "entity_type"],
      [4, "entity_id"],
      [5, "limit"],
      [6, "actor_principal"],
      [7, "occurred_at_from"],
      [8, "occurred_at_to"],
      [9, "source_system"],
      [10, "stream_name"],
      [11, "page_cursor"],
      [12, "ingested_by_principal"],
    ]);
  });
});

async function protoMethods(path: string): Promise<string[]> {
  const source = await readFile(new URL(path, import.meta.url), "utf8");
  return [...source.matchAll(/^\s*rpc\s+(\w+)\(/gm)].map((match) => match[1]!);
}

async function descriptorMethods(
  path: string,
  serviceName: string,
): Promise<string[]> {
  const descriptor = await readFile(new URL(path, import.meta.url));
  const service = Server.fromBytes(descriptor).parsed.services.get(serviceName);
  if (service === undefined) {
    throw new Error(`missing service ${serviceName}`);
  }
  return [...service.methods.keys()];
}

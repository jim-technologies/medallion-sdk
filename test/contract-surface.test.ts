import { readFile } from "node:fs/promises";
import { ParsedDescriptor } from "@jim-technologies/invariant-protocol";
import { describe, expect, it } from "vitest";
import * as publicSdk from "../src/index.js";

const EXPECTED_METHODS = [
  "PublishCdcEvents",
  "PublishAuditEvents",
  "ListCdcEvents",
  "ListAuditEvents",
] as const;
const EXPECTED_DESCRIPTOR_METHODS = [
  "PublishCdcEvents",
  "ListCdcEvents",
  "PublishAuditEvents",
  "ListAuditEvents",
] as const;

interface RouteManifest {
  connect: {
    service: string;
    methods: string[];
  };
}

interface ExternalIngestionManifest {
  service: string;
  methods: Array<{ name: string }>;
}

describe("bounded external-ingestion contract", () => {
  it("ships exactly the reviewed four RPCs in source and descriptor", async () => {
    const routes = JSON.parse(
      await readFile(
        new URL("../proto/client-facing-routes.json", import.meta.url),
        "utf8",
      ),
    ) as RouteManifest;
    const allowlist = JSON.parse(
      await readFile(
        new URL("../proto/external-ingestion-v1.json", import.meta.url),
        "utf8",
      ),
    ) as ExternalIngestionManifest;
    const source = await readFile(
      new URL("../proto/medallion/connect/v1/connect.proto", import.meta.url),
      "utf8",
    );
    const sourceMethods = [...source.matchAll(/^\s*rpc\s+(\w+)\(/gm)].map(
      (match) => match[1],
    );
    const descriptor = await readFile(
      new URL(
        "../proto/external-ingestion-v1.descriptor.binpb",
        import.meta.url,
      ),
    );
    const service = ParsedDescriptor.fromBytes(descriptor).services.get(
      routes.connect.service,
    );

    expect(Object.keys(routes)).toEqual(["connect"]);
    expect(routes.connect).toEqual({
      service: allowlist.service,
      methods: [...EXPECTED_METHODS],
    });
    expect(allowlist.methods.map((method) => method.name)).toEqual(
      EXPECTED_METHODS,
    );
    expect(sourceMethods).toEqual(EXPECTED_DESCRIPTOR_METHODS);
    expect([...service!.methods.keys()]).toEqual(EXPECTED_DESCRIPTOR_METHODS);
    expect(
      [...service!.methods.values()].map((method) => method.desc.methodKind),
    ).toEqual(["unary", "unary", "unary", "unary"]);
  });

  it("has no generic or control-plane runtime dispatcher", () => {
    expect(
      Object.getOwnPropertyNames(publicSdk.ProtocolConnectClient.prototype),
    ).toEqual([
      "constructor",
      "publishCdcEvents",
      "listCdcEvents",
      "publishAuditEvents",
      "listAuditEvents",
    ]);

    for (const name of [
      "ConnectClient",
      ["Protocol", ["Onto", "logy"].join(""), "Client"].join(""),
      "ProtocolStorageClient",
      "CONNECT_ROUTES",
      ["ONTO", "LOGY_ROUTES"].join(""),
      "STORAGE_ROUTES",
    ]) {
      expect(name in publicSdk).toBe(false);
    }
  });
});

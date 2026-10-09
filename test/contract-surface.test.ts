import { readFile } from "node:fs/promises";
import { ParsedDescriptor } from "@jim-technologies/invariant-protocol";
import { describe, expect, it } from "vitest";
import * as publicSdk from "../src/index.js";

const METHODS = [
  "CreateTable",
  "GetTable",
  "ListTables",
  "UpdateTable",
  "AppendRows",
  "RunQuery",
  "GetQueryResults",
];

describe("bounded ingest contract", () => {
  it("ships exactly seven unary ingest methods in source and descriptor", async () => {
    const source = await readFile(
      new URL("../proto/medallion/ingest/v1/ingest.proto", import.meta.url),
      "utf8",
    );
    const bytes = await readFile(
      new URL("../proto/ingest-v1.descriptor.binpb", import.meta.url),
    );
    const descriptor = ParsedDescriptor.fromBytes(bytes);
    expect([...descriptor.services.keys()]).toEqual([
      "medallion.ingest.v1.MedallionIngestService",
    ]);
    const service = descriptor.services.get(
      "medallion.ingest.v1.MedallionIngestService",
    )!;
    expect(
      [...source.matchAll(/^\s*rpc\s+(\w+)\(/gm)].map((match) => match[1]),
    ).toEqual(METHODS);
    expect([...service.methods.keys()]).toEqual(METHODS);
    expect(
      [...service.methods.values()].map((method) => method.desc.methodKind),
    ).toEqual(METHODS.map(() => "unary"));
  });

  it("does not export the retired business surface or a generic dispatcher", () => {
    for (const name of [
      "ProtocolConnectClient",
      "ConnectClient",
      "AuditClient",
      "CdcClient",
      "CONNECT_ROUTES",
      "ProtocolStorageClient",
    ]) {
      expect(name in publicSdk).toBe(false);
    }
  });
});
